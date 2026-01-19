from elody.exceptions import NotFoundException
from apscheduler.schedulers.background import BackgroundScheduler
from configuration import get_features, get_route_mapper, init_mappers
from elody.error_codes import ErrorCode, get_error_code, get_read
from elody.loader import load_apps, load_jobs
from elody.util import CustomJSONEncoder, read_json_as_dict
from flask import Flask, g, make_response, jsonify, Response, request
from flask_swagger_ui import get_swaggerui_blueprint
from glob import glob
from health import init_health_check
from importlib import import_module
from logging_elody.log import log
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from os import getenv, path
from policy_factory import init_policy_factory, get_user_context
from rabbit import init_rabbit, get_rabbit
from secrets import token_hex
from tracing import init_tracer
from werkzeug.exceptions import Forbidden, HTTPException, NotFound, Unauthorized


SWAGGER_URL = "/api/docs"  # URL for exposing Swagger UI (without trailing '/')
API_URL = (
    "/spec/dams-collection-api.json"  # Our API url (can of course be a local resource)
)


def __process_resource_rules(rules):
    rules_map = dict(
        sorted(
            {
                rule["route"]: (rule["route"], rule["resource"], rule["api"])
                for rule in rules
            }.items()
        )
    )
    for route, resource, api in rules_map.values():
        api.add_resource(resource, get_route_mapper().get(resource.__name__, route))


def load_sentry():
    def before_send(event, hint):
        if "exc_info" in hint:
            exc_type, exc_value, tb = hint["exc_info"]
            status_code = getattr(exc_value, "code", None)

            if status_code:
                if 400 <= status_code < 500:
                    return None

        return event

    if getenv("SENTRY_ENABLED", False) in ["True", "true", True]:
        import sentry_sdk
        from sentry_sdk.integrations.flask import FlaskIntegration

        sentry_sdk.init(
            dsn=getenv("SENTRY_DSN"),
            integrations=[FlaskIntegration()],
            ignore_errors=[NotFoundException, NotFound, Forbidden],
            environment=getenv("NOMAD_NAMESPACE"),
            before_send=before_send,
        )


def load_specs(app):
    resource_rules = []
    resources_path = path.join(app.root_path, "resources")
    for spec in get_features().get("specs", {}).keys():
        specs_path = path.join(resources_path, spec)
        resource_paths = glob(path.join(specs_path, "*.py"))
        for sub_spec_module in get_features()["specs"][spec].keys():
            resource_paths.extend(
                glob(path.join(specs_path, f"{sub_spec_module}/*.py"))
            )
        for resource_path in resource_paths:
            try:
                module_path = (
                    resource_path.removeprefix(f"{app.root_path}/")
                    .removesuffix(".py")
                    .replace("/", ".")
                )
                module = import_module(module_path)
                try:
                    app.register_blueprint(module.blueprint)
                except AttributeError:
                    rules = module.resource_rules()
                    resource_rules.extend(rules)
            except ModuleNotFoundError:
                pass
    return resource_rules


def load_app_resources():
    resource_rules = []
    apps = read_json_as_dict(getenv("APPS_MANIFEST"), None)
    for app in apps:
        for resource in apps[app].get("resources", []):
            module = import_module(f"apps.{app}.resources.{resource}")
            try:
                app.register_blueprint(module.blueprint)
            except AttributeError:
                try:
                    rules = module.resource_rules()
                    resource_rules.extend(rules)
                except AttributeError:
                    pass
    return resource_rules


def init_app():
    app = Flask(__name__)
    app.config["RESTFUL_JSON"] = {"cls": CustomJSONEncoder}
    app.secret_key = getenv("SECRET_KEY", token_hex(16))
    init_mappers()
    load_apps(app, None)
    resource_rules = load_specs(app)
    resource_rules.extend(load_app_resources())
    __process_resource_rules(resource_rules)
    return app


def init_scheduler():
    load_background_scheduler = getenv("LOAD_BACKGROUND_SCHEDULER", False)
    if load_background_scheduler:
        scheduler = BackgroundScheduler()
        scheduler.start()
        return scheduler


def register_exporter(app, api):
    from prometheus_flask_exporter import RESTfulPrometheusMetrics

    metrics = RESTfulPrometheusMetrics(app, api, group_by="url_rule")
    metrics.info("collection_api_info", "Metrics for collection-api", version="0.0.1")


def register_swaggerui(app):
    swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL)
    app.register_blueprint(swaggerui_blueprint)


def database_available():
    from storage.storagemanager import StorageManager

    return True, StorageManager().get_db_engine().check_health()


def rabbit_available():
    connection = get_rabbit().get_connection()
    if connection.is_open:
        return True, "Successfully reached RabbitMQ"
    return False, "Failed to reach RabbitMQ"


load_sentry()
tracer = init_tracer()
app = init_app()
FlaskInstrumentor().instrument_app(app)

if scheduler := init_scheduler():
    load_jobs(scheduler, None)
register_swaggerui(app)
from init_api import init_api

api = init_api(app)

init_rabbit(app)
init_health_check(app, database_available, rabbit_available)
init_policy_factory()
if getenv("ENABLE_METRICS", False) in ["True", "true", True]:
    register_exporter(app, api)

try:
    client_app = import_module("apps.client_app")
    client_app.init(app, api)
except (ModuleNotFoundError, AttributeError):
    pass


@app.before_request
def set_dry_run():
    g.dry_run = bool(request.args.get("dry_run", 0, int) or request.args.get("soft"))


@app.errorhandler(HTTPException)
@app.errorhandler(Exception)
def exception(exception):
    item = {}
    try:
        item = get_user_context().bag.get("requested_item", {})
        if not item:
            item = get_user_context().bag.get("item_being_processed")
    except Exception:
        pass
    if exception.__class__ != NotFound:
        log.exception(
            f"{exception.__class__.__name__}: {exception}", item, exc_info=exception
        )
    try:
        return jsonify(message=exception.description), exception.code
    except:
        return jsonify(message=f"{exception.__class__.__name__}: {exception}"), 500


@app.after_request
def intercept_403(response: Response):
    if response.status_code == 403:
        restricted_keys = get_user_context().bag.get("restricted_keys", [])
        if len(restricted_keys) > 0:
            return make_response(
                jsonify(
                    message=f"{get_error_code(ErrorCode.INSUFFICIENT_PERMISSIONS, get_read())} | restricted_keys:{restricted_keys} - You don't have the permission to create/update/delete the following fields: {restricted_keys}.",
                    restricted_keys=restricted_keys,
                ),
                response.status_code,
            )
        if api.handle_error(Forbidden()).get_data() == response.get_data():
            raise Forbidden(
                f"{get_error_code(ErrorCode.INSUFFICIENT_PERMISSIONS_WITHOUT_VARS, get_read())} - You don't have the permission to create/update/delete this resource."
            )
    return response


@app.after_request
def intercept_401(response: Response):
    if response.status_code == 401:
        raise Unauthorized(
            f"{get_error_code(ErrorCode.INVALID_TOKEN, get_read())} - The access token provided is expired, revoked, malformed, or invalid for other reasons."
        )
    return response


if __name__ == "__main__":
    app.run()
