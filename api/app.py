import json

from apscheduler.schedulers.background import BackgroundScheduler
from configuration import get_features, get_route_mapper, init_mappers
from elody.error_codes import ErrorCode, get_error_code, get_read
from elody.exceptions import NotFoundException
from elody.loader import load_apps, load_jobs
from elody.util import CustomJSONEncoder, read_json_as_dict
from flask import Flask, g, make_response, jsonify, Response, request
from flask.json.provider import DefaultJSONProvider
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
from werkzeug.middleware.proxy_fix import ProxyFix
from metrics import init_metrics
from glitchtip import init_glitchtip

SWAGGER_URL = "/api/docs"  # URL for exposing Swagger UI (without trailing '/')
API_URL = (
    "/spec/dams-collection-api.json"  # Our API url (can of course be a local resource)
)


class ElodyJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        return json.dumps(obj, cls=CustomJSONEncoder, **kwargs)

    def loads(self, s, **kwargs):
        return json.loads(s, **kwargs)


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
    app.json = ElodyJSONProvider(app)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_prefix=1)
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


def register_exporter(app, api):  # deprecated
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
    # register_exporter(app, api)
    init_metrics(app, api)

try:
    client_app = import_module("apps.client_app")
    client_app.init(app, api)
except (ModuleNotFoundError, AttributeError):
    pass

init_glitchtip(app, api)


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
    if isinstance(exception, HTTPException):
        return jsonify(message=exception.description), exception.code or 500
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
            return make_response(
                jsonify(
                    message=f"{get_error_code(ErrorCode.INSUFFICIENT_PERMISSIONS_WITHOUT_VARS, get_read())} - You don't have the permission to create/update/delete this resource."
                ),
                403,
            )
    return response


@app.after_request
def intercept_401(response: Response):
    if response.status_code == 401:
        return make_response(
            jsonify(
                message=f"{get_error_code(ErrorCode.INVALID_TOKEN, get_read())} - The access token provided is expired, revoked, malformed, or invalid for other reasons."
            ),
            401,
        )
    return response


if __name__ == "__main__":
    app.run()
