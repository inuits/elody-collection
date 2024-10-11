from apscheduler.schedulers.background import BackgroundScheduler
from configuration import init_mappers
from cron_jobs.ttl_checker import TtlChecker
from elody.loader import load_apps, load_jobs
from elody.util import CustomJSONEncoder
from flask import Flask, make_response, jsonify, Response
from flask_swagger_ui import get_swaggerui_blueprint
from health import init_health_check
from logging_elody.log import log
from os import getenv
from policy_factory import init_policy_factory, get_user_context
from rabbit import init_rabbit, get_rabbit
from secrets import token_hex
from werkzeug.exceptions import HTTPException


SWAGGER_URL = "/api/docs"  # URL for exposing Swagger UI (without trailing '/')
API_URL = (
    "/spec/dams-collection-api.json"  # Our API url (can of course be a local resource)
)


def load_sentry():
    if getenv("SENTRY_ENABLED", False) in ["True", "true", True]:
        import sentry_sdk
        from sentry_sdk.integrations.flask import FlaskIntegration

        sentry_sdk.init(
            dsn=getenv("SENTRY_DSN"),
            integrations=[FlaskIntegration()],
            environment=getenv("NOMAD_NAMESPACE"),
        )


def init_app():
    app = Flask(__name__)
    app.config["RESTFUL_JSON"] = {"cls": CustomJSONEncoder}
    app.secret_key = getenv("SECRET_KEY", token_hex(16))
    init_mappers()
    load_apps(app, None)
    return app


def init_scheduler():
    load_background_scheduler = getenv("LOAD_BACKGROUND_SCHEDULER", False)
    if load_background_scheduler:
        checker = TtlChecker()
        scheduler = BackgroundScheduler()
        scheduler.add_job(checker, "cron", hour=12, minute=10)
        scheduler.start()
        return scheduler


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
app = init_app()

if scheduler := init_scheduler():
    load_jobs(scheduler, None)
register_swaggerui(app)
from init_api import init_api

init_api(app)
init_rabbit(app)
init_health_check(app, database_available, rabbit_available)
init_policy_factory()


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
                    message=f"You don't have the permission to create/update/delete the following fields: {restricted_keys}.",
                    restricted_keys=restricted_keys,
                ),
                response.status_code,
            )
    return response


if __name__ == "__main__":
    app.run()
