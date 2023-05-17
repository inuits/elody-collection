import json
import logging
import os
import secrets

from amqp.amqpmanager import AmqpManager
from apps.loader import load_apps, load_policies, load_queues
from flask import Flask
from flask_restful import Api
from flask_swagger_ui import get_swaggerui_blueprint
from healthcheck import HealthCheck
from inuits_policy_based_auth import PolicyFactory
from storage.storagemanager import StorageManager

if os.getenv("SENTRY_ENABLED", False) in ["True", "true", True]:
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration

    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        integrations=[FlaskIntegration()],
        environment=os.getenv("NOMAD_NAMESPACE"),
    )

SWAGGER_URL = "/api/docs"  # URL for exposing Swagger UI (without trailing '/')
API_URL = (
    "/spec/dams-collection-api.json"  # Our API url (can of course be a local resource)
)

swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL)

app = Flask(__name__)
api = Api(app)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(16))

logging.basicConfig(
    format="%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

rabbit = AmqpManager().get_amqp_manager()
rabbit.init_app(app, "basic", json.loads, json.dumps)

app.register_blueprint(swaggerui_blueprint)


def database_available():
    return True, StorageManager().get_db_engine().check_health()


def rabbit_available():
    return True, rabbit.get_connection().is_open


health = HealthCheck()
if os.getenv("HEALTH_CHECK_EXTERNAL_SERVICES", True) in ["True", "true", True]:
    health.add_check(database_available)
    health.add_check(rabbit_available)
app.add_url_rule("/health", "healthcheck", view_func=lambda: health.run())

policy_factory = PolicyFactory()
load_apps(app)
load_policies(policy_factory, logger)

from resources.config import Config
from resources.entity import (
    Entity,
    EntityDetail,
    EntityMediafiles,
    EntityMediafilesCreate,
    EntityMetadata,
    EntityMetadataKey,
    EntityRelations,
    EntityRelationsAll,
    EntitySetPrimaryMediafile,
    EntitySetPrimaryThumbnail,
)
from resources.filter import (
    FilterEntities,
    FilterEntitiesBySavedSearchId,
    FilterMediafiles,
    FilterMediafilesBySavedSearchId,
)
from resources.history import History
from resources.job import Job, JobDetail
from resources.key_value_store import KeyValueStore, KeyValueStoreDetail
from resources.mediafile import (
    Mediafile,
    MediafileAssets,
    MediafileCopyright,
    MediafileDetail,
)
from resources.saved_search import (
    SavedSearch,
    SavedSearchDetail,
)
from resources.user import UserPermissions
from resources.spec import AsyncAPISpec, OpenAPISpec

api.add_resource(Config, "/config")
api.add_resource(Entity, "/entities")
api.add_resource(EntityDetail, "/entities/<string:id>")
api.add_resource(EntityMediafiles, "/entities/<string:id>/mediafiles")
api.add_resource(EntityMediafilesCreate, "/entities/<string:id>/mediafiles/create")
api.add_resource(EntityMetadata, "/entities/<string:id>/metadata")
api.add_resource(EntityMetadataKey, "/entities/<string:id>/metadata/<string:key>")
api.add_resource(EntityRelations, "/entities/<string:id>/relations")
api.add_resource(EntityRelationsAll, "/entities/<string:id>/relations/all")
api.add_resource(
    EntitySetPrimaryMediafile,
    "/entities/<string:id>/set_primary_mediafile/<string:mediafile_id>",
)
api.add_resource(
    EntitySetPrimaryThumbnail,
    "/entities/<string:id>/set_primary_thumbnail/<string:mediafile_id>",
)

api.add_resource(FilterEntities, "/entities/filter")
api.add_resource(FilterEntitiesBySavedSearchId, "/entities/filter/<string:id>")
api.add_resource(FilterMediafiles, "/mediafiles/filter")
api.add_resource(FilterMediafilesBySavedSearchId, "/mediafiles/filter/<string:id>")

api.add_resource(History, "/history/<string:collection>/<string:id>")

api.add_resource(Job, "/jobs")
api.add_resource(JobDetail, "/jobs/<string:id>")

api.add_resource(KeyValueStore, "/key_value_store")
api.add_resource(KeyValueStoreDetail, "/key_value_store/<string:id>")

api.add_resource(Mediafile, "/mediafiles")
api.add_resource(MediafileAssets, "/mediafiles/<string:id>/assets")
api.add_resource(MediafileCopyright, "/mediafiles/<string:id>/copyright")
api.add_resource(MediafileDetail, "/mediafiles/<string:id>")

api.add_resource(SavedSearch, "/saved_searches")
api.add_resource(SavedSearchDetail, "/saved_searches/<string:id>")

api.add_resource(UserPermissions, "/user/permissions")

api.add_resource(AsyncAPISpec, "/spec/dams-collection-api-events.html")
api.add_resource(OpenAPISpec, "/spec/dams-collection-api.json")

# Initialize RabbitMQ Queues
load_queues()
import resources.queues

if __name__ == "__main__":
    app.run()
