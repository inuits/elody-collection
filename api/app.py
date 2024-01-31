import importlib
import json
import logging
import os
import secrets

from elody.loader import load_apps, load_policies, load_queues
from elody.util import CustomJSONEncoder, custom_json_dumps
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
app.config["RESTFUL_JSON"] = {"cls": CustomJSONEncoder}
api = Api(app)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(16))

tenant_defining_types = os.getenv("TENANT_DEFINING_TYPES")
tenant_defining_types = (
    tenant_defining_types.split(",") if tenant_defining_types else []
)

logging.basicConfig(
    format="%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

amqp_module = importlib.import_module(os.getenv("AMQP_MANAGER", "amqpstorm_flask"))
auto_delete_exchange = os.getenv("AUTO_DELETE_EXCHANGE", False) in [
    1,
    "1",
    True,
    "True",
    "true",
]
durable_exchange = os.getenv("DURABLE_EXCHANGE", True) in [1, "1", True, "True", "true"]
passive_exchange = os.getenv("PASSIVE_EXCHANGE", False) in [
    1,
    "1",
    True,
    "True",
    "true",
]
rabbit = amqp_module.RabbitMQ(
    exchange_params=amqp_module.ExchangeParams(
        auto_delete=auto_delete_exchange,
        durable=durable_exchange,
        passive=passive_exchange,
    )
)
rabbit.init_app(app, "basic", json.loads, custom_json_dumps)

app.register_blueprint(swaggerui_blueprint)


def database_available():
    return True, StorageManager().get_db_engine().check_health()


def rabbit_available():
    connection = rabbit.get_connection()
    if connection.is_open:
        connection.close()
        return True, "Successfully reached RabbitMQ"
    return False, "Failed to reach RabbitMQ"


health = HealthCheck()
if os.getenv("HEALTH_CHECK_EXTERNAL_SERVICES", True) in ["True", "true", True]:
    health.add_check(database_available)
    health.add_check(rabbit_available)
app.add_url_rule("/health", "healthcheck", view_func=lambda: health.run())

policy_factory = PolicyFactory()
load_apps(app, logger)
load_policies(policy_factory, logger)

from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
    GenericObjectMetadata,
    GenericObjectMetadataKey,
    GenericObjectRelations,
)
from resources.batch import Batch
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
    FilterMatchers,
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
    MediafileMetadata,
)
from resources.saved_search import (
    SavedSearch,
    SavedSearchDetail,
)
from resources.tenant import Tenant
from resources.ticket import Ticket, TicketDetail
from resources.spec import AsyncAPISpec, OpenAPISpec

api.add_resource(Batch, "/batch")
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

api.add_resource(FilterMatchers, "/filter/matchers")
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
api.add_resource(MediafileMetadata, "/mediafiles/<string:id>/metadata")

api.add_resource(SavedSearch, "/saved_searches")
api.add_resource(SavedSearchDetail, "/saved_searches/<string:id>")

api.add_resource(Tenant, "/tenants")

api.add_resource(Ticket, "/tickets")
api.add_resource(TicketDetail, "/tickets/<string:id>")

api.add_resource(AsyncAPISpec, "/spec/dams-collection-api-events.html")
api.add_resource(OpenAPISpec, "/spec/dams-collection-api.json")

api.add_resource(GenericObject, "/<string:collection>")
api.add_resource(GenericObjectDetail, "/<string:collection>/<string:id>")
api.add_resource(GenericObjectMetadata, "/<string:collection>/<string:id>/metadata")
api.add_resource(
    GenericObjectMetadataKey, "/<string:collection>/<string:id>/metadata/<string:key>"
)
api.add_resource(GenericObjectRelations, "/<string:collection>/<string:id>/relations")

# Initialize RabbitMQ Queues
load_queues(logger)
import resources.queues

if __name__ == "__main__":
    app.run()
