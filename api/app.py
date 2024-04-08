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
from importlib import import_module
from inuits_policy_based_auth import PolicyFactory
from object_configurations.object_configuration_mapper import ObjectConfigurationMapper
from serialization.serializer import Serializer
from validation.validator import Validator
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
rabbit.init_app(
    app, "basic", json.loads, custom_json_dumps, json_encoder=CustomJSONEncoder
)

app.register_blueprint(swaggerui_blueprint)


def database_available():
    return True, StorageManager().get_db_engine().check_health()


def rabbit_available():
    connection = rabbit.get_connection()
    if connection.is_open:
        return True, "Successfully reached RabbitMQ"
    return False, "Failed to reach RabbitMQ"


health = HealthCheck()
if os.getenv("HEALTH_CHECK_EXTERNAL_SERVICES", True) in ["True", "true", True]:
    health.add_check(database_available)
    health.add_check(rabbit_available)
app.add_url_rule("/health", "healthcheck", view_func=lambda: health.run())

policy_factory = PolicyFactory()
load_apps(app, logger)
try:
    permissions_module = import_module("apps.permissions")
    load_policies(policy_factory, logger, permissions_module.PERMISSIONS)
except ModuleNotFoundError:
    load_policies(policy_factory, logger)

try:
    mapper_module = import_module("apps.mappers")
    object_configuration_mapper = ObjectConfigurationMapper(
        mapper_module.OBJECT_CONFIGURATION_MAPPER
    )
    route_mapper = mapper_module.ROUTE_MAPPER
except ModuleNotFoundError:
    object_configuration_mapper = ObjectConfigurationMapper()
    route_mapper = {}

serialize = Serializer()
Validator = Validator().validator

from resources.generic_object import (
    GenericObject,
    GenericObjectV2,
    GenericObjectDetail,
    GenericObjectDetailV2,
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
    FilterEntitiesV2,
    FilterEntitiesBySavedSearchId,
    FilterGenericObjects,
    FilterGenericObjectsV2,
    FilterGenericObjectsBySavedSearchId,
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
    MediafileDerivatives,
    MediafileParent,
)
from resources.saved_search import (
    SavedSearch,
    SavedSearchDetail,
)
from resources.tenant import Tenant
from resources.ticket import Ticket, TicketDetail
from resources.spec import AsyncAPISpec, OpenAPISpec

api.add_resource(Batch, route_mapper.get(Batch.__name__, "/batch"))
api.add_resource(Config, route_mapper.get(Config.__name__, "/config"))
api.add_resource(Entity, route_mapper.get(Entity.__name__, "/entities"))
api.add_resource(
    EntityDetail, route_mapper.get(EntityDetail.__name__, "/entities/<string:id>")
)
api.add_resource(
    EntityMediafiles,
    route_mapper.get(EntityMediafiles.__name__, "/entities/<string:id>/mediafiles"),
)
api.add_resource(
    EntityMediafilesCreate,
    route_mapper.get(
        EntityMediafilesCreate.__name__, "/entities/<string:id>/mediafiles/create"
    ),
)
api.add_resource(
    EntityMetadata,
    route_mapper.get(EntityMetadata.__name__, "/entities/<string:id>/metadata"),
)
api.add_resource(
    EntityMetadataKey,
    route_mapper.get(
        EntityMetadataKey.__name__, "/entities/<string:id>/metadata/<string:key>"
    ),
)
api.add_resource(
    EntityRelations,
    route_mapper.get(EntityRelations.__name__, "/entities/<string:id>/relations"),
)
api.add_resource(
    EntityRelationsAll,
    route_mapper.get(
        EntityRelationsAll.__name__, "/entities/<string:id>/relations/all"
    ),
)
api.add_resource(
    EntitySetPrimaryMediafile,
    route_mapper.get(
        EntitySetPrimaryMediafile.__name__,
        "/entities/<string:id>/set_primary_mediafile/<string:mediafile_id>",
    ),
)
api.add_resource(
    EntitySetPrimaryThumbnail,
    route_mapper.get(
        EntitySetPrimaryThumbnail.__name__,
        "/entities/<string:id>/set_primary_thumbnail/<string:mediafile_id>",
    ),
)

api.add_resource(
    FilterMatchers, route_mapper.get(FilterMatchers.__name__, "/filter/matchers")
)
api.add_resource(
    FilterEntities, route_mapper.get(FilterEntities.__name__, "/entities/filter")
)
api.add_resource(
    FilterEntitiesV2, route_mapper.get(FilterEntitiesV2.__name__, "/entities/filter_v2")
)
api.add_resource(
    FilterEntitiesBySavedSearchId,
    route_mapper.get(
        FilterEntitiesBySavedSearchId.__name__, "/entities/filter/<string:id>"
    ),
)
api.add_resource(
    FilterMediafiles, route_mapper.get(FilterMediafiles.__name__, "/mediafiles/filter")
)
api.add_resource(
    FilterMediafilesBySavedSearchId,
    route_mapper.get(
        FilterMediafilesBySavedSearchId.__name__, "/mediafiles/filter/<string:id>"
    ),
)

api.add_resource(
    History,
    route_mapper.get(History.__name__, "/history/<string:collection>/<string:id>"),
)

api.add_resource(Job, route_mapper.get(Job.__name__, "/jobs"))
api.add_resource(JobDetail, route_mapper.get(JobDetail.__name__, "/jobs/<string:id>"))

api.add_resource(
    KeyValueStore, route_mapper.get(KeyValueStore.__name__, "/key_value_store")
)
api.add_resource(
    KeyValueStoreDetail,
    route_mapper.get(KeyValueStoreDetail.__name__, "/key_value_store/<string:id>"),
)

api.add_resource(Mediafile, route_mapper.get(Mediafile.__name__, "/mediafiles"))
api.add_resource(
    MediafileAssets,
    route_mapper.get(MediafileAssets.__name__, "/mediafiles/<string:id>/assets"),
)
api.add_resource(
    MediafileCopyright,
    route_mapper.get(MediafileCopyright.__name__, "/mediafiles/<string:id>/copyright"),
)
api.add_resource(
    MediafileDetail,
    route_mapper.get(MediafileDetail.__name__, "/mediafiles/<string:id>"),
)
api.add_resource(
    MediafileMetadata,
    route_mapper.get(MediafileMetadata.__name__, "/mediafiles/<string:id>/metadata"),
)
api.add_resource(
    MediafileDerivatives,
    route_mapper.get(
        MediafileDerivatives.__name__, "/mediafiles/<string:id>/derivatives"
    ),
)
api.add_resource(
    MediafileParent,
    route_mapper.get(MediafileParent.__name__, "/mediafiles/<string:id>/parent"),
)

api.add_resource(SavedSearch, route_mapper.get(SavedSearch.__name__, "/saved_searches"))
api.add_resource(
    SavedSearchDetail,
    route_mapper.get(SavedSearchDetail.__name__, "/saved_searches/<string:id>"),
)

api.add_resource(Tenant, route_mapper.get(Tenant.__name__, "/tenants"))

api.add_resource(Ticket, route_mapper.get(Ticket.__name__, "/tickets"))
api.add_resource(
    TicketDetail, route_mapper.get(TicketDetail.__name__, "/tickets/<string:id>")
)

api.add_resource(
    AsyncAPISpec,
    route_mapper.get(AsyncAPISpec.__name__, "/spec/dams-collection-api-events.html"),
)
api.add_resource(
    OpenAPISpec,
    route_mapper.get(OpenAPISpec.__name__, "/spec/dams-collection-api.json"),
)

api.add_resource(
    GenericObject, route_mapper.get(GenericObject.__name__, "/<string:collection>")
)
api.add_resource(
    GenericObjectV2,
    route_mapper.get(GenericObjectV2.__name__, "/v2/<string:collection>"),
)
api.add_resource(
    GenericObjectDetail,
    route_mapper.get(GenericObjectDetail.__name__, "/<string:collection>/<string:id>"),
)
api.add_resource(
    GenericObjectDetailV2,
    route_mapper.get(
        GenericObjectDetailV2.__name__, "/v2/<string:collection>/<string:id>"
    ),
)
api.add_resource(
    GenericObjectMetadata,
    route_mapper.get(
        GenericObjectMetadata.__name__, "/<string:collection>/<string:id>/metadata"
    ),
)
api.add_resource(
    GenericObjectMetadataKey,
    route_mapper.get(
        GenericObjectMetadataKey.__name__,
        "/<string:collection>/<string:id>/metadata/<string:key>",
    ),
)
api.add_resource(
    GenericObjectRelations,
    route_mapper.get(
        GenericObjectRelations.__name__, "/<string:collection>/<string:id>/relations"
    ),
)
api.add_resource(
    FilterGenericObjects,
    route_mapper.get(FilterGenericObjects.__name__, "/<string:collection>/filter"),
)
api.add_resource(
    FilterGenericObjectsV2,
    route_mapper.get(FilterGenericObjectsV2.__name__, "/<string:collection>/filter_v2"),
)
api.add_resource(
    FilterGenericObjectsBySavedSearchId,
    route_mapper.get(
        FilterGenericObjectsBySavedSearchId.__name__,
        "/<string:collection>/filter/<string:id>",
    ),
)

# Initialize RabbitMQ Queues
load_queues(logger)
import resources.queues

if __name__ == "__main__":
    app.run()
