import logging
import os

from flask import Flask
from flask_rabmq import RabbitMQ
from flask_restful import Api
from flask_swagger_ui import get_swaggerui_blueprint
from inuits_jwt_auth.authorization import JWTValidator, MyResourceProtector

SWAGGER_URL = "/api/docs"  # URL for exposing Swagger UI (without trailing '/')
API_URL = (
    "/spec/dams-collection-api.json"  # Our API url (can of course be a local resource)
)

swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL)

app = Flask(__name__)

api = Api(app)

app.config.update(
    {
        "RABMQ_RABBITMQ_URL": os.getenv("RABMQ_RABBITMQ_URL", "amqp://localhost:5672"),
        "RABMQ_SEND_EXCHANGE_NAME": os.getenv("RABMQ_SEND_EXCHANGE_NAME", "dams"),
        "RABMQ_SEND_EXCHANGE_TYPE": "topic",
        "SECRET_KEY": "SomethingNotEntirelySecret",
        "TESTING": True,
        "DEBUG": True,
    }
)

logging.basicConfig(
    format="%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

ramq = RabbitMQ()
ramq.init_app(app=app)
ramq.run_consumer()

require_oauth = MyResourceProtector(
    os.getenv("STATIC_JWT", False),
    {},
    os.getenv("REQUIRE_TOKEN", True) == ("True" or "true" or True),
)
validator = JWTValidator(
    logger,
    os.getenv("STATIC_JWT", False),
    os.getenv("STATIC_ISSUER", False),
    os.getenv("STATIC_PUBLIC_KEY", False),
    os.getenv("REALMS", "").split(","),
    os.getenv("REQUIRE_TOKEN", True) == ("True" or "true" or True),
)
require_oauth.register_token_validator(validator)

app.register_blueprint(swaggerui_blueprint)

from resources.entity import (
    Entity,
    EntityComponents,
    EntityDetail,
    BoxVisit,
    BoxVisitDetail,
    EntityMediafiles,
    EntitySetPrimaryMediafile,
    EntitySetPrimaryThumbnail,
    EntityMediafilesCreate,
    EntityMetadata,
    EntityMetadataKey,
    EntityParent,
    EntityRelations,
    EntityTypes,
    EntityUsage,
)
from resources.job import Job, JobDetail
from resources.key_value_store import KeyValueStore, KeyValueStoreDetail
from resources.mediafile import Mediafile, MediafileDetail
from resources.spec import AsyncAPISpec, OpenAPISpec
from resources.tenant import Tenant, TenantDetail

api.add_resource(Entity, "/entities")
api.add_resource(EntityComponents, "/entities/<string:id>/components")
api.add_resource(EntityDetail, "/entities/<string:id>")
api.add_resource(BoxVisit, "/box_visit")
api.add_resource(BoxVisitDetail, "/box_visit/<string:id>")
api.add_resource(
    EntitySetPrimaryMediafile,
    "/entities/<string:id>/set_primary_mediafile/<string:mediafile_id>",
)
api.add_resource(
    EntitySetPrimaryThumbnail,
    "/entities/<string:id>/set_primary_thumbnail/<string:mediafile_id>",
)

api.add_resource(EntityMediafiles, "/entities/<string:id>/mediafiles")
api.add_resource(EntityMediafilesCreate, "/entities/<string:id>/mediafiles/create")
api.add_resource(EntityMetadata, "/entities/<string:id>/metadata")
api.add_resource(EntityMetadataKey, "/entities/<string:id>/metadata/<string:key>")
api.add_resource(EntityParent, "/entities/<string:id>/parent")
api.add_resource(EntityRelations, "/entities/<string:id>/relations")
api.add_resource(EntityTypes, "/entities/<string:id>/types")
api.add_resource(EntityUsage, "/entities/<string:id>/usage")

api.add_resource(Job, "/jobs")
api.add_resource(JobDetail, "/jobs/<string:id>")

api.add_resource(KeyValueStore, "/key_value_store")
api.add_resource(KeyValueStoreDetail, "/key_value_store/<string:id>")

api.add_resource(Mediafile, "/mediafiles")
api.add_resource(MediafileDetail, "/mediafiles/<string:id>")

api.add_resource(AsyncAPISpec, "/spec/dams-collection-api-events.html")
api.add_resource(OpenAPISpec, "/spec/dams-collection-api.json")

api.add_resource(Tenant, "/tenants")
api.add_resource(TenantDetail, "/tenants/<string:id>")

if __name__ == "__main__":
    app.run(debug=True)
