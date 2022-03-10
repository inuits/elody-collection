import json
import logging
import os

from flask import Flask
from flask_restful import Api
from flask_swagger_ui import get_swaggerui_blueprint
from inuits_jwt_auth.authorization import JWTValidator, MyResourceProtector
from storage.storagemanager import StorageManager
from rabbitmq_pika_flask import RabbitMQ

SWAGGER_URL = "/api/docs"  # URL for exposing Swagger UI (without trailing '/')
API_URL = (
    "/spec/dams-collection-api.json"  # Our API url (can of course be a local resource)
)

swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL)

app = Flask(__name__)

api = Api(app)

app.config.update(
    {
        "MQ_EXCHANGE": os.getenv("RABMQ_SEND_EXCHANGE_NAME", "dams"),
        "MQ_URL": os.getenv("RABMQ_RABBITMQ_URL", "amqp://localhost:5672"),
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

rabbit = RabbitMQ()
rabbit.init_app(app, "basic", json.loads, json.dumps)


@rabbit.queue("dams.mediafile_changed")
def mediafile_changed(routing_key, body, message_id):
    data = body["data"]
    if "old_mediafile" not in data or "mediafile" not in data:
        logger.error("Message malformed: missing 'old_mediafile' or 'mediafile'")
        return True
    StorageManager().get_db_engine().handle_mediafile_status_change(
        data["old_mediafile"], data["mediafile"]
    )
    StorageManager().get_db_engine().reindex_mediafile_parents(data["mediafile"])
    return True


require_oauth = MyResourceProtector(
    os.getenv("STATIC_JWT", False),
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
    EntityDetail,
    EntityMediafiles,
    EntitySetPrimaryMediafile,
    EntitySetPrimaryThumbnail,
    EntityMediafilesCreate,
    EntityMetadata,
    EntityMetadataKey,
    EntityRelations,
    EntityRelationsAll,
)
from resources.job import Job, JobDetail
from resources.box_visit import (
    BoxVisit,
    BoxVisitDetail,
    BoxVisitRelations,
    BoxVisitRelationsAll,
)
from resources.key_value_store import KeyValueStore, KeyValueStoreDetail
from resources.mediafile import Mediafile, MediafileDetail, MediafileCopyright
from resources.spec import AsyncAPISpec, OpenAPISpec
from resources.tenant import Tenant, TenantDetail

api.add_resource(Entity, "/entities")
api.add_resource(EntityDetail, "/entities/<string:id>")
api.add_resource(BoxVisit, "/box_visits")
api.add_resource(BoxVisitDetail, "/box_visits/<string:id>")
api.add_resource(BoxVisitRelations, "/box_visits/<string:id>/relations")
api.add_resource(BoxVisitRelationsAll, "/box_visits/<string:id>/relations/all")
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
api.add_resource(EntityRelations, "/entities/<string:id>/relations")
api.add_resource(EntityRelationsAll, "/entities/<string:id>/relations/all")

api.add_resource(Job, "/jobs")
api.add_resource(JobDetail, "/jobs/<string:id>")

api.add_resource(KeyValueStore, "/key_value_store")
api.add_resource(KeyValueStoreDetail, "/key_value_store/<string:id>")

api.add_resource(Mediafile, "/mediafiles")
api.add_resource(MediafileDetail, "/mediafiles/<string:id>")
api.add_resource(MediafileCopyright, "/mediafiles/<string:id>/copyright")

api.add_resource(AsyncAPISpec, "/spec/dams-collection-api-events.html")
api.add_resource(OpenAPISpec, "/spec/dams-collection-api.json")

api.add_resource(Tenant, "/tenants")
api.add_resource(TenantDetail, "/tenants/<string:id>")

if __name__ == "__main__":
    app.run(debug=True)
