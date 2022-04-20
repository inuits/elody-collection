import json
import logging
import os

from flask import Flask
from flask_restful import Api
from flask_swagger_ui import get_swaggerui_blueprint
from healthcheck import HealthCheck
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
        "MQ_EXCHANGE": os.getenv("RABMQ_SEND_EXCHANGE_NAME"),
        "MQ_URL": os.getenv("RABMQ_RABBITMQ_URL"),
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


def database_available():
    return True, StorageManager().get_db_engine().check_health().json()


def rabbit_available():
    return True, rabbit.get_connection().is_open


health = HealthCheck()
if os.getenv("HEALTH_CHECK_EXTERNAL_SERVICES", True) in ["True", "true", True]:
    health.add_check(database_available)
    health.add_check(rabbit_available)
app.add_url_rule("/health", "healthcheck", view_func=lambda: health.run())


@rabbit.queue("dams.child_relation_changed")
def child_relation_changed(routing_key, body, message_id):
    data = body["data"]
    if "collection" not in data or "parent_id" not in data:
        logger.error("Message malformed: missing 'collection' or 'parent_id'")
        return
    StorageManager().get_db_engine().update_parent_relation_values(
        data["collection"], data["parent_id"]
    )


@rabbit.queue("dams.mediafile_changed")
def mediafile_changed(routing_key, body, message_id):
    data = body["data"]
    if "old_mediafile" not in data or "mediafile" not in data:
        logger.error("Message malformed: missing 'old_mediafile' or 'mediafile'")
        return
    StorageManager().get_db_engine().handle_mediafile_status_change(
        data["old_mediafile"], data["mediafile"]
    )
    StorageManager().get_db_engine().reindex_mediafile_parents(data["mediafile"])


@rabbit.queue("dams.mediafile_deleted")
def mediafile_deleted(routing_key, body, message_id):
    data = body["data"]
    if "mediafile" not in data or "linked_entities" not in data:
        logger.error("Message malformed: missing 'mediafile' or 'linked_entities'")
        return
    StorageManager().get_db_engine().handle_mediafile_deleted(data["linked_entities"])
    StorageManager().get_db_engine().reindex_mediafile_parents(
        parents=data["linked_entities"]
    )


require_oauth = MyResourceProtector(
    os.getenv("REQUIRE_TOKEN", True) == ("True" or "true" or True),
)
validator = JWTValidator(
    logger,
    os.getenv("STATIC_ISSUER", False),
    os.getenv("STATIC_PUBLIC_KEY", False),
    os.getenv("REALMS", "").split(","),
    os.getenv("ROLE_PERMISSION_FILE", "role_permission.json"),
    os.getenv("SUPER_ADMIN_ROLE", "role_super_admin"),
    os.getenv("REMOTE_TOKEN_VALIDATION", False),
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


@app.after_request
def add_header(response):
    response.headers["Jaeger-trace-id"] = os.getenv("JAEGER_TRACE_ID", "default-id")
    return response


if __name__ == "__main__":
    app.run(debug=True)
