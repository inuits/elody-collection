from flask import Flask
from flask_oidc import OpenIDConnect
from flask_rabmq import RabbitMQ
from flask_restful import Api
from flask_swagger_ui import get_swaggerui_blueprint

import logging
import os

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
        "OIDC_CLIENT_SECRETS": "client_secrets.json",
        "OIDC_ID_TOKEN_COOKIE_SECURE": False,
        "OIDC_REQUIRE_VERIFIED_EMAIL": False,
        "OIDC_USER_INFO_ENABLED": True,
        "OIDC_OPENID_REALM": os.getenv("OIDC_OPENID_REALM"),
        "OIDC_SCOPES": ["openid", "email", "profile"],
        "OIDC_INTROSPECTION_AUTH_METHOD": "client_secret_post",
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

oidc = OpenIDConnect(app)

app.register_blueprint(swaggerui_blueprint)

from resources.entity import (
    Entity,
    EntityDetail,
    EntityMetadata,
    EntityMetadataKey,
    EntityMediafiles,
    EntityMediafilesCreate,
    EntityRelations,
)
from resources.importer import (
    ImporterStart,
    ImporterDirectories,
)
from resources.mediafile import Mediafile, MediafileDetail
from resources.spec import OpenAPISpec, AsyncAPISpec
from resources.tenant import Tenant, TenantDetail

api.add_resource(EntityRelations, "/entities/<string:id>/relations")
api.add_resource(EntityMediafilesCreate, "/entities/<string:id>/mediafiles/create")
api.add_resource(EntityMediafiles, "/entities/<string:id>/mediafiles")
api.add_resource(EntityMetadataKey, "/entities/<string:id>/metadata/<string:key>")
api.add_resource(EntityMetadata, "/entities/<string:id>/metadata")
api.add_resource(EntityDetail, "/entities/<string:id>")
api.add_resource(Entity, "/entities")

api.add_resource(ImporterStart, "/importer/start")
api.add_resource(ImporterDirectories, "/importer/directories")

api.add_resource(MediafileDetail, "/mediafiles/<string:id>")
api.add_resource(Mediafile, "/mediafiles")

api.add_resource(OpenAPISpec, "/spec/dams-collection-api.json")
api.add_resource(AsyncAPISpec, "/spec/dams-collection-api-events.html")

api.add_resource(TenantDetail, "/tenants/<string:id>")
api.add_resource(Tenant, "/tenants")

if __name__ == "__main__":
    app.run(debug=True)
