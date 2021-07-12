import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask
import logging

from flask_jwt_extended import JWTManager
from flask_restful_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint
from flask import Flask
from flask_mongoalchemy import MongoAlchemy
from flask_oidc import OpenIDConnect
from flask_rabmq import RabbitMQ
from flask_restful import Api

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask_apispec.extension import FlaskApiSpec

load_dotenv()
load_dotenv(dotenv_path=Path('.') / '.env')
app = Flask(__name__)
# SWAGGER_UI
WEB_DOCS_URL = "/api/job-status/docs"
JSON_DOCS_URL = "/job-status/docs"

blueprint = get_swaggerui_blueprint(
    WEB_DOCS_URL, JSON_DOCS_URL, config={"app_name": "Job Status API"}
)
api = swagger.docs(
    Api(app),
    apiVersion="0.1",
    basePath="http://localhost:8700",
    resourcePath="/",
    produces=["application/json", "text/html"],
    api_spec_url="/api/spec",
    description="The Job Status API",
)

logging.basicConfig(
    format="%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

app.config.setdefault('RABMQ_RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')
app.config.setdefault('RABMQ_SEND_EXCHANGE_NAME', 'dams')
app.config.setdefault('RABMQ_SEND_EXCHANGE_TYPE', 'topic')
app.config.setdefault('RABMQ_SEND_POOL_SIZE', 2)
app.config.setdefault('RABMQ_SEND_POOL_ACQUIRE_TIMEOUT', 5)
app.config['MONGOALCHEMY_DATABASE'] = 'library'
app.config.update(
    {
        "SECRET_KEY": os.getenv("SECRET_KEY"),
        "TESTING": True,
        "DEBUG": True,
        # Uncomment thses settings for openIDConnect

        'OIDC_CLIENT_SECRETS': 'client_secrets.json',
        'OIDC_ID_TOKEN_COOKIE_SECURE': False,
        'OIDC_REQUIRE_VERIFIED_EMAIL': False,
        'OIDC_USER_INFO_ENABLED': True,
        'OIDC_OPENID_REALM': 'flask-demo',
        'OIDC_SCOPES': ['openid', 'email', 'profile'],
        'OIDC_INTROSPECTION_AUTH_METHOD': 'client_secret_post'
    }
)

oidc = OpenIDConnect(app)
# Set Flask-Restful API
# api = Api(app)
# Set Database ORM
database = MongoAlchemy(app)
# Set RabitMQ
ramq = RabbitMQ()
ramq.init_app(app=app)
ramq.run_consumer()

app.config.update({
    'APISPEC_SPEC': APISpec(
        title='Job Status API',
        version='v1',
        plugins=[MarshmallowPlugin()],
        openapi_version='2.0.0'
    ),
    'APISPEC_SWAGGER_URL': '/job-status/',  # JSON view
    'APISPEC_SWAGGER_UI_URL': '/api/docs'  # HTML View
})

api_documentation = FlaskApiSpec(app)
