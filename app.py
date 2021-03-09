from flask import Flask
from flask_restful import Api
from flask_restful_swagger import swagger
from flask_oidc import OpenIDConnect

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

#sentry_sdk.init(
#    os.getenv('SENTRY_DSN'),
#    integrations=[FlaskIntegration()]
#)

app = Flask(__name__)

api = swagger.docs(
    Api(app),
    apiVersion='0.1',
    basePath="http://localhost:8000",
    resourcePath="/",
    produces=["application/json", "text/html"],
    api_spec_url="/api/spec",
    description="The DAMS collection API",
)

app.config.update({
    'SECRET_KEY': 'SomethingNotEntirelySecret',
    'TESTING': True,
    'DEBUG': True,
    'OIDC_CLIENT_SECRETS': 'client_secrets.json',
    'OIDC_ID_TOKEN_COOKIE_SECURE': False,
    'OIDC_REQUIRE_VERIFIED_EMAIL': False,
    'OIDC_USER_INFO_ENABLED': True,
    'OIDC_OPENID_REALM': 'mediamosa-ng',
    'OIDC_SCOPES': ['openid', 'email', 'profile'],
    'OIDC_INTROSPECTION_AUTH_METHOD': 'client_secret_post'
})

oidc = OpenIDConnect(app)

from resources.tenant import Tenant, TenantDetail
from resources.entity import Entity, EntityDetail, EntityMetadata, EntityMetadataKey
from resources.mediafile import Mediafile, MediafileDetail

api.add_resource(TenantDetail, '/tenants/<string:id>')
api.add_resource(Tenant, '/tenants')

api.add_resource(EntityMetadataKey, '/entities/<string:id>/metadata/<string:key>')
api.add_resource(EntityMetadata, '/entities/<string:id>/metadata')
api.add_resource(EntityDetail, '/entities/<string:id>')
api.add_resource(Entity, '/entities')

api.add_resource(MediafileDetail, '/mediafiles/<string:id>')
api.add_resource(Mediafile, '/mediafiles')

if __name__ == '__main__':
    app.run(debug=True)
