import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api

api_bp = Blueprint("user", __name__)
api = Api(api_bp)


class UserPermissions(CoghentBaseResource):
    @app.require_oauth("get-user-permissions")
    def get(self):
        return app.require_oauth.get_token_permissions(
            app.validator.role_permission_mapping
        )


api.add_resource(UserPermissions, "/user/permissions")
