from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext

api_bp = Blueprint("user", __name__)
api = Api(api_bp)


class UserPermissions(CoghentBaseResource):
    @policy_factory.apply_policies(RequestContext(request, ["get-user-permissions"]))
    def get(self):
        return policy_factory.get_user_context().scopes


api.add_resource(UserPermissions, "/user/permissions")
