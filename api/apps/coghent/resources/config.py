from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext
from resources.config import Config

api_bp = Blueprint("config", __name__)
api = Api(api_bp)


class CoghentConfig(CoghentBaseResource, Config):
    @policy_factory.apply_policies(RequestContext(request, ["read-config"]))
    def get(self):
        return super().get()


api.add_resource(CoghentConfig, "/config")
