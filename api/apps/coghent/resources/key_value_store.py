from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext
from resources.key_value_store import KeyValueStore, KeyValueStoreDetail

api_bp = Blueprint("key_value_store", __name__)
api = Api(api_bp)


class CoghentKeyValueStore(CoghentBaseResource, KeyValueStore):
    @policy_factory.apply_policies(RequestContext(request, ["create-key-value-store"]))
    def post(self):
        return super().post()


class CoghentKeyValueStoreDetail(CoghentBaseResource, KeyValueStoreDetail):
    @policy_factory.apply_policies(RequestContext(request, ["read-key-value-store"]))
    def get(self, id):
        return super().get(id)

    @policy_factory.apply_policies(RequestContext(request, ["update-key-value-store"]))
    def put(self, id):
        return super().put(id)

    @policy_factory.apply_policies(RequestContext(request, ["patch-key-value-store"]))
    def patch(self, id):
        return super().patch(id)

    @policy_factory.apply_policies(RequestContext(request, ["delete-key-value-store"]))
    def delete(self, id):
        return super().delete(id)


api.add_resource(CoghentKeyValueStore, "/key_value_store")
api.add_resource(CoghentKeyValueStoreDetail, "/key_value_store/<string:id>")
