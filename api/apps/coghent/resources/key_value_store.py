import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api
from resources.key_value_store import KeyValueStore, KeyValueStoreDetail

api_bp = Blueprint("key_value_store", __name__)
api = Api(api_bp)


class CoghentKeyValueStore(CoghentBaseResource, KeyValueStore):
    @app.require_oauth("create-key-value-store")
    def post(self):
        return super().post()


class CoghentKeyValueStoreDetail(CoghentBaseResource, KeyValueStoreDetail):
    @app.require_oauth("read-key-value-store")
    def get(self, id):
        return super().get(id)

    @app.require_oauth("update-key-value-store")
    def put(self, id):
        return super().put(id)

    @app.require_oauth("patch-key-value-store")
    def patch(self, id):
        return super().patch(id)

    @app.require_oauth("delete-key-value-store")
    def delete(self, id):
        return super().delete(id)


api.add_resource(CoghentKeyValueStore, "/key_value_store")
api.add_resource(CoghentKeyValueStoreDetail, "/key_value_store/<string:id>")
