import app
import util

from flask import request
from resources.base_resource import BaseResource
from validator import key_value_store_schema


class KeyValueStore(BaseResource):
    @app.require_oauth()
    def post(self):
        content = request.get_json()
        self._abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        key_value_store = self.storage.save_item_to_collection(
            "key_value_store", content
        )
        return key_value_store, 201


class KeyValueStoreDetail(BaseResource):
    @app.require_oauth()
    def get(self, id):
        return self._abort_if_item_doesnt_exist("key_value_store", id)

    @app.require_oauth()
    def put(self, id):
        kvs = self._abort_if_item_doesnt_exist("key_value_store", id)
        content = request.get_json()
        self._abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        key_value_store = self.storage.update_item_from_collection(
            "key_value_store", util.get_raw_id(kvs), content
        )
        return key_value_store, 201

    @app.require_oauth()
    def patch(self, id):
        kvs = self._abort_if_item_doesnt_exist("key_value_store", id)
        content = request.get_json()
        self._abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        key_value_store = self.storage.patch_item_from_collection(
            "key_value_store", util.get_raw_id(kvs), content
        )
        return key_value_store, 201

    @app.require_oauth()
    def delete(self, id):
        kvs = self._abort_if_item_doesnt_exist("key_value_store", id)
        self.storage.delete_item_from_collection(
            "key_value_store", util.get_raw_id(kvs)
        )
        return "", 204
