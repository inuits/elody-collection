import app

from resources.base_resource import BaseResource
from validator import key_value_store_schema


class KeyValueStore(BaseResource):
    @app.require_oauth("create-key-value-store")
    def post(self):
        content = self._get_request_body()
        self._abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        key_value_store = self.storage.save_item_to_collection(
            "key_value_store", content
        )
        return key_value_store, 201


class KeyValueStoreDetail(BaseResource):
    @app.require_oauth("read-key-value-store")
    def get(self, id):
        return self._abort_if_item_doesnt_exist("key_value_store", id)

    @app.require_oauth("update-key-value-store")
    def put(self, id):
        kvs = self._abort_if_item_doesnt_exist("key_value_store", id)
        content = self._get_request_body()
        self._abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        key_value_store = self.storage.update_item_from_collection(
            "key_value_store", self._get_raw_id(kvs), content
        )
        return key_value_store, 201

    @app.require_oauth("patch-key-value-store")
    def patch(self, id):
        kvs = self._abort_if_item_doesnt_exist("key_value_store", id)
        content = self._get_request_body()
        self._abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        key_value_store = self.storage.patch_item_from_collection(
            "key_value_store", self._get_raw_id(kvs), content
        )
        return key_value_store, 201

    @app.require_oauth("delete-key-value-store")
    def delete(self, id):
        kvs = self._abort_if_item_doesnt_exist("key_value_store", id)
        self.storage.delete_item_from_collection(
            "key_value_store", self._get_raw_id(kvs)
        )
        return "", 204
