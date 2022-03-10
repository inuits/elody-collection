import app
from validator import key_value_store_schema
from resources.base_resource import BaseResource


class KeyValueStore(BaseResource):
    @app.require_oauth()
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        KeyValueStore = self.storage.save_item_to_collection("key_value_store", content)
        return KeyValueStore, 201


class KeyValueStoreDetail(BaseResource):
    @app.require_oauth()
    def get(self, id):
        KeyValueStore = self.abort_if_item_doesnt_exist("key_value_store", id)
        return KeyValueStore

    @app.require_oauth()
    def patch(self, id):
        self.abort_if_item_doesnt_exist("key_value_store", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        KeyValueStore = self.storage.patch_item_from_collection(
            "key_value_store", id, content
        )
        return KeyValueStore, 201

    @app.require_oauth()
    def put(self, id):
        self.abort_if_item_doesnt_exist("key_value_store", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("KeyValueStore", content, key_value_store_schema)
        KeyValueStore = self.storage.update_item_from_collection(
            "key_value_store", id, content
        )
        return KeyValueStore, 201

    @app.require_oauth()
    def delete(self, id):
        self.abort_if_item_doesnt_exist("key_value_store", id)
        self.storage.delete_item_from_collection("key_value_store", id)
        return "", 204
