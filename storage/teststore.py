import uuid


class TestStorageManager:
    collections = {"entities": {}, "tenants": {}, "mediafiles": {}}

    def get_item_from_collection_by_id(self, collection, id):
        return self.collections[collection].get(id, None)

    def save_item_to_collection(self, collection, content):
        id = str(uuid.uuid4())
        content["_id"] = id
        self.collections[collection][id] = content
        return self.collections[collection][id]

    def delete_item_from_collection(self, collection, id):
        self.collections[collection].pop(id, None)

    def drop_collection(self, collection):
        self.collections[collection].clear()

    def drop_all_collections(self):
        for collection in self.collections:
            self.drop_collection(collection)
