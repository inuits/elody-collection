import uuid


class TestStorageManager:
    collections = {"entities": {}, "tenants": {}, "mediafiles": {}}

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        count = len(self.collections[collection])
        items["count"] = count
        items["results"] = list(self.collections[collection].values())[
            skip : skip + limit
        ]
        return items

    def get_item_from_collection_by_id(self, collection, id):
        return self.collections[collection].get(id, None)

    def save_item_to_collection(self, collection, content):
        id = str(uuid.uuid4())
        content["_id"] = id
        self.collections[collection][id] = content
        return self.collections[collection][id]

    def update_item_from_collection(self, collection, id, content):
        self.collections[collection][id] = content
        return self.collections[collection][id]

    def delete_item_from_collection(self, collection, id):
        self.collections[collection].pop(id, None)

    def drop_collection(self, collection):
        self.collections[collection].clear()

    def drop_all_collections(self):
        for collection in self.collections:
            self.drop_collection(collection)
