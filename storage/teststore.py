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

    def get_collection_item_metadata(self, collection, id):
        return self.collections[collection][id]["metadata"]

    def get_collection_item_metadata_key(self, collection, id, key):
        return list(
            filter(
                lambda elem: elem["key"] == key,
                self.get_collection_item_metadata(collection, id),
            )
        )

    def save_item_to_collection(self, collection, content):
        id = str(uuid.uuid4())
        content["_id"] = id
        self.collections[collection][id] = content
        return self.collections[collection][id]

    def update_item_from_collection(self, collection, id, content):
        self.collections[collection][id] = content
        return self.collections[collection][id]

    def patch_item_from_collection(self, collection, id, content):
        entity = self.collections[collection][id]
        for key in content:
            if key in entity:
                entity[key] = content[key]
        return entity

    def delete_item_from_collection(self, collection, id):
        self.collections[collection].pop(id, None)

    def drop_collection(self, collection):
        self.collections[collection].clear()

    def drop_all_collections(self):
        for collection in self.collections:
            self.drop_collection(collection)
