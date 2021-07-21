import uuid


class MemoryStorageManager:
    collections = {"config": {}, "entities": {}, "mediafiles": {}, "tenants": {}}

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        count = len(self.collections[collection])
        items["count"] = count
        items["results"] = list(self.collections[collection].values())[
            skip : skip + limit
        ]
        return items

    def get_item_from_collection_by_id(self, collection, obj_id):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            return self.collections[collection].get(gen_id, None)
        return None

    def get_collection_item_metadata(self, collection, obj_id):
        if item := self.get_item_from_collection_by_id(collection, obj_id):
            return item["metadata"] if "metadata" in item else []
        return None

    def get_collection_item_metadata_key(self, collection, obj_id, key):
        if metadata := self.get_collection_item_metadata(collection, obj_id):
            return list(
                filter(
                    lambda elem: elem["key"] == key,
                    metadata,
                )
            )
        return None

    def get_collection_item_mediafiles(self, collection, obj_id):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            return list(
                filter(
                    lambda elem: gen_id in elem[collection],
                    self.collections["mediafiles"].values(),
                )
            )
        return None

    def add_collection_item_metadata(self, collection, obj_id, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id]["metadata"].append(content)
            return content
        return None

    def add_mediafile_to_entity(self, collection, obj_id, mediafile_id):
        if item := self.get_item_from_collection_by_id(collection, obj_id):
            identifiers = item["identifiers"]
            self.collections["mediafiles"][mediafile_id][collection] = identifiers
            return self.collections["mediafiles"][mediafile_id]
        return None

    def save_item_to_collection(self, collection, content):
        gen_id = str(uuid.uuid4())
        content["_id"] = gen_id
        if "identifiers" in content:
            content["identifiers"].insert(0, gen_id)
        self.collections[collection][gen_id] = content
        return self.collections[collection][gen_id]

    def update_item_from_collection(self, collection, obj_id, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id] = content
            return self.collections[collection][gen_id]
        return None

    def update_collection_item_metadata(self, collection, obj_id, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id]["metadata"] = content
            return self.collections[collection][gen_id]["metadata"]
        return None

    def patch_item_from_collection(self, collection, obj_id, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            for key in content:
                self.collections[collection][gen_id][key] = content[key]
            return self.collections[collection][gen_id]
        return None

    def delete_item_from_collection(self, collection, obj_id):
        gen_id = self._get_collection_item_gen_id_by_identifier(collection, obj_id)
        self.collections[collection].pop(gen_id, None)

    def delete_collection_item_metadata_key(self, collection, obj_id, key):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id]["metadata"] = list(
                filter(
                    lambda elem: elem["key"] != key,
                    self.collections[collection][gen_id]["metadata"],
                )
            )

    def drop_all_collections(self):
        [self.collections[collection].clear() for collection in self.collections]

    def _get_collection_item_gen_id_by_identifier(self, collection, obj_id):
        for item in self.collections[collection].values():
            if ("identifiers" in item and obj_id in item["identifiers"]) or item[
                "_id"
            ] == obj_id:
                return item["_id"]
        return None
