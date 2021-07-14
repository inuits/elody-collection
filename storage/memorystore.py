import uuid


class MemoryStorageManager:
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
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        if main_id:
            return self.collections[collection].get(main_id, None)
        return None

    def get_collection_item_metadata(self, collection, id):
        collection_item = self.get_item_from_collection_by_id(collection, id)
        if collection_item and "metadata" in collection_item:
            return collection_item["metadata"]
        return None

    def get_collection_item_metadata_key(self, collection, id, key):
        if self.get_item_from_collection_by_id(collection, id):
            return list(
                filter(
                    lambda elem: elem["key"] == key,
                    self.get_collection_item_metadata(collection, id),
                )
            )
        return None

    def get_collection_item_mediafiles(self, collection, id):
        if self.get_item_from_collection_by_id(collection, id):
            return list(
                filter(
                    lambda elem: id in elem[collection],
                    self.collections["mediafiles"].values(),
                )
            )
        return None

    def add_collection_item_metadata(self, collection, id, content):
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        if main_id:
            self.collections[collection][main_id]["metadata"].append(content)
            return content
        return None

    def add_mediafile_to_entity(self, collection, id, mediafile_id):
        collection_item = self._get_collection_item_by_id(collection, id)
        if collection_item:
            identifiers = collection_item["identifiers"]
            if collection_item["_id"] not in identifiers:
                identifiers.append(collection_item["_id"])
            self.collections["mediafiles"][mediafile_id][collection] = identifiers
            return self.collections["mediafiles"][mediafile_id]
        return None

    def save_item_to_collection(self, collection, content):
        id = str(uuid.uuid4())
        content["_id"] = id
        self.collections[collection][id] = content
        return self.collections[collection][id]

    def update_item_from_collection(self, collection, id, content):
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        if main_id:
            self.collections[collection][main_id] = content
            return self.collections[collection][main_id]
        return None

    def update_collection_item_metadata(self, collection, id, content):
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        if main_id:
            self.collections[collection][main_id]["metadata"] = content
            return content
        return None

    def patch_item_from_collection(self, collection, id, content):
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        if main_id:
            item = self.collections[collection][main_id]
            for key in content:
                item[key] = content[key]
            return item
        return None

    def delete_item_from_collection(self, collection, id):
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        self.collections[collection].pop(main_id, None)

    def delete_collection_item_metadata_key(self, collection, id, key):
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        if main_id:
            self.collections[collection][main_id]["metadata"] = list(
                filter(
                    lambda elem: elem["key"] != key,
                    self.get_collection_item_metadata(collection, main_id),
                )
            )

    def drop_collection(self, collection):
        self.collections[collection].clear()

    def drop_all_collections(self):
        for collection in self.collections:
            self.drop_collection(collection)

    def _get_collection_item_by_id(self, collection, id):
        main_id = self._get_collection_item_main_id_by_identifier(collection, id)
        if main_id:
            return self.collections[collection][main_id]
        return None

    def _get_collection_item_main_id_by_identifier(self, collection, id):
        for collection_item in self.collections[collection].values():
            if (
                "identifiers" in collection_item
                and id in collection_item["identifiers"]
            ) or collection_item["_id"] == id:
                return collection_item["_id"]
        return None
