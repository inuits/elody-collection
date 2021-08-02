import uuid


class MemoryStorageManager:
    collections = {"entities": {}, "mediafiles": {}, "tenants": {}}

    def get_items_from_collection(self, collection, skip=0, limit=20, ids=False):
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

    def get_collection_item_sub_item(self, collection, obj_id, sub_item):
        if item := self.get_item_from_collection_by_id(collection, obj_id):
            return item[sub_item] if sub_item in item else []
        return None

    def get_collection_item_sub_item_key(self, collection, obj_id, sub_item, key):
        if obj := self.get_collection_item_sub_item(collection, obj_id, sub_item):
            return list(
                filter(
                    lambda elem: elem["key"] == key,
                    obj,
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

    def add_mediafile_to_collection_item(self, collection, obj_id, mediafile_id):
        if item := self.get_item_from_collection_by_id(collection, obj_id):
            identifiers = item["identifiers"]
            if collection not in self.collections["mediafiles"][mediafile_id]:
                self.collections["mediafiles"][mediafile_id][collection] = identifiers
            else:
                self.collections["mediafiles"][mediafile_id][collection].extend(identifiers)
            return self.collections["mediafiles"][mediafile_id]
        return None

    def add_sub_item_to_collection_item(self, collection, obj_id, sub_item, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id][sub_item].append(content)
            return content
        return None

    def add_relations_to_collection_item(self, collection, id, content):
        self.add_sub_item_to_collection_item(collection, id, "relations", content)
        self._update_child_relations(collection, id, content)
        return self.get_collection_item_sub_item(collection, id, "relations")

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

    def update_collection_item_sub_item(self, collection, obj_id, sub_item, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id][sub_item] = content
            return self.collections[collection][gen_id][sub_item]
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

    def delete_collection_item_sub_item_key(self, collection, obj_id, sub_item, key):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id][sub_item] = list(
                filter(
                    lambda elem: elem["key"] != key,
                    self.collections[collection][gen_id][sub_item],
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

    def _map_relation(self, relation):
        mapping = {"authoredBy": "authored", "isIn": "contains"}
        return mapping.get(relation)

    def _update_child_relations(self, collection, obj_id, relations):
        for relation in relations:
            dst_relation = self._map_relation(relation["type"])
            dst_id = relation["key"]
            dst_content = {"key": obj_id, "type": dst_relation}
            self.add_sub_item_to_collection_item(
                collection, dst_id, "relations", dst_content
            )
