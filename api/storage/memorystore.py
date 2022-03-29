import uuid

from copy import deepcopy


class MemoryStorageManager:
    collections = {"entities": {}, "mediafiles": {}, "tenants": {}, "jobs": {}}

    def get_entities(
            self, skip=0, limit=20, item_type=None, ids=None, skip_relations=0
    ):
        items = dict()
        items["results"] = list(self.collections["entities"].values())
        if ids:
            items["results"] = [
                self.collections["entities"].get(id)
                for id in ids
                if id in self.collections["entities"]
            ]
        if item_type:
            items["results"] = list(
                filter(lambda elem: elem["type"] == item_type, items["results"])
            )
        for entity in items["results"]:
            mediafiles = self.get_collection_item_mediafiles("entities", entity["_id"])
            for mediafile in mediafiles:
                if "is_primary" in mediafile and mediafile["is_primary"] is True:
                    entity["primary_mediafile_location"] = mediafile[
                        "original_file_location"
                    ]
                if (
                        "is_primary_thumbnail" in mediafile
                        and mediafile["is_primary_thumbnail"] is True
                ):
                    entity["primary_thumbnail_location"] = mediafile[
                        "thumbnail_file_location"
                    ]
        items["count"] = len(items["results"])
        items["results"] = items["results"][skip: skip + limit]
        return deepcopy(items)

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        results = list(self.collections[collection].values())
        items["count"] = len(results)
        items["results"] = results[skip: skip + limit]
        return deepcopy(items)

    def get_item_from_collection_by_id(self, collection, obj_id):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            return deepcopy(self.collections[collection].get(gen_id, None))
        return None

    def get_collection_item_sub_item(self, collection, obj_id, sub_item):
        if item := self.get_item_from_collection_by_id(collection, obj_id):
            return deepcopy(item[sub_item]) if sub_item in item else []
        return None

    def get_collection_item_sub_item_key(self, collection, obj_id, sub_item, key):
        if obj := self.get_collection_item_sub_item(collection, obj_id, sub_item):
            return deepcopy(
                list(
                    filter(
                        lambda elem: elem["key"] == key,
                        obj,
                    )
                )
            )
        return None

    def get_collection_item_relations(self, collection, obj_id):
        return self.get_collection_item_sub_item(collection, obj_id, "relations")

    def get_collection_item_mediafiles(self, collection, obj_id):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            return deepcopy(
                list(
                    filter(
                        lambda elem: gen_id in elem[collection],
                        self.collections["mediafiles"].values(),
                    )
                )
            )
        return None

    def add_mediafile_to_collection_item(
            self, collection, obj_id, mediafile_id, mediafile_public
    ):
        if item := self.get_item_from_collection_by_id(collection, obj_id):
            identifiers = item["identifiers"]
            if collection not in self.collections["mediafiles"][mediafile_id]:
                self.collections["mediafiles"][mediafile_id][collection] = identifiers
            else:
                self.collections["mediafiles"][mediafile_id][collection].extend(
                    identifiers
                )
            return self.collections["mediafiles"][mediafile_id]
        return None

    def add_sub_item_to_collection_item(self, collection, obj_id, sub_item, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            if sub_item not in self.collections[collection][obj_id]:
                self.collections[collection][gen_id][sub_item] = content
            else:
                self.collections[collection][gen_id][sub_item].extend(content)
            return content
        return None

    def add_relations_to_collection_item(self, collection, id, content):
        self.add_sub_item_to_collection_item(collection, id, "relations", content)
        self._add_child_relations(collection, id, content)
        return self.get_collection_item_sub_item(collection, id, "relations")

    def save_item_to_collection(self, collection, content):
        gen_id = str(uuid.uuid4())
        content["_id"] = gen_id
        if "identifiers" not in content:
            content["identifiers"] = [gen_id]
        else:
            content["identifiers"].insert(0, gen_id)
        self.collections[collection][gen_id] = content
        return self.collections[collection][gen_id]

    def update_item_from_collection(self, collection, obj_id, content):
        if gen_id := self._get_collection_item_gen_id_by_identifier(collection, obj_id):
            self.collections[collection][gen_id] = content
            return self.collections[collection][gen_id]
        return None

    def update_collection_item_sub_item(self, collection, obj_id, sub_item, content):
        patch_data = {sub_item: content}
        self.patch_item_from_collection(collection, obj_id, patch_data)
        return content

    def update_collection_item_relations(self, collection, obj_id, content):
        for item in self.get_collection_item_sub_item(collection, obj_id, "relations"):
            self.delete_collection_item_sub_item_key(
                collection, item["key"], "relations", obj_id
            )
        self.update_collection_item_sub_item(collection, obj_id, "relations", content)
        self._add_child_relations(collection, obj_id, content)
        return content

    def patch_collection_item_relations(self, collection, obj_id, content):
        for item in content:
            self.delete_collection_item_sub_item_key(
                collection, obj_id, "relations", item["key"]
            )
            self.delete_collection_item_sub_item_key(
                collection, item["key"], "relations", obj_id
            )
        relations = self.get_collection_item_sub_item(collection, obj_id, "relations")
        self.update_collection_item_sub_item(
            collection, obj_id, "relations", relations + content
        )
        self._add_child_relations(collection, obj_id, content)
        return content

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
        patch_data = {sub_item: []}
        all_sub_items = self.get_collection_item_sub_item(collection, obj_id, sub_item)
        for obj in all_sub_items:
            if obj["key"] != key:
                patch_data[sub_item].append(obj)
        self.patch_item_from_collection(collection, obj_id, patch_data)

    def drop_all_collections(self):
        [self.collections[collection].clear() for collection in self.collections]

    def handle_mediafile_status_change(self, old_mediafile, mediafile):
        return

    def reindex_mediafile_parents(self, mediafile):
        return

    def _get_collection_item_gen_id_by_identifier(self, collection, obj_id):
        for item in self.collections[collection].values():
            if ("identifiers" in item and obj_id in item["identifiers"]) or item[
                "_id"
            ] == obj_id:
                return deepcopy(item["_id"])
        return None

    def _map_entity_relation(self, relation):
        mapping = {
            "authoredBy": "authored",
            "isIn": "contains",
            "authored": "authoredBy",
            "contains": "isIn",
        }
        return mapping.get(relation)

    def _add_child_relations(self, collection, obj_id, relations):
        for relation in relations:
            dst_relation = self._map_entity_relation(relation["type"])
            dst_id = relation["key"]
            dst_content = [{"key": obj_id, "type": dst_relation}]
            self.add_sub_item_to_collection_item(
                collection, dst_id, "relations", dst_content
            )

    def check_health(self):
        return True
