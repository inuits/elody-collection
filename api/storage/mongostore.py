import os
import uuid

from pymongo import MongoClient


class MongoStorageManager:
    character_replace_map = {".": "="}

    def __init__(self):
        mongo_host = os.getenv("MONGO_DB_HOST", "mongo")
        mongo_port = int(os.getenv("MONGO_DB_PORT", 27017))
        mongo_db = os.getenv("MONGO_DB_NAME", "dams")
        client = MongoClient(mongo_host, mongo_port)
        self.db = client[mongo_db]

    def get_entities(
            self, skip=0, limit=20, item_type=None, ids=None, skip_relations=0
    ):
        if ids:
            return self.get_items_from_collection_by_ids("entities", ids)
        return self.get_items_from_collection("entities", skip, limit, item_type)

    def get_items_from_collection(self, collection, skip=0, limit=20, item_type=None):
        items = dict()
        if item_type:
            documents = self.db[collection].find(
                self._get_entities_by_type_query(item_type), skip=skip, limit=limit
            )
            count = self.db[collection].count_documents(
                self._get_entities_by_type_query(item_type)
            )
        else:
            documents = self.db[collection].find(skip=skip, limit=limit)
            count = self.db[collection].count_documents({})
        items["count"] = count
        items["results"] = list()
        for document in documents:
            items["results"].append(self._prepare_mongo_document(document, True))
        return items

    def get_items_from_collection_by_ids(self, collection, ids):
        items = dict()
        documents = self.db[collection].find(self._get_multiple_id_query(ids))
        count = self.db[collection].count_documents(self._get_multiple_id_query(ids))
        items["count"] = count
        items["results"] = list()
        for document in documents:
            items["results"].append(self._prepare_mongo_document(document, True))
        return items

    def get_item_from_collection_by_id(self, collection, id):
        document = self.db[collection].find_one(self._get_id_query(id))
        if document:
            document = self._prepare_mongo_document(document, True)
        return document

    def get_collection_item_sub_item(self, collection, id, sub_item):
        ret = []
        document = self.db[collection].find_one(
            self._get_id_query(id), {sub_item: 1, "_id": 0}
        )
        if document and sub_item in document:
            ret = document[sub_item]
        return ret

    def get_collection_item_sub_item_key(self, collection, id, sub_item, key):
        ret = []
        all_sub_items = self.get_collection_item_sub_item(collection, id, sub_item)
        for obj in all_sub_items:
            if obj["key"] == key:
                ret.append(obj)
        return ret

    def get_collection_item_relations(self, collection, id):
        return self.get_collection_item_sub_item(collection, id, "relations")

    def get_collection_item_mediafiles(self, collection, id):
        mediafiles = []
        for mediafile in self.db["mediafiles"].find({collection: id}):
            mediafiles.append(mediafile)
        return mediafiles

    def add_mediafile_to_collection_item(
            self, collection, id, mediafile_id, mediafile_public
    ):
        mediafile = None
        identifiers = self.db[collection].find_one(
            self._get_id_query(id), {"identifiers": 1}
        )
        if identifiers and "identifiers" in identifiers:
            identifiers = identifiers["identifiers"]
            self.db["mediafiles"].update_one(
                self._get_id_query(mediafile_id),
                {"$addToSet": {"entities": {"$each": identifiers}}},
            )
            mediafile = self.db["mediafiles"].find_one(self._get_id_query(mediafile_id))
        return mediafile

    def add_sub_item_to_collection_item(self, collection, id, sub_item, content):
        result = self.db[collection].update_one(
            self._get_id_query(id), {"$addToSet": {sub_item: {"$each": content}}}
        )
        return content if result.modified_count else None

    def add_relations_to_collection_item(self, collection, id, content):
        self.add_sub_item_to_collection_item(collection, id, "relations", content)
        self._add_child_relations(collection, id, content)
        return content

    def save_item_to_collection(self, collection, content):
        content = self._prepare_mongo_document(content, False, str(uuid.uuid4()))
        item_id = self.db[collection].insert_one(content).inserted_id
        return self.get_item_from_collection_by_id(collection, item_id)

    def update_item_from_collection(self, collection, id, content):
        content = self._prepare_mongo_document(content, False)
        self.db[collection].replace_one(self._get_id_query(id), content)
        return self.get_item_from_collection_by_id(collection, id)

    def update_collection_item_sub_item(self, collection, id, sub_item, content):
        patch_data = {sub_item: content}
        self.patch_item_from_collection(collection, id, patch_data)
        return content

    def update_collection_item_relations(self, collection, id, content):
        for item in self.get_collection_item_sub_item(collection, id, "relations"):
            self.delete_collection_item_sub_item_key(
                collection, item["key"], "relations", id
            )
        self.update_collection_item_sub_item(collection, id, "relations", content)
        self._add_child_relations(collection, id, content)
        return content

    def patch_collection_item_relations(self, collection, id, content):
        for item in content:
            self.delete_collection_item_sub_item_key(
                collection, id, "relations", item["key"]
            )
            self.delete_collection_item_sub_item_key(
                collection, item["key"], "relations", id
            )
        relations = self.get_collection_item_sub_item(collection, id, "relations")
        self.update_collection_item_sub_item(
            collection, id, "relations", relations + content
        )
        self._add_child_relations(collection, id, content)
        return content

    def patch_item_from_collection(self, collection, id, content):
        content = self._prepare_mongo_document(content, False)
        self.db[collection].update_one(self._get_id_query(id), {"$set": content})
        return self.get_item_from_collection_by_id(collection, id)

    def delete_item_from_collection(self, collection, id):
        self.db[collection].delete_one(self._get_id_query(id))

    def delete_collection_item_sub_item_key(self, collection, id, sub_item, key):
        patch_data = {sub_item: []}
        all_sub_items = self.get_collection_item_sub_item(collection, id, sub_item)
        for obj in all_sub_items:
            if obj["key"] != key:
                patch_data[sub_item].append(obj)
        self.patch_item_from_collection(collection, id, patch_data)

    def drop_all_collections(self):
        self.db.entities.drop()
        self.db.jobs.drop()
        self.db.mediafiles.drop()
        self.db.tenants.drop()

    def handle_mediafile_status_change(self, old_mediafile, mediafile):
        return

    def reindex_mediafile_parents(self, mediafile):
        return

    def _prepare_mongo_document(self, document, reversed, id=None):
        if id:
            document["_id"] = id
            if "identifiers" not in document:
                document["identifiers"] = [id]
            else:
                document["identifiers"].insert(0, id)
        if "data" in document:
            document["data"] = self._replace_dictionary_keys(document["data"], reversed)
        return document

    def _replace_dictionary_keys(self, data, reversed):
        if type(data) is dict:
            new_dict = dict()
            for key, value in data.items():
                new_value = value
                if type(value) is list:
                    new_value = list()
                    for object in value:
                        new_value.append(
                            self._replace_dictionary_keys(object, reversed)
                        )
                else:
                    new_value = self._replace_dictionary_keys(value, reversed)
                for original_char, replace_char in self.character_replace_map.items():
                    if reversed:
                        new_dict[key.replace(replace_char, original_char)] = new_value
                    else:
                        new_dict[key.replace(original_char, replace_char)] = new_value
            return new_dict
        return data

    def _get_id_query(self, id):
        return {"$or": [{"_id": id}, {"identifiers": id}]}

    def _get_multiple_id_query(self, ids):
        return {"$or": [{"_id": {"$in": ids}}, {"identifiers": {"$in": ids}}]}

    def _get_entities_by_type_query(self, item_type):
        return {"type": item_type}

    def _map_entity_relation(self, relation):
        mapping = {
            "authoredBy": "authored",
            "isIn": "contains",
            "authored": "authoredBy",
            "contains": "isIn",
        }
        return mapping.get(relation)

    def _add_child_relations(self, collection, id, relations):
        for relation in relations:
            dst_relation = self._map_entity_relation(relation["type"])
            dst_id = relation["key"]
            dst_content = [{"key": id, "type": dst_relation}]
            self.add_sub_item_to_collection_item(
                collection, dst_id, "relations", dst_content
            )

    def check_health(self):
        return True
