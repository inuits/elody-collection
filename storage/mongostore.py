from pymongo import MongoClient
import os
from dotenv import load_dotenv
import uuid


class MongoStorageManager:
    character_replace_map = {".": "="}

    def __init__(self):
        self.mongo_host = os.getenv("MONGO_DB_HOST")
        self.mongo_port = int(os.getenv("MONGO_DB_PORT"))
        self.mongo_db = os.getenv("MONGO_DB_NAME")

        self.client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.client[self.mongo_db]

    def save_tenant(self, tenant_json):
        return self.save_item_to_collection("assets", tenant_json)

    def update_tenant(self, tenant_json):
        return self.update_item_from_collection("tenants", tenant_json)

    def delete_tenant(self, id):
        self.delete_item_from_collection("tenants", id)

    def get_tenant_by_id(self, id):
        return self.get_item_from_collection_by_id("tenants", id)

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        count = self.db[collection].count_documents({})
        items["count"] = count
        items["results"] = list()
        for document in self.db[collection].find(skip=skip, limit=limit):
            items["results"].append(self._prepare_mongo_document(document, True))
        return items

    def get_item_from_collection_by_id(self, collection, id):
        document = self.db[collection].find_one(self._get_id_query(id))
        if document:
            document = self._prepare_mongo_document(document, True)
        return document

    def get_collection_item_metadata(self, collection, id):
        metadata = []
        document = self.db[collection].find_one(
            self._get_id_query(id), {"metadata": 1, "_id": 0}
        )
        if document and "metadata" in document:
            metadata = document["metadata"]
        return metadata

    def get_collection_item_metadata_key(self, collection, id, key):
        metadata = []
        all_metadata = self.get_collection_item_metadata(collection, id)
        for metadata_object in all_metadata:
            if metadata_object["key"] == key:
                metadata.append(metadata_object)
        return metadata

    def get_collection_item_mediafiles(self, collection, id):
        mediafiles = []
        for mediafile in self.db["mediafiles"].find({collection: id}):
            mediafiles.append(mediafile)
        return mediafiles

    def add_mediafile_to_entity(self, collection, id, mediafile_id):
        mediafile = None
        identifiers = self.db[collection].find_one(
            self._get_id_query(id), {"identifiers": 1}
        )
        if identifiers and "identifiers" in identifiers:
            identifiers = identifiers["identifiers"]
            self.db["mediafiles"].update_one(
                self._get_id_query(mediafile_id), {"$set": {"entities": identifiers}}
            )
            mediafile = self.db["mediafiles"].find_one(self._get_id_query(mediafile_id))
        return mediafile

    def add_collection_item_metadata(self, collection, id, content):
        self.db[collection].update_one(
            self._get_id_query(id), {"$addToSet": {"metadata": content}}
        )
        return content

    def save_item_to_collection(self, collection, content):
        content = self._prepare_mongo_document(content, False, str(uuid.uuid4()))
        item_id = self.db[collection].insert_one(content).inserted_id
        return self.get_item_from_collection_by_id(collection, item_id)

    def update_item_from_collection(self, collection, id, content):
        content = self._prepare_mongo_document(content, False)
        self.db[collection].replace_one(self._get_id_query(id), content)
        return self.get_item_from_collection_by_id(collection, id)

    def update_collection_item_metadata(self, collection, id, content):
        patch_data = {"metadata": content}
        item = self.patch_item_from_collection(collection, id, patch_data)
        return item["metadata"]

    def patch_item_from_collection(self, collection, id, content):
        content = self._prepare_mongo_document(content, False)
        self.db[collection].update_one(self._get_id_query(id), {"$set": content})
        return self.get_item_from_collection_by_id(collection, id)

    def delete_item_from_collection(self, collection, id):
        self.db[collection].delete_one(self._get_id_query(id))

    def delete_collection_item_metadata_key(self, collection, id, key):
        patch_data = {"metadata": []}
        all_metadata = self.get_collection_item_metadata(collection, id)
        for metadata_object in all_metadata:
            if metadata_object["key"] != key:
                patch_data["metadata"].append(metadata_object)
        self.patch_item_from_collection(collection, id, patch_data)

    def _prepare_mongo_document(self, document, reversed, id=None):
        if id:
            document["_id"] = id
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
