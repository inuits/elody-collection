from pymongo import MongoClient
import os
from dotenv import load_dotenv
from bson.objectid import ObjectId

class MongoStorageManager:
    def __init__(self):
        load_dotenv('.env')

        self.mongo_host = os.getenv('MONGO_DB_HOST')
        self.mongo_port = int(os.getenv('MONGO_DB_PORT'))
        self.mongo_db = os.getenv('MONGO_DB_NAME')

        self.client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.client[self.mongo_db]
        
    def save_tenant(self, tenant_json):
        return self.save_item_to_collection('assets', tenant_json)

    def update_tenant(self, tenant_json):
        return self.update_item_from_collection('tenants', tenant_json)

    def delete_tenant(self, id):
        self.delete_item_from_collection('tenants', id)

    def get_tenant_by_id(self, id):
        return self.get_item_from_collection_by_id('tenants', id)

    def get_item_from_collection_by_id(self, collection, id):
        return self._object_id_to_string(self.db[collection].find_one({"_id": id}))

    def save_item_to_collection(self, collection, content):
        item_id = self.db[collection].insert_one(content).inserted_id
        return self.get_item_from_collection_by_id(collection, item_id)

    def update_item_from_collection(self, collection, content):
        id = content['_id']
        self.db[collection].replace_one({"_id": id}, content)
        return self.get_item_from_collection_by_id(collection, id)

    def patch_item_from_collection(self, collection, content):
        id = content['_id']
        self.db[collection].update_one({"_id": id}, {"$set": content})
        return self.get_item_from_collection_by_id(collection, id)

    def delete_item_from_collection(self, collection, id):
        self.db[collection].delete_one({"_id": id})

    def _object_id_to_string(self, document):
        document['_id'] = str(document['_id'])
        return document
