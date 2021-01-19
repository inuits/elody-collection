from pymongo import MongoClient
import os
from dotenv import load_dotenv

class MongoStorageManager:
    def __init__(self):
        load_dotenv('.env')

        self.mongo_host = os.getenv('MONGO_DB_HOST')
        self.mongo_port = int(os.getenv('MONGO_DB_PORT'))
        self.mongo_db = os.getenv('MONGO_DB_NAME')

        self.client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.client[self.mongo_db]
        self.tenants = self.db.tenants
        
    def save_tenant(self, tenant_json):
        tenant_id = self.tenants.insert_one(tenant_json).inserted_id
        return self.get_tenant_by_id(tenant_id)

    def update_tenant(self, tenant_json):
        id = tenant_json["_id"]
        self.tenants.replace_one({"_id": id}, tenant_json)
        return self.get_tenant_by_id(id)

    def delete_tenant(self, id):
        self.tenants.delete_one({"_id": id})

    def get_tenant_by_id(self, id):
        return self.tenants.find_one({"_id": id})
