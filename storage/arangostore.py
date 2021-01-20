from pyArango.connection import *
from pyArango.theExceptions import *
import os
from dotenv import load_dotenv

class ArangoStorageManager:
    def __init__(self):
        load_dotenv('.env')

        self.arango_host = os.getenv('ARANGO_DB_HOST')
        self.arango_username = os.getenv('ARANGO_DB_USERNAME')
        self.arango_password = os.getenv('ARANGO_DB_PASSWORD')
        self.arango_db_name = os.getenv('ARANGO_DB_NAME')

        self.conn = Connection(username=self.arango_username, password=self.arango_password)
        self.db = self.conn[self.arango_db_name]
        self.tenants = self.db['tenants']

    def save_tenant(self, tenant_json):
        tenant = self.tenants.createDocument(tenant_json)
        tenant.save()
        return tenant.getStore()

    def update_tenant(self, tenant_json):
        tenant = self.tenants[tenant_json['_key']]
        tenant.set(tenant_json)
        tenant.patch()
        return tenant.getStore()

    def delete_tenant(self, id):
        self.tenants[id].delete()        

    def get_tenant_by_id(self, id):
        return self.get_item_from_collection_by_id('tenants', id)

    def get_item_from_collection_by_id(self, collection, id):
        try:
            item = self.db[collection][id].getStore()
        except DocumentNotFoundError:
            item = None
        return item
