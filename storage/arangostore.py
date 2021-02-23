from pyArango.connection import *
from pyArango.theExceptions import *
import os
from dotenv import load_dotenv
import uuid

class ArangoStorageManager:
    def __init__(self):
        load_dotenv('.env')

        self.arango_host = os.getenv('ARANGO_DB_HOST')
        self.arango_username = os.getenv('ARANGO_DB_USERNAME')
        self.arango_password = os.getenv('ARANGO_DB_PASSWORD')
        self.arango_db_name = os.getenv('ARANGO_DB_NAME')

        self.conn = Connection(username=self.arango_username, password=self.arango_password)
        self.db = self.conn[self.arango_db_name]

    def save_tenant(self, tenant_json):
        return self.save_item_to_collection('tenants', tenant_json)

    def update_tenant(self, tenant_json):
        return self.update_item_from_collection('tenants', tenant_json)

    def delete_tenant(self, id):
        self.delete_item_from_collection('tenants', id)

    def get_tenant_by_id(self, id):
        return self.get_item_from_collection_by_id('tenants', id)

    def get_item_from_collection_by_id(self, collection, id):
        try:
            item = self.db[collection][id].getStore()
        except DocumentNotFoundError:
            item = None
        return item

    def save_item_to_collection(self, collection, content):
        content['_key'] = str(uuid.uuid4())
        item = self.db[collection].createDocument(content)
        item.save()
        return item.getStore()

    def update_item_from_collection(self, collection, content):
        item = self.db[collection][content['_key']]
        item.set(content)
        item.save()
        return item.getStore()

    def patch_item_from_collection(self, collection, content):
        item = self.db[collection][content['_key']]
        item.patch(content)
        item.save()
        return item.getStore()

    def delete_item_from_collection(self, collection, id):
        self.db[collection][id].delete()
