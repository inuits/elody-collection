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

    def update_tenant(self, id, tenant_json):
        return self.update_item_from_collection('tenants', id, tenant_json)

    def delete_tenant(self, id):
        self.delete_item_from_collection('tenants', id)

    def get_tenant_by_id(self, id):
        return self.get_item_from_collection_by_id('tenants', id)

    def get_item_from_collection_by_id(self, collection, id):
        try:
            key = self._get_key_for_id(collection, id)
            item = self.db[collection][key].getStore()
        except DocumentNotFoundError:
            item = None
        return item

    def get_collection_item_metadata(self, collection, id):
        metadata = []
        aql = 'FOR c in @@collection FILTER @id IN c.identifiers OR c._key == @id RETURN c.metadata'
        bind = {'id': id, '@collection': collection}
        queryResults = self.db.AQLQuery(aql, rawResults=True, bindVars=bind)
        for queryResult in queryResults:
            metadata.append(queryResult)
        return metadata

    def save_item_to_collection(self, collection, content):
        content['_key'] = str(uuid.uuid4())
        item = self.db[collection].createDocument(content)
        item.save()
        return item.getStore()

    def update_item_from_collection(self, collection, id, content):
        key = self._get_key_for_id(collection, id)
        item = self.db[collection][key]
        item.set(content)
        item.save()
        return item.getStore()

    def patch_item_from_collection(self, collection, id, content):
        key = self._get_key_for_id(collection, id)
        item = self.db[collection][key]
        item.set(content)
        item.patch()
        return item.getStore()

    def delete_item_from_collection(self, collection, id):
        key = self._get_key_for_id(collection, id)
        self.db[collection][key].delete()

    def _get_key_for_id(self, collection, id):
        key = None
        aql = 'FOR c in @@collection FILTER @id IN c.identifiers OR c._key == @id RETURN c._key'
        bind = {'id': id, '@collection': collection}
        queryResult = self.db.AQLQuery(aql, rawResults=True, bindVars=bind)
        if queryResult:
            key = queryResult[0]
        return key
