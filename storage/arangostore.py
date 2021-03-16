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

        self.conn = Connection(arangoURL='http://' + self.arango_host + ':8529', username=self.arango_username, password=self.arango_password)
        self.db = self._create_database_if_not_exists(self.arango_db_name)

    def save_tenant(self, tenant_json):
        return self.save_item_to_collection('tenants', tenant_json)

    def update_tenant(self, id, tenant_json):
        return self.update_item_from_collection('tenants', id, tenant_json)

    def delete_tenant(self, id):
        self.delete_item_from_collection('tenants', id)

    def get_tenant_by_id(self, id):
        return self.get_item_from_collection_by_id('tenants', id)

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        count = self.db[collection].count()
        items['count'] = count
        items['results'] = list()
        for document in self.db[collection].fetchAll(skip=skip, limit=limit):
            items['results'].append(document.getStore())
        return items

    def get_item_from_collection_by_id(self, collection, id):
        try:
            key = self._get_key_for_id(collection, id)
            item = self.db[collection][key].getStore()
        except DocumentNotFoundError:
            item = None
        return item

    def get_collection_item_metadata(self, collection, id):
        metadata = []
        queryResults = self._get_field_for_id(collection, id, 'metadata')
        if queryResults:
            metadata = queryResults[0]
        return metadata

    def get_collection_item_metadata_key(self, collection, id, key):
        metadata = []
        aql = '''
FOR c IN @@collection
    FILTER @id IN c.identifiers OR c._key == @id
    FOR metadata IN c.metadata
        FILTER metadata.key == @key
        RETURN metadata
'''
        bind = {'@collection': collection, 'id': id, 'key': key}
        queryResults = self._execute_query(aql, bind)
        for queryResult in queryResults:
            metadata.append(queryResult)
        return metadata

    def get_collection_item_mediafiles(self, collection, id):
        return []

    def add_mediafile_to_entity(self, collection, id, mediafile_id):
        return []
 
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
        queryResult = self._get_field_for_id(collection, id, '_key')
        if queryResult:
            key = queryResult[0]
        return key

    def _get_field_for_id(self, collection, id, field):
        aql = 'FOR c in @@collection FILTER @id IN c.identifiers OR c._key == @id RETURN c.@field'
        bind = {'id': id, '@collection': collection, 'field': field}
        return self._execute_query(aql, bind)

    def _execute_query(self, aql, bindVars):
        return self.db.AQLQuery(aql, rawResults=True, bindVars=bindVars)

    def _create_database_if_not_exists(self, arango_db_name):
        if not self.conn.hasDatabase(arango_db_name):
            return self.conn.createDatabase(arango_db_name)
        else:
            return self.conn[arango_db_name]
