import os
import sys
import uuid

from .py_arango_connection_extension import PyArangoConnection as Connection
from pyArango.theExceptions import *
from werkzeug.exceptions import BadRequest


class ArangoStorageManager:
    def __init__(self):
        self.arango_host = os.getenv("ARANGO_DB_HOST")
        self.arango_username = os.getenv("ARANGO_DB_USERNAME")
        self.arango_password = os.getenv("ARANGO_DB_PASSWORD")
        self.arango_db_name = os.getenv("ARANGO_DB_NAME")
        self.entity_collection_name = os.getenv("ENTITY_COLLECTION", "entities")
        self.mediafile_collection_name = os.getenv("MEDIAFILE_COLLECTION", "mediafiles")
        self.mediafile_edge_name = os.getenv("MEDIAFILE_EDGE", "hasMediafile")
        self.default_graph_name = os.getenv("DEFAULT_GRAPH", "assets")
        self.entity_relations = ["authoredBy", "authored", "isIn", "contains", "components", "parent", "isTypeOf", "isUsedIn" ]
        self.edges = self.entity_relations + ["hasMediafile"]

        self.conn = Connection(
            arangoURL="http://" + self.arango_host + ":8529",
            username=self.arango_username,
            password=self.arango_password,
        )
        self.db = self._create_database_if_not_exists(self.arango_db_name)

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        count = self.db[collection].count()
        items["count"] = count
        items["results"] = list()
        for document in self.db[collection].fetchAll(skip=skip, limit=limit):
            items["results"].append(document.getStore())
        return items

    def get_items_from_collection_by_ids(self, collection, ids):
        items = dict()
        items["results"] = list()
        aql = """
FOR c IN @@collection
    FILTER c._key IN @ids
    RETURN c
"""
        bind = {"@collection": collection, "ids": ids}
        queryResults = self._execute_query(aql, bind)
        items["count"] = len(queryResults)
        for queryResult in queryResults:
            items["results"].append(queryResult)
        return items

    def get_item_from_collection_by_id(self, collection, id):
        item = self.get_raw_item_from_collection_by_id(collection, id)
        if item:
            item = item.getStore()
        return item

    def get_raw_item_from_collection_by_id(self, collection, id):
        try:
            key = self._get_key_for_id(collection, id)
            item = self.db[collection][key]
        except DocumentNotFoundError:
            item = None
        return item

    def get_collection_item_sub_item(self, collection, id, sub_item):
        ret = []
        queryResults = self._get_field_for_id(collection, id, sub_item)
        if queryResults:
            ret = queryResults[0]
        return ret

    def get_collection_item_sub_item_key(self, collection, id, sub_item, key):
        ret = []
        aql = """
FOR c IN @@collection
    FILTER @id IN c.identifiers OR c._key == @id
    FOR obj IN c.@sub_item
        FILTER obj.key == @key
        RETURN obj
"""
        bind = {"@collection": collection, "id": id, "sub_item": sub_item, "key": key}
        queryResults = self._execute_query(aql, bind)
        for queryResult in queryResults:
            ret.append(queryResult)
        return ret

    def get_collection_item_relations(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        relations = []
        for relation in self.entity_relations:
            for edge in entity.getOutEdges(self.db[relation]):
                relations.append({"key": edge["_to"], "type": relation})
        return relations

    def get_collection_item_types(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        types = []
        for edge in entity.getOutEdges(self.db["isTypeOf"]):
            types.append({"key": edge["_to"], "type": "isTypeOf"})
        return types

    def get_collection_item_usage(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        usage = []
        for edge in entity.getOutEdges(self.db["isUsedIn"]):
            usage.append({"key": edge["_to"], "type": "isUsedIn"})
        return usage

    def get_collection_item_components(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        relations = []
        for edge in entity.getOutEdges(self.db["components"]):
            relations.append({"key": edge["_to"], "type": "components", "order": edge["order"]})
            relations = sorted(relations, key=lambda tup: tup["order"])
        return relations

    def get_collection_item_parent(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        relations = []
        for edge in entity.getOutEdges(self.db["parent"]):
            relations.append({"key": edge["_to"], "type": "parent"})
        return relations

    def get_collection_item_mediafiles(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        mediafiles = []
        for edge in entity.getOutEdges(self.db[self.mediafile_edge_name]):
            mediafiles.append(self.db.fetchDocument(edge["_to"]).getStore())
        return mediafiles

    def add_mediafile_to_collection_item(self, collection, id, mediafile_id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        if "primary_mediafile_id" not in entity:
            dict_entity = entity.getStore()
            dict_entity["primary_mediafile_id"] = mediafile_id
            if "primary_thumbnail_file_location" not in entity:
                mediafile =  self.db.fetchDocument(mediafile_id)
                if mediafile is not None:
                    dict_entity["primary_thumbnail_file_location"] = mediafile["thumbnail_file_location"]
            self.update_item_from_collection("entities", id, dict_entity)
        if not entity:
            return None
        self.db.graphs[self.default_graph_name].createEdge(
            self.mediafile_edge_name, entity["_id"], mediafile_id, {}
        )

        return self.db.fetchDocument(mediafile_id).getStore()

    def add_sub_item_to_collection_item(self, collection, id, sub_item, content):
        aql = """
FOR c IN @@collection
    FILTER @id IN c.identifiers OR c._key == @id
    LET newSubItem = APPEND(c.@sub_item, @content, true)
    UPDATE c WITH {@sub_item: newSubItem} IN @@collection
"""
        bind = {
            "@collection": collection,
            "id": id,
            "sub_item": sub_item,
            "content": content,
        }
        self._execute_query(aql, bind)
        return content

    def add_relations_to_collection_item(self, collection, id, relations):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        if not entity:
            return None
        for relation in relations:
            self.db.graphs[self.default_graph_name].createEdge(
                relation["type"], entity["_id"], relation["key"], {"order": relation["order"]} if "order" in relation.keys() and relation["type"] == "components" else {}
            )
            self.db.graphs[self.default_graph_name].createEdge(
                self._map_entity_relation(relation["type"]),
                relation["key"],
                entity["_id"],
                {},
            )
        return relations

    def save_item_to_collection(self, collection, content):
        _id = str(uuid.uuid4())
        content["_key"] = _id
        if "identifiers" in content:
            content["identifiers"].insert(0, _id)
        item = self.db[collection].createDocument(content)
        item.save()
        return item.getStore()

    def update_item_from_collection(self, collection, id, content):
        key = self._get_key_for_id(collection, id)
        item = self.db[collection][key]
        item.set(content)
        item.save()
        return item.getStore()

    def update_collection_item_sub_item(self, collection, id, sub_item, content):
        patch_data = {sub_item: content}
        item = self.patch_item_from_collection(collection, id, patch_data)
        return item[sub_item]

    def update_collection_item_relations(self, collection, id, content):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        for relation in self.entity_relations:
            for edge in entity.getEdges(self.db[relation]):
                edge.delete()
        return self.add_relations_to_collection_item(collection, id, content)

    def patch_collection_item_relations(self, collection, id, content):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        for item in content:
            for relation in self.entity_relations:
                for edge in entity.getEdges(self.db[relation]):
                    if edge["_from"] == item["key"] or edge["_to"] == item["key"]:
                        edge.delete()
        return self.add_relations_to_collection_item(collection, id, content)

    def patch_item_from_collection(self, collection, id, content):
        key = self._get_key_for_id(collection, id)
        item = self.db[collection][key]
        item.set(content)
        item.patch()
        return item.getStore()

    def delete_item_from_collection(self, collection, id):
        key = self._get_key_for_id(collection, id)
        item = self.db[collection][key]
        for edge_name in self.edges:
            for edge in item.getEdges(self.db[edge_name]):
                edge.delete()
        item.delete()

    def delete_collection_item_sub_item_key(self, collection, id, sub_item, key):
        aql = """
FOR c IN @@collection
    FILTER @id IN c.identifiers OR c._key == @id
    LET filteredSubItems = (
        FOR obj IN c.@sub_item
            FILTER obj.key != @key
            RETURN obj
    )
    UPDATE c WITH {@sub_item: filteredSubItems} IN @@collection
"""
        bind = {"@collection": collection, "id": id, "sub_item": sub_item, "key": key}
        queryResults = self._execute_query(aql, bind)

    def drop_all_collections(self):
        return

    def _map_entity_relation(self, relation):
        mapping = {
            "authoredBy": "authored",
            "isIn": "contains",
            "authored": "authoredBy",
            "contains": "isIn",
            "components": "parent",
            "parent": "components",
            "isTypeOf": "isUsedIn",
            "isUsedIn": "isTypeOf"
        }
        return mapping.get(relation)

    def _get_key_for_id(self, collection, id):
        key = None
        queryResult = self._get_field_for_id(collection, id, "_key")
        if queryResult:
            key = queryResult[0]
        return key

    def _get_field_for_id(self, collection, id, field):
        aql = "FOR c in @@collection FILTER @id IN c.identifiers OR c._key == @id RETURN c.@field"
        bind = {"id": id, "@collection": collection, "field": field}
        return self._execute_query(aql, bind)

    def _execute_query(self, aql, bindVars):
        return self.db.AQLQuery(aql, rawResults=True, bindVars=bindVars)

    def _create_database_if_not_exists(self, arango_db_name):
        if not self.conn.hasDatabase(arango_db_name):
            self.conn.createDatabase(arango_db_name)
            for collection in ["entities", "tenants", "jobs", "mediafiles"]:
                self.conn.createCollection(collection, arango_db_name)
            for edge in ["authoredBy", "contains", "isIn", "authored", "components", "parent"]:
                self.conn.createEdge(edge, arango_db_name)
            self.conn.createGraph(self.default_graph_name, arango_db_name,
                                  {"edgeDefinitions": [
                                      {"collection": "authored", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "authoredBy", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "components", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "parent", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "contains", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "isIn", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "isTypeOf", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "isUsedIn", "from": ["entities"], "to": ["entities"]},
                                      {"collection": "hasMediafile", "from": ["entities"], "to": ["mediafiles"]},

                                  ],
                                   "orphanCollections": []}
                                  )
            return arango_db_name
        else:
            return self.conn[arango_db_name]
