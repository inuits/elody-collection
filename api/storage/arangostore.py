import os
import sys
import uuid

import app
from .py_arango_connection_extension import PyArangoConnection as Connection
from pyArango.theExceptions import DocumentNotFoundError, CreationError


class ArangoStorageManager:
    def __init__(self):
        self.arango_host = os.getenv("ARANGO_DB_HOST")
        self.arango_username = os.getenv("ARANGO_DB_USERNAME")
        self.arango_password = os.getenv("ARANGO_DB_PASSWORD")
        self.arango_db_name = os.getenv("ARANGO_DB_NAME")
        self.mediafile_edge_name = os.getenv("MEDIAFILE_EDGE", "hasMediafile")
        self.default_graph_name = os.getenv("DEFAULT_GRAPH", "assets")
        self.entity_relations = [
            "authoredBy",
            "authored",
            "isIn",
            "contains",
            "components",
            "parent"
        ]
        self.edges = self.entity_relations + ["hasMediafile"]
        self.conn = Connection(
            arangoURL=self.arango_host,
            username=self.arango_username,
            password=self.arango_password,
        )
        self.db = self._create_database_if_not_exists(self.arango_db_name)

    def get_entities(self, skip, limit, item_type=None, ids=None):
        ids_filter = "FILTER c._key IN @ids" if ids else ""
        type_filter = 'FILTER c.type == "{}"'.format(item_type) if item_type else ""
        aql = """
FOR c IN entities
    {}
    {}
""".format(
            ids_filter, type_filter
        )
        aql2 = """
    LET new_metadata = (
        FOR item,edge IN OUTBOUND c GRAPH 'assets'
            FILTER edge._id NOT LIKE 'hasMediafile%'
            LET relation = {'key': edge._to, 'type': FIRST(SPLIT(edge._id, '/'))}
            RETURN HAS(edge, 'label') ? MERGE(relation, {'label': IS_NULL(edge.label.`@value`) ? edge.label : edge.label.`@value`}) : relation
    )
    LET all_metadata = {'metadata': APPEND(c.metadata, new_metadata)}
    LET primary_items = (
        FOR item, edge IN OUTBOUND c hasMediafile
            FILTER edge.is_primary == true || edge.is_primary_thumbnail == true
            LET primary = edge.is_primary != true ? null : {primary_mediafile_location: item.original_file_location, primary_mediafile: item.filename}
            LET primary_thumb = edge.is_primary_thumbnail != true ? null : {primary_thumbnail_location: item.thumbnail_file_location}
            RETURN primary != null AND primary_thumb != null ? MERGE(primary, primary_thumb) : (primary ? primary : primary_thumb)
    )
    LET merged_primary_items = COUNT(primary_items) > 1 ? MERGE(FIRST(primary_items), LAST(primary_items)) : FIRST(primary_items)
    LIMIT @skip, @limit
    RETURN merged_primary_items == null ? MERGE(c, all_metadata) : MERGE(c, all_metadata, merged_primary_items)
"""
        bind = {"skip": skip, "limit": limit}
        if ids:
            bind["ids"] = ids
        results = self.db.AQLQuery(
            aql + aql2, rawResults=True, bindVars=bind, fullCount=True
        )
        items = dict()
        items["count"] = results.extra["stats"]["fullCount"]
        items["results"] = list(results)
        return items

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        results = self.db[collection].fetchAll(skip=skip, limit=limit, rawResults=True)
        items["count"] = self.db[collection].count()
        items["results"] = list(results)
        return items

    def get_items_from_collection_by_fields(self, collection, fields, skip=0, limit=20):
        items = dict()
        items["results"] = list()
        extra_query = ""
        for field_name, field_value in fields.items():
            extra_query = (
                extra_query
                + """FILTER c.{} == \"{}\"
            """.format(
                    field_name, field_value
                )
            )
        aql = """
FOR c IN @@collection
    {}
    LIMIT @skip, @limit
    RETURN c
""".format(
            extra_query
        )
        bind = {"@collection": collection, "skip": skip, "limit": limit}
        results = self.db.AQLQuery(aql, rawResults=True, bindVars=bind, fullCount=True)
        items["count"] = results.extra["stats"]["fullCount"]
        items["results"] = list(results)
        return items

    def get_item_from_collection_by_id(self, collection, id):
        item = self.get_raw_item_from_collection_by_id(collection, id)
        if item:
            item = item.getStore()
        return item

    def _try_get_item_from_collection_by_key(self, collection, key):
        try:
            item = self.db[collection][key]
        except DocumentNotFoundError:
            item = None
        return item

    def get_raw_item_from_collection_by_id(self, collection, id):
        item = self._try_get_item_from_collection_by_key(collection, id)
        if not item:
            if key := self._get_field_for_id(collection, id, "_key"):
                item = self._try_get_item_from_collection_by_key(collection, key)
        return item

    def get_collection_item_sub_item(self, collection, id, sub_item):
        return self._get_field_for_id(collection, id, sub_item)

    def get_collection_item_sub_item_key(self, collection, id, sub_item, key):
        aql = """
FOR c IN @@collection
    FILTER @id IN c.identifiers OR c._key == @id
    FOR obj IN c.@sub_item
        FILTER obj.key == @key
        RETURN obj
"""
        bind = {"@collection": collection, "id": id, "sub_item": sub_item, "key": key}
        results = self.db.AQLQuery(aql, rawResults=True, bindVars=bind)
        return list(results)

    def get_collection_item_relations(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        relations = []
        for relation in self.entity_relations:
            for edge in entity.getOutEdges(self.db[relation]):
                relation_object = {}
                edge = edge.getStore()
                for key in edge.keys():
                    if key[0] != "_":
                        relation_object[key] = edge[key]
                relation_object["key"] = edge["_to"]
                relation_object["type"] = relation
                if relation_object not in relations:
                    relations.append(relation_object)
                if "value" in relation_object and (
                        relation_object["value"] == "Productie" or
                        relation_object["value"] == "InformatieObject" or
                        relation_object["value"] == "ConceptueelDing"):
                    sub_entity = self.get_raw_item_from_collection_by_id(collection,
                                                                         relation_object["key"].split("entities/")[1])

                    for sub_edge in sub_entity.getOutEdges(self.db[relation]):
                        relation_object = {}
                        sub_edge = sub_edge.getStore()
                        for key in sub_edge.keys():
                            if key[0] != "_":
                                relation_object[key] = sub_edge[key]
                        relation_object["key"] = sub_edge["_to"]
                        relation_object["type"] = relation
                        if relation_object["value"] == "Creatie":

                            sub_entity2 = self.get_raw_item_from_collection_by_id(collection,
                                                                                  relation_object["key"].split(
                                                                                      "entities/")[1])
                            for sub_edge2 in sub_entity2.getOutEdges(self.db[relation]):
                                relation_object = {}
                                sub_edge2 = sub_edge2.getStore()
                                for key in sub_edge2.keys():
                                    if key[0] != "_":
                                        relation_object[key] = sub_edge2[key]
                                relation_object["key"] = sub_edge2["_to"]
                                relation_object["type"] = relation
                                if relation_object not in relations:
                                    relations.append(relation_object)

                        elif relation_object["label"] != "vervaardiger.rol":
                            if relation_object not in relations:
                                relations.append(relation_object)

        return relations

    def get_collection_item_types(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        types = []
        for edge in entity.getOutEdges(self.db["isTypeOf"]):
            relation = {}
            edge = edge.getStore()
            for key in edge.keys():
                if key[0] != "_":
                    relation[key] = edge[key]
            relation["key"] = edge["_to"]
            relation["type"] = "isTypeOf"
            types.append(relation)
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
            relation = {}
            edge = edge.getStore()
            for key in edge.keys():
                if key[0] != "_":
                    relation[key] = edge[key]
            relation["key"] = edge["_to"]
            relation["type"] = "components"
            relations.append(relation)
            # relations = sorted(relations, key=lambda tup: tup["order"])
        return relations

    def get_collection_item_parent(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        relations = []
        for edge in entity.getOutEdges(self.db["parent"]):
            relations.append({"key": edge["_to"], "type": "parent"})
        return relations

    def get_collection_item_mediafiles(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        mediafiles = list()
        for edge in entity.getOutEdges(self.db[self.mediafile_edge_name]):
            mediafile = self.db.fetchDocument(edge["_to"]).getStore()
            if "is_primary" in edge:
                mediafile["is_primary"] = edge["is_primary"]
            if "is_primary_thumbnail" in edge:
                mediafile["is_primary_thumbnail"] = edge["is_primary_thumbnail"]

            mediafiles.append(mediafile)
        return mediafiles

    def set_primary_field_collection_item(
            self, collection, entity_id, mediafile_id, field
    ):
        entity = self.get_raw_item_from_collection_by_id(collection, entity_id)
        for edge in entity.getOutEdges(self.db[self.mediafile_edge_name]):
            new_primary_id = "mediafiles/{}".format(mediafile_id)
            if edge["_to"] != new_primary_id and edge[field]:
                edge[field] = False
                edge.save()
            elif edge["_to"] == new_primary_id and (
                    field not in edge or not edge[field]
            ):
                edge[field] = True
                edge.save()

    def add_mediafile_to_collection_item(self, collection, id, mediafile_id, mediafile_public):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        if not entity:
            return None
        extra_data = {"is_primary": mediafile_public, "is_primary_thumbnail": mediafile_public}
        if mediafile_public:
            for edge in entity.getOutEdges(self.db["hasMediafile"]):
                if "is_primary" in edge and edge["is_primary"] is True:
                    extra_data["is_primary"] = False
                if "is_primary_thumbnail" in edge and edge["is_primary_thumbnail"] is True:
                    extra_data["is_primary_thumbnail"] = False
        self.db.graphs[self.default_graph_name].createEdge(
            self.mediafile_edge_name, entity["_id"], mediafile_id, extra_data
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
        self.db.AQLQuery(aql, rawResults=True, bindVars=bind)
        return content

    def add_relations_to_collection_item(self, collection, id, relations):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        if not entity:
            return None
        for relation in relations:
            extra_data = {}
            for key in relation.keys():
                if key[0] != "_":
                    extra_data[key] = relation[key]
            self.db.graphs[self.default_graph_name].createEdge(
                relation["type"], entity["_id"], relation["key"], extra_data
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
        if "identifiers" not in content:
            content["identifiers"] = [_id]
        else:
            content["identifiers"].insert(0, _id)
        item = self.db[collection].createDocument(content)
        item.save()
        return item.getStore()

    def update_item_from_collection(self, collection, id, content):
        item = self.get_raw_item_from_collection_by_id(collection, id)
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
        item = self.get_raw_item_from_collection_by_id(collection, id)
        item.set(content)
        item.patch()
        return item.getStore()

    def delete_item_from_collection(self, collection, id):
        item = self.get_raw_item_from_collection_by_id(collection, id)
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
        self.db.AQLQuery(aql, rawResults=True, bindVars=bind)

    def drop_all_collections(self):
        self.db["entities"].truncate()
        self.db["jobs"].truncate()
        self.db["mediafiles"].truncate()
        self.db["tenants"].truncate()
        for edge in self.edges:
            self.db[edge].truncate()

    def _map_entity_relation(self, relation):
        mapping = {
            "authoredBy": "authored",
            "isIn": "contains",
            "authored": "authoredBy",
            "contains": "isIn",
            "components": "parent",
            "parent": "components",
            "isTypeOf": "isUsedIn",
            "isUsedIn": "isTypeOf",
            "carriedOutBy": "hasCarriedOut",
            "hasCarriedOut": "carriedOutBy",
        }
        return mapping.get(relation)

    def _get_field_for_id(self, collection, id, field):
        aql = "FOR c in @@collection FILTER c.object_id == @id OR @id IN c.identifiers OR c._key == @id RETURN c.@field"
        bind = {"id": id, "@collection": collection, "field": field}
        result = self.db.AQLQuery(aql, rawResults=True, bindVars=bind)
        if result.__len__():
            if result.__len__() > 1:
                return list(result)
            return result[0]
        return None

    def _create_database_if_not_exists(self, arango_db_name):
        if not self.conn.hasDatabase(arango_db_name):
            self.conn.createDatabase(arango_db_name)
        for collection in [
            "entities",
            "tenants",
            "jobs",
            "mediafiles",
            "key_value_store",
        ]:
            try:
                self.conn.createCollection(collection, arango_db_name)
                self.create_unique_indexes(collection, arango_db_name)
            except CreationError:
                self.create_unique_indexes(collection, arango_db_name)
                continue
        for edge in self.edges:
            try:
                self.conn.createEdge(edge, arango_db_name)
            except CreationError as ex:
                continue
        try:
            self.conn.createGraph(
                self.default_graph_name,
                arango_db_name,
                {
                    "edgeDefinitions": [],
                    "orphanCollections": [],
                },
            )
        except CreationError as ex:
            pass
        for edge_name in self.edges:
            to = ["entities"]
            if edge_name == "hasMediafile":
                to = ["mediafiles"]
            edge_definition = {"collection": edge_name, "from": ["entities"], "to": to}
            try:
                self.conn.addEdgeDefinitionToGraph(
                    arango_db_name, self.default_graph_name, edge_definition
                )
            except CreationError as ex:
                continue
        return self.conn[arango_db_name]

    def create_unique_indexes(self, collection, arango_db_name):
        if collection == "entities":
            try:
                self.conn[arango_db_name]['entities'].ensureIndex(fields=["object_id"],
                                                                  index_type="hash", unique=True, sparse=True)
                self.conn[arango_db_name]['entities'].ensureIndex(fields=["data.dcterms:isVersionOf"],
                                                                  index_type="hash", unique=True, sparse=True)
            except Exception as ex:
                app.logger.error("Could not create unique index: " + str(ex))
