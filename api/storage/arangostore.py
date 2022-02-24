import json
import sys

from cloudevents.http import CloudEvent, to_json

import app
import os
import uuid

from pyArango.theExceptions import DocumentNotFoundError, CreationError
from .py_arango_connection_extension import PyArangoConnection as Connection


class ArangoStorageManager:
    def __init__(self):
        self.arango_host = os.getenv("ARANGO_DB_HOST")
        self.arango_username = os.getenv("ARANGO_DB_USERNAME")
        self.arango_password = os.getenv("ARANGO_DB_PASSWORD")
        self.arango_db_name = os.getenv("ARANGO_DB_NAME")
        self.default_graph_name = os.getenv("DEFAULT_GRAPH", "assets")
        self.collections = [
            "box_visits",
            "entities",
            "jobs",
            "key_value_store",
            "mediafiles",
            "tenants",
        ]
        self.entity_relations = [
            "authoredBy",
            "authored",
            "isIn",
            "contains",
            "components",
            "frames",
            "parent",
            "stories",
            "box_stories",
            "box",
            "visited",
            "inBasket",
        ]
        self.edges = self.entity_relations + ["hasMediafile"]
        self.conn = Connection(
            arangoURL=self.arango_host,
            username=self.arango_username,
            password=self.arango_password,
        )
        self.db = self._create_database_if_not_exists()

    def get_box_visits(self, skip, limit, item_type=None, ids=None):
        ids_filter = "FILTER c._key IN @ids" if ids else ""
        type_filter = f'FILTER c.type == "{item_type}"' if item_type else ""
        aql = """
    FOR c IN box_visits
        {}
        {}
    """.format(
            ids_filter, type_filter
        )
        aql1 = """
            LET new_metadata = (
                FOR item,edge IN OUTBOUND c GRAPH 'assets'
                    FILTER edge._id NOT LIKE 'hasMediafile%'
                    LET relation = {'key': edge._to, 'type': FIRST(SPLIT(edge._id, '/'))}
                    RETURN HAS(edge, 'label') ? MERGE(relation, {'label': IS_NULL(edge.label.`@value`) ? edge.label : edge.label.`@value`}) : relation
            )
            LET all_metadata = {'metadata': APPEND(c.metadata, new_metadata)}
            LIMIT @skip, @limit
            RETURN MERGE(c, all_metadata)
            """
        bind = {"skip": skip, "limit": limit}
        if ids:
            bind["ids"] = ids
        results = self.db.AQLQuery(
            aql + aql1, rawResults=True, bindVars=bind, fullCount=True
        )
        items = dict()
        items["count"] = results.extra["stats"]["fullCount"]
        results = list(results)
        results_sorted = (
            [
                result_item
                for i in ids
                for result_item in results
                if result_item["_key"] == i
            ]
            if ids
            else results
        )
        items["results"] = results_sorted

        return items

    def get_entities(self, skip, limit, item_type=None, ids=None, skip_relations=0):
        ids_filter = "FILTER c._key IN @ids" if ids else ""
        type_filter = f'FILTER c.type == "{item_type}"' if item_type else ""
        aql = """
FOR c IN entities
    {}
    {}
""".format(
            ids_filter, type_filter
        )
        if skip_relations == 1:
            aql2 = """       
            LET all_metadata = {'metadata': c.metadata}
                """
        else:
            aql2 = """
            LET new_metadata = (
                FOR item,edge IN OUTBOUND c GRAPH 'assets'
                    FILTER edge._id NOT LIKE 'hasMediafile%'
                    LET relation = {'key': edge._to, 'type': FIRST(SPLIT(edge._id, '/'))}
                    RETURN HAS(edge, 'label') ? MERGE(relation, {'label': IS_NULL(edge.label.`@value`) ? edge.label : edge.label.`@value`}) : relation
            )
            LET all_metadata = {'metadata': APPEND(c.metadata, new_metadata)}
            """
        aql3 = """    
        LET primary_items = (
            FOR item, edge IN OUTBOUND c hasMediafile
                FILTER edge.is_primary == true || edge.is_primary_thumbnail == true
                LET primary = edge.is_primary != true ? null : {primary_mediafile_location: item.original_file_location, primary_mediafile: item.filename, primary_transcode_location: item.transcode_file_location, primary_width: item.img_width, primary_height: item.img_height}
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
            aql + aql2 + aql3, rawResults=True, bindVars=bind, fullCount=True
        )
        items = dict()
        items["count"] = results.extra["stats"]["fullCount"]
        results = list(results)
        results_sorted = (
            [
                result_item
                for i in ids
                for result_item in results
                if result_item["_key"] == i
            ]
            if ids
            else results
        )
        items["results"] = results_sorted
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

    def get_collection_item_relations(
            self, collection, id, include_sub_relations=False
    ):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        relations = []
        if entity["type"] == "asset":
            entity_relations = ["isIn", "components", "parent"]
        elif entity["type"] in ["thesaurus", "museum"]:
            entity_relations = []
        elif entity["type"] == "box_visit":
            entity_relations = ["stories", "visited", "inBasket"]
        elif entity["type"] == "box":
            entity_relations = ["box_stories"]
        elif entity["type"] == "story":
            entity_relations = ["frames", "box"]
        elif entity["type"] == "frame":
            entity_relations = ["stories", "components"]
        else:
            entity_relations = ["components"]
        for relation in entity_relations:
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
                if include_sub_relations and (
                        "value" in relation_object
                        and (
                                relation_object["value"]
                                in [
                                    "Productie",
                                    "InformatieObject",
                                    "ConceptueelDing",
                                    "InformatieObject",
                                    "Classificatie",
                                ]
                        )
                        or (
                                "label" in relation_object
                                and (relation_object["label"] in ["MaterieelDing.bestaatUit"])
                        )
                ):
                    sub_entity = self.get_raw_item_from_collection_by_id(
                        collection, relation_object["key"].split("entities/")[1]
                    )

                    for sub_edge in sub_entity.getOutEdges(self.db[relation]):
                        relation_object = {}
                        sub_edge = sub_edge.getStore()
                        for key in sub_edge.keys():
                            if key[0] != "_":
                                relation_object[key] = sub_edge[key]
                        relation_object["key"] = sub_edge["_to"]
                        relation_object["type"] = relation
                        if relation_object["value"] == "Creatie":

                            sub_entity2 = self.get_raw_item_from_collection_by_id(
                                collection, relation_object["key"].split("entities/")[1]
                            )
                            for sub_edge2 in sub_entity2.getOutEdges(self.db[relation]):
                                relation_object = {}
                                sub_edge2 = sub_edge2.getStore()
                                for key in sub_edge2.keys():
                                    if key[0] != "_":
                                        relation_object[key] = sub_edge2[key]
                                relation_object["key"] = sub_edge2["_to"]
                                relation_object["type"] = relation
                                if (
                                        relation_object not in relations
                                        and relation_object["label"] != "vervaardiger.rol"
                                ):
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
        for edge in entity.getOutEdges(self.db["hasMediafile"]):
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
        for edge in entity.getOutEdges(self.db["hasMediafile"]):
            new_primary_id = f"mediafiles/{mediafile_id}"
            if edge["_to"] != new_primary_id and edge[field]:
                edge[field] = False
                edge.save()
            elif edge["_to"] == new_primary_id and (
                    field not in edge or not edge[field]
            ):
                edge[field] = True
                edge.save()

    def add_mediafile_to_collection_item(
            self, collection, id, mediafile_id, mediafile_public
    ):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        if not entity:
            return None
        extra_data = {
            "is_primary": mediafile_public,
            "is_primary_thumbnail": mediafile_public,
        }
        if mediafile_public:
            for edge in entity.getOutEdges(self.db["hasMediafile"]):
                if "is_primary" in edge and edge["is_primary"] is True:
                    extra_data["is_primary"] = False
                if (
                        "is_primary_thumbnail" in edge
                        and edge["is_primary_thumbnail"] is True
                ):
                    extra_data["is_primary_thumbnail"] = False
        self.db.graphs[self.default_graph_name].createEdge(
            "hasMediafile", entity["_id"], mediafile_id, extra_data
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

    def add_relations_to_collection_item(self, collection, id, relations, parent=True):
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
            optional_label = self._map_entity_relation_parent_label(relation["label"]) if "label" in relation else None
            if optional_label is not None:
                extra_data = {
                    "label": optional_label,
                    "value": entity["data"]["MensgemaaktObject.titel"]["@value"],
                }
            else:
                extra_data = {}
            if parent:
                self.db.graphs[self.default_graph_name].createEdge(
                    self._map_entity_relation(relation["type"]),
                    relation["key"],
                    entity["_id"],
                    extra_data,
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
        raw_item = self.get_raw_item_from_collection_by_id(collection, id)
        raw_item.set(content)
        raw_item.save()
        item = raw_item.getStore()
        self._update_parent_relation_values(raw_item, item)
        return item

    def update_collection_item_sub_item(self, collection, id, sub_item, content):
        patch_data = {sub_item: content}
        item = self.patch_item_from_collection(collection, id, patch_data)
        return item[sub_item]

    def update_collection_item_relations(self, collection, id, content, parent=True):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        for relation in self.entity_relations:
            for edge in entity.getEdges(self.db[relation]):
                edge.delete()
        return self.add_relations_to_collection_item(collection, id, content, parent)

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        for item in content:
            for relation in self.entity_relations:
                for edge in entity.getEdges(self.db[relation]):
                    if edge["_from"] == item["key"] or edge["_to"] == item["key"]:
                        edge.delete()
        return self.add_relations_to_collection_item(collection, id, content, parent)

    def patch_item_from_collection(self, collection, id, content):
        raw_item = self.get_raw_item_from_collection_by_id(collection, id)
        raw_item.set(content)
        raw_item.patch()
        item = raw_item.getStore()
        self._update_parent_relation_values(raw_item, item)
        return item

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

    def get_custom_query(self, aql, variables):
        return self.db.AQLQuery(aql, rawResults=True, bindVars=variables)

    def drop_all_collections(self):
        for collection in self.collections:
            self.db[collection].truncate()
        for edge in self.edges:
            self.db[edge].truncate()

    def _get_mediafile_publication_status(self, mediafile):
        if "metadata" not in mediafile:
            return ""
        for metadata in mediafile["metadata"]:
            if metadata["key"] == "publication_status":
                return metadata["value"]
        return ""

    def _get_primary_items(self, raw_entity):
        result = {"primary_mediafile": "", "primary_thumbnail": ""}
        for edge in raw_entity.getOutEdges(self.db["hasMediafile"]):
            if "is_primary" in edge and edge["is_primary"]:
                result["primary_mediafile"] = edge["_to"]
            if "is_primary_thumbnail" in edge and edge["is_primary_thumbnail"]:
                result["primary_thumbnail"] = edge["_to"]
        return result

    def _set_new_primary(self, raw_entity, mediafile=False, thumbnail=False):
        for edge in raw_entity.getOutEdges(self.db["hasMediafile"]):
            potential_mediafile = self.db.fetchDocument(edge["_to"]).getStore()
            if self._get_mediafile_publication_status(potential_mediafile) == "publiek":
                if mediafile:
                    edge["is_primary"] = True
                if thumbnail:
                    edge["is_primary_thumbnail"] = True
                edge.save()
                return

    def handle_mediafile_status_change(self, old_mediafile, mediafile):
        old_publication_status = self._get_mediafile_publication_status(old_mediafile)
        new_publication_status = self._get_mediafile_publication_status(mediafile)
        if old_publication_status == new_publication_status:
            return
        for edge in self.db.fetchDocument(mediafile["_id"]).getInEdges(
                self.db["hasMediafile"]
        ):
            raw_entity = self.db.fetchDocument(edge["_from"])
            primary_items = self._get_primary_items(raw_entity)
            if new_publication_status == "publiek":
                if not primary_items["primary_mediafile"]:
                    edge["is_primary"] = True
                    edge.save()
                if not primary_items["primary_thumbnail"]:
                    edge["is_primary_thumbnail"] = True
                    edge.save()
            else:
                change_primary_mediafile = (
                        primary_items["primary_mediafile"] == mediafile["_id"]
                )
                change_primary_thumbnail = (
                        primary_items["primary_thumbnail"] == mediafile["_id"]
                )
                if change_primary_mediafile or change_primary_thumbnail:
                    edge["is_primary"] = False
                    edge["is_primary_thumbnail"] = False
                    edge.save()
                    self._set_new_primary(
                        raw_entity, change_primary_mediafile, change_primary_thumbnail
                    )

    def _update_parent_relation_values(self, raw_entity, entity):
        if "metadata" not in entity:
            return
        new_value = None
        for title_key in ["title", "fullname", "fullName", "description"]:
            if new_value is None:
                for metadata in entity["metadata"]:
                    if "key" in metadata and metadata["key"] == title_key:
                        new_value = metadata["value"]
            else:
                break
        if new_value is not None:
            for edge in raw_entity.getEdges(self.db["components"]):
                if edge["key"] == entity["_id"]:
                    patch = {"value": new_value}
                    edge.set(patch)
                    edge.patch()
                    attributes = {"type": "dams.edge_changed", "source": "dams"}
                    data = {
                        "location": "/entities?ids={}&skip_relations=1".format(entity["_key"]),
                    }
                    event = CloudEvent(attributes, data)
                    message = json.loads(to_json(event))
                    app.rabbit.send(message, routing_key="dams.edge_changed")


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
            "frames": "stories",
            "box": "box_stories",
            "box_stories": "box"
        }
        return mapping.get(relation)

    def _map_entity_relation_parent_label(self, relation):
        mapping = {"GecureerdeCollectie.bestaatUit": "Collectie.naam"}
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

    def _create_unique_indexes(self, arango_db_name):
        try:
            self.conn[arango_db_name]["entities"].ensureIndex(
                fields=["object_id"], index_type="hash", unique=True, sparse=True
            )
            """ Currently disabled because of LDES conflicts
            self.conn[arango_db_name]["entities"].ensureIndex(
                fields=["data.dcterms:isVersionOf"],
                index_type="hash",
                unique=True,
                sparse=True,
            )
            """
            self.conn[arango_db_name]["box_visits"].ensureIndex(
                fields=["code"], index_type="hash", unique=True, sparse=True
            )
        except Exception as ex:
            app.logger.error(f"Could not create unique index: {ex}")

    def _create_database_if_not_exists(self):
        if not self.conn.hasDatabase(self.arango_db_name):
            self.conn.createDatabase(self.arango_db_name)
        for collection in self.collections:
            try:
                self.conn.createCollection(collection, self.arango_db_name)
            except CreationError:
                continue
        self._create_unique_indexes(self.arango_db_name)
        for edge in self.edges:
            try:
                self.conn.createEdge(edge, self.arango_db_name)
            except CreationError:
                continue
        try:
            self.conn.createGraph(
                self.default_graph_name,
                self.arango_db_name,
                {
                    "edgeDefinitions": [],
                    "orphanCollections": [],
                },
            )
        except CreationError:
            pass
        for edge_name in self.edges:
            to = ["entities"]
            fr = ["entities"]
            if edge_name == "hasMediafile":
                to = ["mediafiles"]
            elif edge_name == "stories":
                fr = ["box_visits"]
            try:
                self.conn.addEdgeDefinitionToGraph(
                    self.default_graph_name,
                    self.arango_db_name,
                    {"collection": edge_name, "from": fr, "to": to},
                )
            except CreationError:
                continue
        return self.conn[self.arango_db_name]
