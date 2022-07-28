import app
import json
import os
import random
import string
import uuid

from cloudevents.http import CloudEvent, to_json
from exceptions import NonUniqueException
from pyArango.theExceptions import CreationError, DocumentNotFoundError, UpdateError
from storage.py_arango_connection_extension import PyArangoConnection as Connection
from time import sleep


class ArangoStorageManager:
    def __init__(self):
        self.arango_host = os.getenv("ARANGO_DB_HOST")
        self.arango_username = os.getenv("ARANGO_DB_USERNAME")
        self.arango_password = os.getenv("ARANGO_DB_PASSWORD")
        self.arango_db_name = os.getenv("ARANGO_DB_NAME")
        self.default_graph_name = os.getenv("DEFAULT_GRAPH", "assets")
        self.event_delay = float(os.getenv("EVENT_DELAY", 0.02))
        self.event_batch_limit = int(os.getenv("EVENT_BATCH_LIMIT", 50))
        self.collections = [
            "box_visits",
            "entities",
            "jobs",
            "key_value_store",
            "mediafiles",
            "tenants",
        ]
        self.entity_relations = [
            "box",
            "box_stories",
            "components",
            "contains",
            "frames",
            "isIn",
            "parent",
            "stories",
            "story_box",
            "story_box_visits",
            "visited",
        ]
        self.edges = [*self.entity_relations, "hasMediafile"]
        self.conn = Connection(
            arangoURL=self.arango_host,
            username=self.arango_username,
            password=self.arango_password,
        )
        self.db = self._create_database_if_not_exists()

    def get_box_visits(self, skip, limit, item_type=None, ids=None):
        aql = f"""
            FOR c IN box_visits
            {"FILTER c._key IN @ids" if ids else ""}
            {f'FILTER c.type == "{item_type}"' if item_type else ""}
        """
        aql2 = """
            LET new_metadata = (
                FOR item,edge IN OUTBOUND c GRAPH 'assets'
                    FILTER edge._id NOT LIKE 'hasMediafile%'
                    LET relation = {'key': edge._to, 'type': edge.type}
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
            f"{aql}{aql2}", rawResults=True, bindVars=bind, fullCount=True
        )
        items = dict()
        items["count"] = results.extra["stats"]["fullCount"]
        items["results"] = list(results)
        if ids:
            items["results"] = [
                result_item
                for i in ids
                for result_item in items["results"]
                if result_item["_key"] == i
            ]
        return items

    def __generate_unique_code(self):
        codes = ["".join(random.choices(string.digits, k=8)) for i in range(5)]
        aql = """
            FOR bv IN @@collection
                FILTER bv.code IN @code_list
                RETURN bv.code
        """
        bind = {"@collection": "box_visits", "code_list": codes}
        results = list(self.db.AQLQuery(aql, rawResults=True, bindVars=bind))
        return next((x for x in codes if x not in results), None)

    def generate_box_visit_code(self):
        code = self.__generate_unique_code()
        while not code:
            code = self.__generate_unique_code()
        return code

    def get_entities(self, skip, limit, skip_relations=0, filters=None):
        aql = f"""
            WITH mediafiles
            FOR c IN entities
                {"FILTER c._key IN @ids" if "ids" in filters else ""}
                {f'FILTER c.type == "{filters["type"]}"' if "type" in filters else ""}
                {f'FILTER c.user == "{filters["user"]}"' if "user" in filters else ""}
        """
        if skip_relations == 1:
            aql2 = "LET all_metadata = {'metadata': c.metadata}"
        else:
            aql2 = """
                LET new_metadata = (
                    FOR item,edge IN OUTBOUND c GRAPH 'assets'
                        FILTER edge._id NOT LIKE 'hasMediafile%' AND edge._id NOT LIKE 'contains%'
                        LET relation = {'key': edge._to, 'type': FIRST(SPLIT(edge._id, '/'))}
                        RETURN HAS(edge, 'label') ? MERGE(relation, {'label': IS_NULL(edge.label.`@value`) ? edge.label : edge.label.`@value`}) : relation
                )
                LET all_metadata = {'metadata': APPEND(c.metadata, new_metadata)}
            """
        aql3 = """
            LET primary_items = (
                FOR item, edge IN OUTBOUND c hasMediafile
                    FILTER edge.is_primary == true || edge.is_primary_thumbnail == true
                    LET primary = edge.is_primary != true ? null : {primary_mediafile_location: item.original_file_location, primary_mediafile: item.filename, primary_transcode: item.transcode_filename, primary_transcode_location: item.transcode_file_location, primary_width: item.img_width, primary_height: item.img_height}
                    LET primary_thumb = edge.is_primary_thumbnail != true ? null : {primary_thumbnail_location: item.thumbnail_file_location}
                    RETURN primary != null AND primary_thumb != null ? MERGE(primary, primary_thumb) : (primary ? primary : primary_thumb)
            )
            LET merged_primary_items = COUNT(primary_items) > 1 ? MERGE(FIRST(primary_items), LAST(primary_items)) : FIRST(primary_items)
            LIMIT @skip, @limit
            RETURN merged_primary_items == null ? MERGE(c, all_metadata) : MERGE(c, all_metadata, merged_primary_items)
        """
        bind = {"skip": skip, "limit": limit}
        if "ids" in filters:
            bind["ids"] = filters["ids"]
        results = self.db.AQLQuery(
            aql + aql2 + aql3, rawResults=True, bindVars=bind, fullCount=True
        )
        items = dict()
        items["count"] = results.extra["stats"]["fullCount"]
        items["results"] = list(results)
        if "ids" in filters:
            items["results"] = [
                result_item
                for i in filters["ids"]
                for result_item in items["results"]
                if result_item["_key"] == i
            ]
        return items

    def get_items_from_collection(self, collection, skip=0, limit=20):
        items = dict()
        results = self.db[collection].fetchAll(skip=skip, limit=limit, rawResults=True)
        items["count"] = self.db[collection].count()
        items["results"] = list(results)
        return items

    def get_items_from_collection_by_fields(self, collection, fields, skip=0, limit=20):
        items = dict()
        extra_query = ""
        for name, value in fields.items():
            extra_query += f'FILTER c.{name} == "{value}"\n'
        aql = f"""
            FOR c IN @@collection
                {extra_query}
                LIMIT @skip, @limit
                RETURN c
        """
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

    def __get_relevant_relations(self, type):
        return {
            "asset": ["isIn", "components", "parent"],
            "thesaurus": [],
            "museum": [],
            "box_visit": ["stories", "visited", "story_box"],
            "box": ["box_stories"],
            "story": ["frames", "box", "story_box_visits"],
            "frame": ["stories", "components"],
        }.get(type, ["components"])

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude_relations=None
    ):
        if exclude_relations is None:
            exclude_relations = []
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        relevant_relations = self.__get_relevant_relations(entity["type"])
        relations = []
        for relation in relevant_relations:
            if relation not in exclude_relations:
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
                            and (
                                relation_object["label"] in ["MaterieelDing.bestaatUit"]
                            )
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
                                    collection,
                                    relation_object["key"].split("entities/")[1],
                                )
                                for sub_edge2 in sub_entity2.getOutEdges(
                                    self.db[relation]
                                ):
                                    relation_object = {}
                                    sub_edge2 = sub_edge2.getStore()
                                    for key in sub_edge2.keys():
                                        if key[0] != "_":
                                            relation_object[key] = sub_edge2[key]
                                    relation_object["key"] = sub_edge2["_to"]
                                    relation_object["type"] = relation
                                    if (
                                        relation_object not in relations
                                        and relation_object["label"]
                                        != "vervaardiger.rol"
                                    ):
                                        relations.append(relation_object)

                            elif relation_object["label"] != "vervaardiger.rol":
                                if relation_object not in relations:
                                    relations.append(relation_object)

        return relations

    def __add_mediafile_to_list(self, mediafile, mediafiles):
        if "is_primary" in mediafile and mediafile["is_primary"]:
            mediafiles.insert(0, mediafile)
        elif "is_primary_thumbnail" in mediafile and mediafile["is_primary_thumbnail"]:
            mediafiles.insert(1, mediafile)
        else:
            mediafiles.append(mediafile)

    def get_collection_item_mediafiles(self, collection, id):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        mediafiles = list()
        for edge in entity.getOutEdges(self.db["hasMediafile"]):
            mediafile = self.db.fetchDocument(edge["_to"]).getStore()
            if "is_primary" in edge:
                mediafile["is_primary"] = edge["is_primary"]
            if "is_primary_thumbnail" in edge:
                mediafile["is_primary_thumbnail"] = edge["is_primary_thumbnail"]
            self.__add_mediafile_to_list(mediafile, mediafiles)
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
            optional_label = (
                self._map_entity_relation_parent_label(relation["label"])
                if "label" in relation
                else None
            )
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
        try:
            item = self.db[collection].createDocument(content)
            item.save()
        except CreationError as ce:
            if ce.errors["code"] == 409:
                raise NonUniqueException(ce.errors["errorMessage"])
            raise ce
        return item.getStore()

    def update_item_from_collection(self, collection, id, content):
        raw_item = self.get_raw_item_from_collection_by_id(collection, id)
        try:
            raw_item.set(content)
            raw_item.save()
            item = raw_item.getStore()
        except UpdateError as ue:
            if ue.errors["code"] == 409:
                raise NonUniqueException(ue.errors["errorMessage"])
            raise ue
        self._trigger_child_relation_changed(collection, id)
        return item

    def _trigger_child_relation_changed(self, collection, id):
        attributes = {"type": "dams.child_relation_changed", "source": "dams"}
        data = {"parent_id": id, "collection": collection}
        event = CloudEvent(attributes, data)
        message = json.loads(to_json(event))
        app.rabbit.send(message, routing_key="dams.child_relation_changed")

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

    def __remove_edges(self, entity, relation, edge_collection):
        for edge in entity.getEdges(self.db[edge_collection]):
            if edge["_from"] == relation["key"] or edge["_to"] == relation["key"]:
                edge.delete()

    def __remove_relations(self, entity, relations, parent=True):
        for relation in relations:
            self.__remove_edges(entity, relation, relation["type"])
            if parent:
                self.__remove_edges(
                    entity, relation, self._map_entity_relation(relation["type"])
                )

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        self.__remove_relations(entity, content, parent)
        return self.add_relations_to_collection_item(collection, id, content, parent)

    def delete_collection_item_relations(self, collection, id, content, parent=True):
        entity = self.get_raw_item_from_collection_by_id(collection, id)
        self.__remove_relations(entity, content, parent)

    def patch_item_from_collection(self, collection, id, content):
        raw_item = self.get_raw_item_from_collection_by_id(collection, id)
        try:
            raw_item.set(content)
            raw_item.patch()
            item = raw_item.getStore()
        except UpdateError as ue:
            if ue.errors["code"] == 409:
                raise NonUniqueException(ue.errors["errorMessage"])
            raise ue
        self._trigger_child_relation_changed(collection, id)
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

    def handle_mediafile_deleted(self, parents):
        for item in parents:
            if item["primary_mediafile"] or item["primary_thumbnail"]:
                raw_entity = self.db.fetchDocument(item["entity_id"])
                self._set_new_primary(
                    raw_entity, item["primary_mediafile"], item["primary_thumbnail"]
                )

    def get_mediafile_linked_entities(self, mediafile):
        linked_entities = []
        for edge in self.db["hasMediafile"].getEdges(mediafile["_id"]):
            linked_entities.append(
                {
                    "entity_id": edge["_from"],
                    "primary_mediafile": edge["is_primary"],
                    "primary_thumbnail": edge["is_primary_thumbnail"],
                }
            )
        return linked_entities

    def __signal_entity_changed(self, entity):
        attributes = {"type": "dams.entity_changed", "source": "dams"}
        data = {
            "location": f'/entities/{entity["_key"]}',
            "type": entity["type"] if "type" in entity else "unspecified",
        }
        event = CloudEvent(attributes, data)
        message = json.loads(to_json(event))
        app.rabbit.send(message, routing_key="dams.entity_changed")

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        if parents:
            for item in parents:
                entity = self.db.fetchDocument(item["entity_id"]).getStore()
                self.__signal_entity_changed(entity)
        if mediafile:
            for edge in self.db.fetchDocument(mediafile["_id"]).getInEdges(
                self.db["hasMediafile"]
            ):
                entity = self.db.fetchDocument(edge["_from"]).getStore()
                self.__signal_entity_changed(entity)

    def update_parent_relation_values(self, collection, parent_id):
        raw_entity = self.get_raw_item_from_collection_by_id(collection, parent_id)
        entity = raw_entity.getStore()
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
            parent_ids_from_changed_edges = []
            for edgeType in ["isIn", "components"]:
                for edge in raw_entity.getEdges(self.db[edgeType]):
                    if edge["key"] == entity["_id"]:
                        # only patch if new value is different from old value
                        if edge["value"] != new_value:
                            patch = {"value": new_value}
                            edge.set(patch)
                            edge.patch()
                            parent_ids_from_changed_edges.append(entity["_key"])
                            # send event message in batches
                            if (
                                len(parent_ids_from_changed_edges)
                                > self.event_batch_limit
                            ):
                                self._send_edge_changed_message(
                                    parent_ids_from_changed_edges
                                )
                                parent_ids_from_changed_edges = []
            # send remaining messages
            if len(parent_ids_from_changed_edges) > 0:
                self._send_edge_changed_message(parent_ids_from_changed_edges)

    def _send_edge_changed_message(self, parent_ids_from_changed_edges):
        attributes = {"type": "dams.edge_changed", "source": "dams"}
        data = {
            "location": f'/entities?ids={",".join(parent_ids_from_changed_edges)}&skip_relations=1'
        }
        event = CloudEvent(attributes, data)
        message = json.loads(to_json(event))
        app.rabbit.send(message, routing_key="dams.edge_changed")
        if self.event_delay > 0:
            sleep(self.event_delay)

    def _map_entity_relation(self, relation):
        return {
            "box": "box_stories",
            "box_stories": "box",
            "components": "parent",
            "contains": "isIn",
            "frames": "stories",
            "isIn": "contains",
            "parent": "components",
            "stories": "frames",
        }.get(relation)

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
            elif edge_name in ["stories", "story_box"]:
                fr = ["box_visits"]
            elif edge_name == "story_box_visits":
                to = ["box_visits"]
            try:
                self.conn.define_edge_in_graph(
                    self.default_graph_name,
                    self.arango_db_name,
                    {"collection": edge_name, "from": fr, "to": to},
                )
            except CreationError:
                continue
        return self.conn[self.arango_db_name]

    def check_health(self):
        return self.conn.get_cluster_health(self.arango_db_name)
