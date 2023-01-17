import app
import os
import uuid
import util

from pyArango.theExceptions import CreationError, DocumentNotFoundError, UpdateError
from storage.genericstore import GenericStorageManager
from storage.py_arango_connection_extension import PyArangoConnection as Connection


class ArangoStorageManager(GenericStorageManager):
    def __init__(self):
        self.arango_db_name = os.getenv("ARANGO_DB_NAME")
        self.default_graph_name = os.getenv("DEFAULT_GRAPH", "assets")
        self.collections = [
            "abstracts",
            "box_visits",
            "entities",
            "history",
            "jobs",
            "key_value_store",
            "mediafiles",
        ]
        self.entity_relations = [
            "box",
            "box_stories",
            "components",
            "contains",
            "frames",
            "hasTestimony",
            "isIn",
            "isTestimonyFor",
            "parent",
            "stories",
            "story_box",
            "story_box_visits",
            "visited",
        ]
        self.edges = [*self.entity_relations, "hasMediafile"]
        self.conn = Connection(
            arangoURL=os.getenv("ARANGO_DB_HOST"),
            username=os.getenv("ARANGO_DB_USERNAME"),
            password=os.getenv("ARANGO_DB_PASSWORD"),
        )
        self.db = self.__create_database_if_not_exists()
        self.key_cache = {}

    def __create_database_if_not_exists(self):
        if not self.conn.hasDatabase(self.arango_db_name):
            self.conn.createDatabase(self.arango_db_name)
        for collection in self.collections:
            try:
                self.conn.createCollection(collection, self.arango_db_name)
            except CreationError:
                continue
        self.__create_unique_indexes(self.arango_db_name)
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

    def __create_unique_indexes(self, arango_db_name):
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

    def __get_collection_item_sub_item_aql(self, collection, id, sub_item):
        aql = """
            FOR c in @@collection
                FILTER c.object_id == @id OR @id IN c.identifiers OR c._key == @id
                RETURN c.@sub_item
        """
        bind = {"id": id, "@collection": collection, "sub_item": sub_item}
        result = self.db.AQLQuery(aql, rawResults=True, bindVars=bind)
        result_len = result.__len__()
        if not result_len:
            return None
        return list(result) if result_len > 1 else result[0]

    def __get_mediafile_index(self, mediafile, highest_order):
        if "order" in mediafile:
            return mediafile["order"]
        if mediafile.get("is_primary", False):
            return highest_order + 1
        if mediafile.get("is_primary_thumbnail", False):
            return highest_order + 2
        return highest_order + 3

    def __get_primary_items(self, raw_entity):
        result = {"primary_mediafile": "", "primary_thumbnail": ""}
        for edge in raw_entity.getOutEdges(self.db["hasMediafile"]):
            if "is_primary" in edge and edge["is_primary"]:
                result["primary_mediafile"] = edge["_to"]
            if "is_primary_thumbnail" in edge and edge["is_primary_thumbnail"]:
                result["primary_thumbnail"] = edge["_to"]
        return result

    def __get_raw_item_from_collection_by_id(self, collection, id):
        if item := self.__try_get_item_from_collection_by_key(collection, id):
            return item
        if key := self.key_cache.get(id):
            return self.__try_get_item_from_collection_by_key(collection, key)
        if key := self.__get_collection_item_sub_item_aql(collection, id, "_key"):
            self.key_cache[id] = key
            return self.__try_get_item_from_collection_by_key(collection, key)
        return None

    def __get_relevant_relations(self, type, exclude=None):
        relations = {
            "asset": ["isIn", "components", "parent", "hasTestimony"],
            "box": ["box_stories"],
            "box_visit": ["stories", "visited", "story_box"],
            "frame": ["stories", "components"],
            "museum": [],
            "story": ["frames", "box", "story_box_visits"],
            "testimony": ["isTestimonyFor"],
            "thesaurus": [],
        }.get(type, ["components"])
        return [x for x in relations if not exclude or x not in exclude]

    def __invalidate_key_cache_for_item(self, item):
        for id in item["identifiers"]:
            if id in self.key_cache:
                del self.key_cache[id]
        if "object_id" in item and item["object_id"] in self.key_cache:
            del self.key_cache[item["object_id"]]

    def __map_entity_relation(self, relation):
        return {
            "box": "box_stories",
            "box_stories": "box",
            "components": "parent",
            "contains": "isIn",
            "frames": "stories",
            "hasTestimony": "isTestimonyFor",
            "isIn": "contains",
            "isTestimonyFor": "hasTestimony",
            "parent": "components",
            "stories": "frames",
        }.get(relation)

    def __remove_edges(self, entity, relation, edge_collection):
        for edge in entity.getEdges(self.db[edge_collection]):
            if edge["_from"] == relation["key"] or edge["_to"] == relation["key"]:
                edge.delete()

    def __remove_relations(self, entity, relations, parent=True):
        for relation in relations:
            self.__remove_edges(entity, relation, relation["type"])
            if parent:
                self.__remove_edges(
                    entity, relation, self.__map_entity_relation(relation["type"])
                )

    def __set_new_primary(self, raw_entity, mediafile=False, thumbnail=False):
        for edge in raw_entity.getOutEdges(self.db["hasMediafile"]):
            potential_mediafile = self.db.fetchDocument(edge["_to"]).getStore()
            if util.mediafile_is_public(potential_mediafile):
                if mediafile:
                    edge["is_primary"] = True
                if thumbnail:
                    edge["is_primary_thumbnail"] = True
                edge.save()
                return

    def __try_get_item_from_collection_by_key(self, collection, key):
        try:
            item = self.db[collection][key]
        except DocumentNotFoundError:
            item = None
        return item

    def add_mediafile_to_collection_item(
        self, collection, id, mediafile_id, mediafile_public
    ):
        entity = self.__get_raw_item_from_collection_by_id(collection, id)
        if not entity:
            return None
        extra_data = {
            "is_primary": mediafile_public,
            "is_primary_thumbnail": mediafile_public,
        }
        if mediafile_public:
            for edge in entity.getOutEdges(self.db["hasMediafile"]):
                if "is_primary" in edge and edge["is_primary"]:
                    extra_data["is_primary"] = False
                if "is_primary_thumbnail" in edge and edge["is_primary_thumbnail"]:
                    extra_data["is_primary_thumbnail"] = False
        self.db.graphs[self.default_graph_name].createEdge(
            "hasMediafile", entity["_id"], mediafile_id, extra_data
        )
        return self.db.fetchDocument(mediafile_id).getStore()

    def add_relations_to_collection_item(self, collection, id, relations, parent=True):
        entity = self.__get_raw_item_from_collection_by_id(collection, id)
        if not entity:
            return None
        for relation in relations:
            extra_data = {}
            for key in [x for x in relation.keys() if x[0] != "_"]:
                extra_data[key] = relation[key]
            self.db.graphs[self.default_graph_name].createEdge(
                relation["type"], entity["_id"], relation["key"], extra_data
            )
            if not parent:
                continue
            extra_data = {}
            if relation.get("label") == "GecureerdeCollectie.bestaatUit":
                extra_data = {
                    "label": "Collectie.naam",
                    "value": entity["data"]["MensgemaaktObject.titel"]["@value"],
                }
            self.db.graphs[self.default_graph_name].createEdge(
                self.__map_entity_relation(relation["type"]),
                relation["key"],
                entity["_id"],
                extra_data,
            )
        return relations

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

    def check_health(self):
        return self.conn.get_cluster_health(self.arango_db_name)

    def delete_collection_item_relations(self, collection, id, content, parent=True):
        entity = self.__get_raw_item_from_collection_by_id(collection, id)
        self.__remove_relations(entity, content, parent)

    def delete_collection_item_sub_item_key(self, collection, id, sub_item, key):
        aql = """
            FOR c IN @@collection
                FILTER @id IN c.identifiers OR c._key == @id
                FILTER c.@sub_item != null
                LET filteredSubItems = (
                    FOR obj IN c.@sub_item
                        FILTER obj.key != @key
                        RETURN obj
                )
                UPDATE c WITH {@sub_item: filteredSubItems} IN @@collection
        """
        bind = {"@collection": collection, "id": id, "sub_item": sub_item, "key": key}
        self.db.AQLQuery(aql, rawResults=True, bindVars=bind)

    def delete_item_from_collection(self, collection, id):
        item = self.__get_raw_item_from_collection_by_id(collection, id)
        self.__invalidate_key_cache_for_item(item)
        for edge_name in self.edges:
            for edge in item.getEdges(self.db[edge_name]):
                edge.delete()
        item.delete()

    def drop_all_collections(self):
        for collection in [*self.collections, *self.edges]:
            self.db[collection].truncate()

    def get_collection_item_mediafiles(self, collection, id):
        entity = self.__get_raw_item_from_collection_by_id(collection, id)
        mediafiles = list()
        highest_order = -1
        for edge in entity.getOutEdges(self.db["hasMediafile"]):
            mediafile = self.db.fetchDocument(edge["_to"]).getStore()
            if "is_primary" in edge:
                mediafile["is_primary"] = edge["is_primary"]
            if "is_primary_thumbnail" in edge:
                mediafile["is_primary_thumbnail"] = edge["is_primary_thumbnail"]
            if "order" in mediafile and mediafile["order"] > highest_order:
                highest_order = mediafile["order"]
            mediafiles.append(mediafile)
        return sorted(
            mediafiles, key=lambda x: self.__get_mediafile_index(x, highest_order)
        )

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude=None
    ):
        if not exclude:
            exclude = []
        entity = self.__get_raw_item_from_collection_by_id(collection, id)
        relevant_relations = self.__get_relevant_relations(entity["type"], exclude)
        relations = []
        for relation in relevant_relations:
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
                    sub_entity = self.__get_raw_item_from_collection_by_id(
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
                            sub_entity2 = self.__get_raw_item_from_collection_by_id(
                                collection,
                                relation_object["key"].split("entities/")[1],
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

    def get_entities(self, skip=0, limit=20, skip_relations=0, filters=None):
        if not filters:
            filters = {}
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
            items["results"].sort(key=lambda x: filters["ids"].index(x["_key"]))
        return items

    def get_history_for_item(self, collection, id, timestamp=None, all_entries=None):
        aql = f"""
            FOR h IN history
                FILTER h.collection == '{collection}'
                FILTER h.object._key == '{id}' OR h.object.object_id == '{id}' OR '{id}' IN h.object.identifiers
                {f"SORT DATE_DIFF(h.timestamp, {timestamp}, 's', true)" if timestamp else "SORT h.timestamp DESC"}
                {"LIMIT 1" if not all_entries else ""}
                RETURN h
        """
        history = list(self.db.AQLQuery(aql, rawResults=True))
        return history if all_entries else history[0]

    def get_item_from_collection_by_id(self, collection, id):
        if item := self.__get_raw_item_from_collection_by_id(collection, id):
            item = item.getStore()
        return item

    def get_items_from_collection(
        self,
        collection,
        skip=0,
        limit=20,
        fields=None,
        filters=None,
        sort=None,
        asc=True,
    ):
        items = dict()
        extra_query = ""
        title_filter = ""
        if not fields:
            fields = {}
        if not filters:
            filters = {}
        for name, value in fields.items():
            if value is None:
                extra_query += f"FILTER c.{name} == null\n"
            else:
                extra_query += f'FILTER c.{name} == "{value}"\n'
        if "title" in filters:
            title_filter = f"""
                FOR metadata IN c.metadata
                    FILTER metadata.key == "title"
                    FILTER LIKE(metadata.value, "%{filters["title"]}%", true)
            """
        aql = f"""
            FOR c IN @@collection
                {"FILTER c._key IN @ids" if "ids" in filters else ""}
                {"FILTER c.user == @user_or_public OR NOT c.private" if "user_or_public" in filters else ""}
                {extra_query}
                {title_filter}
                {f'SORT c.{sort} {"ASC" if asc else "DESC"}' if sort else ""}
                LIMIT @skip, @limit
                RETURN c
        """
        bind = {"@collection": collection, "skip": skip, "limit": limit}
        if "ids" in filters:
            bind["ids"] = filters["ids"]
        if "user_or_public" in filters:
            bind["user_or_public"] = filters["user_or_public"]
        results = self.db.AQLQuery(aql, rawResults=True, bindVars=bind, fullCount=True)
        items["count"] = results.extra["stats"]["fullCount"]
        items["results"] = list(results)
        return items

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

    def get_metadata_values_for_collection_item_by_key(self, collection, key):
        aql = """
            FOR c IN @@collection
                FILTER c.metadata != NULL
                FOR metadata IN c.metadata
                    FILTER metadata.key == @key
                    RETURN DISTINCT(metadata.value)
        """
        bind = {"@collection": collection, "key": key}
        results = self.db.AQLQuery(aql, rawResults=True, bindVars=bind)
        return list(results)

    def handle_mediafile_deleted(self, parents):
        for item in parents:
            if item["primary_mediafile"] or item["primary_thumbnail"]:
                raw_entity = self.db.fetchDocument(item["entity_id"])
                self.__set_new_primary(
                    raw_entity, item["primary_mediafile"], item["primary_thumbnail"]
                )

    def handle_mediafile_status_change(self, mediafile):
        for edge in self.db.fetchDocument(mediafile["_id"]).getInEdges(
            self.db["hasMediafile"]
        ):
            raw_entity = self.db.fetchDocument(edge["_from"])
            primary_items = self.__get_primary_items(raw_entity)
            if util.mediafile_is_public(mediafile):
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
                    self.__set_new_primary(
                        raw_entity, change_primary_mediafile, change_primary_thumbnail
                    )

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        self.delete_collection_item_relations(collection, id, content, parent)
        return self.add_relations_to_collection_item(collection, id, content, parent)

    def patch_item_from_collection(self, collection, id, content):
        raw_item = self.__get_raw_item_from_collection_by_id(collection, id)
        try:
            raw_item.set(content)
            raw_item.patch()
            item = raw_item.getStore()
        except UpdateError as ue:
            if ue.errors["code"] == 409:
                raise util.NonUniqueException(ue.errors["errorMessage"])
            raise ue
        util.signal_child_relation_changed(collection, id)
        return item

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        if mediafile:
            parents = self.get_mediafile_linked_entities(mediafile)
        for item in parents:
            entity = self.db.fetchDocument(item["entity_id"]).getStore()
            util.signal_entity_changed(entity)

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
                raise util.NonUniqueException(ce.errors["errorMessage"])
            raise ce
        return item.getStore()

    def set_primary_field_collection_item(
        self, collection, entity_id, mediafile_id, field
    ):
        entity = self.__get_raw_item_from_collection_by_id(collection, entity_id)
        for edge in entity.getOutEdges(self.db["hasMediafile"]):
            new_primary_id = f"mediafiles/{mediafile_id}"
            if edge["_to"] != new_primary_id and field in edge and edge[field]:
                edge[field] = False
                edge.save()
            elif edge["_to"] == new_primary_id and field in edge and not edge[field]:
                edge[field] = True
                edge.save()

    def update_collection_item_relations(self, collection, id, content, parent=True):
        entity = self.__get_raw_item_from_collection_by_id(collection, id)
        for relation in self.entity_relations:
            for edge in entity.getEdges(self.db[relation]):
                edge.delete()
        return self.add_relations_to_collection_item(collection, id, content, parent)

    def update_item_from_collection(self, collection, id, content):
        raw_item = self.__get_raw_item_from_collection_by_id(collection, id)
        try:
            raw_item.set(content)
            raw_item.save()
            item = raw_item.getStore()
        except UpdateError as ue:
            if ue.errors["code"] == 409:
                raise util.NonUniqueException(ue.errors["errorMessage"])
            raise ue
        util.signal_child_relation_changed(collection, id)
        return item

    def update_parent_relation_values(self, collection, parent_id):
        raw_entity = self.__get_raw_item_from_collection_by_id(collection, parent_id)
        entity = raw_entity.getStore()

        def get_value_from_metadata():
            for title_key in ["title", "fullname", "fullName", "description"]:
                for metadata in entity.get("metadata", list()):
                    if metadata.get("key") == title_key:
                        return metadata["value"]
            return None

        if not (new_value := get_value_from_metadata()):
            return
        changed_ids = set()
        for edge_type in ["isIn", "components"]:
            for edge in raw_entity.getEdges(self.db[edge_type]):
                if edge["key"] == entity["_id"] and edge["value"] != new_value:
                    edge.set({"value": new_value})
                    edge.patch()
                    changed_ids.add(entity["_key"])
        if len(changed_ids):
            util.signal_edge_changed(changed_ids)
