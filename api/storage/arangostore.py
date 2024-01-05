import app
import os

from arango import (
    ArangoClient,
    DocumentInsertError,
    DocumentReplaceError,
    DocumentUpdateError,
)
from elody.exceptions import NonUniqueException
from elody.util import (
    custom_json_dumps,
    mediafile_is_public,
    signal_child_relation_changed,
    signal_edge_changed,
    signal_entity_changed,
)
from storage.genericstore import GenericStorageManager


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
        self.client = ArangoClient(
            hosts=os.getenv("ARANGO_DB_HOST"), serializer=custom_json_dumps
        )
        self.sys_db = self.client.db(
            "_system",
            username=os.getenv("ARANGO_DB_USERNAME"),
            password=os.getenv("ARANGO_DB_PASSWORD"),
        )
        self.db = None
        self.id_cache = {}
        self.__create_database_if_not_exists()

    def __create_database_if_not_exists(self):
        if not self.sys_db.has_database(self.arango_db_name):
            self.sys_db.create_database(self.arango_db_name)
        self.db = self.client.db(
            self.arango_db_name,
            username=os.getenv("ARANGO_DB_USERNAME"),
            password=os.getenv("ARANGO_DB_PASSWORD"),
        )
        for collection in self.collections:
            if not self.db.has_collection(collection):
                self.db.create_collection(collection)
        entity_indices = self.db.collection("entities").indexes()
        box_visits_indices = self.db.collection("box_visits").indexes()
        if not any(x["fields"] == ["identifiers[*]"] for x in entity_indices):
            self.db.collection("entities").add_hash_index(
                fields=["identifiers[*]"], unique=True
            )
        if not any(x["fields"] == ["object_id"] for x in entity_indices):
            self.db.collection("entities").add_hash_index(
                fields=["object_id"], unique=True, sparse=True
            )
        if not any(x["fields"] == ["code"] for x in box_visits_indices):
            self.db.collection("box_visits").add_hash_index(
                fields=["code"], unique=True, sparse=True
            )
        if not self.db.has_graph(self.default_graph_name):
            self.db.create_graph(self.default_graph_name)
        graph = self.db.graph(self.default_graph_name)
        for edge in self.edges:
            if graph.has_edge_definition(edge):
                continue
            definitions = {
                "hasMediafile": ("entities", "mediafiles"),
                "stories": ("box_visits", "entities"),
                "story_box": ("box_visits", "entities"),
                "story_box_visits": ("entities", "box_visits"),
            }
            fr, to = definitions.get(edge, ("entities", "entities"))
            graph.create_edge_definition(
                edge_collection=edge,
                from_vertex_collections=[fr],
                to_vertex_collections=[to],
            )

    def __get_collection_item_sub_item_aql(self, collection, id, sub_item):
        aql = """
            FOR c in @@collection
                FILTER c.object_id == @id OR @id IN c.identifiers OR c._key == @id
                RETURN c.@sub_item
        """
        bind = {"id": id, "@collection": collection, "sub_item": sub_item}
        result = list(self.db.aql.execute(aql, bind_vars=bind))
        if not result:
            return None
        return result if len(result) > 1 else result[0]

    def __get_id_for_collection_item(self, collection, identifier):
        if item_id := self.id_cache.get(identifier):
            return item_id
        if self.db.collection(collection).has(identifier):
            if identifier.startswith(f"{collection}/"):
                self.id_cache[identifier] = identifier
                self.id_cache[identifier.removeprefix(f"{collection}/")] = identifier
                return identifier
            else:
                new_identifier = f"{collection}/{identifier}"
                self.id_cache[identifier] = new_identifier
                self.id_cache[new_identifier] = new_identifier
                return new_identifier
        if item_id := self.__get_collection_item_sub_item_aql(
            collection, identifier, "_id"
        ):
            self.id_cache[identifier] = item_id
            return item_id
        return None

    def __get_primary_items(self, item_id):
        result = {"primary_mediafile": "", "primary_thumbnail": ""}
        for edge in self.db.collection("hasMediafile").find({"_from": item_id}):
            if edge["is_primary"]:
                result["primary_mediafile"] = edge["_to"]
            if edge["is_primary_thumbnail"]:
                result["primary_thumbnail"] = edge["_to"]
        return result

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

    def __invalidate_id_cache_for_item(self, item_id):
        self.id_cache = {
            key: value for key, value in self.id_cache.items() if value != item_id
        }

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

    def __remove_edges(self, item_id, relation, edge_collection):
        for edge in self.db.aql.execute(
            f"FOR i IN {edge_collection} FILTER i._from == '{item_id}' OR i._to == '{item_id}' RETURN i"
        ):
            if edge["_from"] == relation["key"] or edge["_to"] == relation["key"]:
                self.db.delete_document(edge)

    def __remove_relations(self, item_id, relations, parent=True):
        for relation in relations:
            self.__remove_edges(item_id, relation, relation["type"])
            if parent:
                self.__remove_edges(
                    item_id, relation, self.__map_entity_relation(relation["type"])
                )

    def __set_new_primary(self, item_id, mediafile=False, thumbnail=False):
        for edge in self.db.collection("hasMediafile").find({"_from": item_id}):
            potential_mediafile = self.db.document(edge["_to"])
            if mediafile_is_public(potential_mediafile):
                if mediafile:
                    edge["is_primary"] = True
                if thumbnail:
                    edge["is_primary_thumbnail"] = True
                self.db.update_document(edge)
                return

    def add_mediafile_to_collection_item(
        self, collection, id, mediafile_id, mediafile_public
    ):
        if not (item_id := self.__get_id_for_collection_item(collection, id)):
            return None
        data = {
            "_from": item_id,
            "_to": mediafile_id,
            "is_primary": mediafile_public,
            "is_primary_thumbnail": mediafile_public,
        }
        if mediafile_public:
            for edge in self.db.collection("hasMediafile").find({"_from": item_id}):
                if "is_primary" in edge and edge["is_primary"]:
                    data["is_primary"] = False
                if "is_primary_thumbnail" in edge and edge["is_primary_thumbnail"]:
                    data["is_primary_thumbnail"] = False
        self.db.graph(self.default_graph_name).edge_collection("hasMediafile").insert(
            data
        )
        return self.db.document(mediafile_id)

    def add_relations_to_collection_item(
        self, collection, id, relations, parent=True, dst_collection=None
    ):
        if not (item_id := self.__get_id_for_collection_item(collection, id)):
            return None
        for relation in relations:
            data = {}
            for key in [x for x in relation.keys() if x[0] != "_"]:
                data[key] = relation[key]
            data["_from"] = item_id
            data["_to"] = relation["key"]
            self.db.graph(self.default_graph_name).edge_collection(
                relation["type"]
            ).insert(data)
            if not parent:
                continue
            data = {"_from": relation["key"], "_to": item_id}
            if relation.get("label") == "GecureerdeCollectie.bestaatUit":
                data["label"] = "Collectie.naam"
                data["value"] = self.db.document(item_id)["data"][
                    "MensgemaaktObject.titel"
                ]["@value"]
            self.db.graph(self.default_graph_name).edge_collection(
                self.__map_entity_relation(relation["type"])
            ).insert(data)
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
        self.db.aql.execute(aql, bind_vars=bind)
        return content

    def check_health(self):
        return self.db.conn.ping()

    def delete_collection_item_relations(self, collection, id, content, parent=True):
        item_id = self.__get_id_for_collection_item(collection, id)
        self.__remove_relations(item_id, content, parent)

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
        self.db.aql.execute(aql, bind_vars=bind)

    def delete_item_from_collection(self, collection, id):
        item_id = self.__get_id_for_collection_item(collection, id)
        self.__invalidate_id_cache_for_item(item_id)
        self.db.graph(self.default_graph_name).vertex_collection(collection).delete(
            item_id
        )

    def drop_all_collections(self):
        for collection in [*self.collections, *self.edges]:
            self.db.collection(collection).truncate()

    def get_collection_item_mediafiles(self, collection, id, skip=0, limit=0, asc=1, sort="order"):
        item_id = self.__get_id_for_collection_item(collection, id)
        mediafiles = list()
        edges = list(
            self.db.collection("hasMediafile").find(
                {"_from": item_id}, skip=skip, limit=limit
            )
        )
        edges.sort(key=lambda x: x["order"] if "order" in x else len(edges) + 1)
        for edge in edges:
            mediafile = self.db.document(edge["_to"])
            if "is_primary" in edge:
                mediafile["is_primary"] = edge["is_primary"]
            if "is_primary_thumbnail" in edge:
                mediafile["is_primary_thumbnail"] = edge["is_primary_thumbnail"]
            mediafiles.append(mediafile)
        return mediafiles

    def get_collection_item_mediafiles_count(self, id):
        query = """
            FOR hm IN hasMediafile
                FILTER hm._from == @id
                COLLECT WITH COUNT INTO length
                RETURN length
        """
        bind_vars = {"id": id}
        return list(self.db.aql.execute(query, bind_vars=bind_vars))[0]

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude=None
    ):
        if not exclude:
            exclude = []
        entity = self.get_item_from_collection_by_id(collection, id)
        relevant_relations = self.__get_relevant_relations(entity["type"], exclude)
        relations = []
        for relation in relevant_relations:
            for edge in self.db.collection(relation).find({"_from": entity["_id"]}):
                relation_object = {}
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
                    sub_entity = self.get_item_from_collection_by_id(
                        collection, relation_object["key"].split("entities/")[1]
                    )
                    for sub_edge in self.db.collection(relation).find(
                        {"_from": sub_entity["_id"]}
                    ):
                        relation_object = {}
                        for key in sub_edge.keys():
                            if key[0] != "_":
                                relation_object[key] = sub_edge[key]
                        relation_object["key"] = sub_edge["_to"]
                        relation_object["type"] = relation
                        if relation_object["value"] == "Creatie":
                            sub_entity2 = self.get_item_from_collection_by_id(
                                collection,
                                relation_object["key"].split("entities/")[1],
                            )
                            for sub_edge2 in self.db.collection(relation).find(
                                {"_from": sub_entity2["_id"]}
                            ):
                                relation_object = {}
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

    def get_entities(
        self,
        skip=0,
        limit=20,
        skip_relations=0,
        filters=None,
        order_by=None,
        ascending=True,
    ):
        if not filters:
            filters = {}
        aql = f"""
            WITH mediafiles
            FOR c IN entities
                {"FILTER c._key IN @ids" if "ids" in filters else ""}
                {f'FILTER c.type == "{filters["type"]}"' if "type" in filters else ""}
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
        results = self.db.aql.execute(
            aql + aql2 + aql3, bind_vars=bind, full_count=True
        )
        items = dict()
        items["count"] = results.statistics()["fullCount"]
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
        history = list(self.db.aql.execute(aql))
        return history if all_entries else history[0]

    def get_item_from_collection_by_id(self, collection, id):
        if item_id := self.__get_id_for_collection_item(collection, id):
            return self.db.document(item_id)
        return None

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
        results = self.db.aql.execute(aql, bind_vars=bind, full_count=True)
        items["count"] = results.statistics()["fullCount"]
        items["results"] = list(results)
        return items

    def get_mediafile_linked_entities(self, mediafile):
        linked_entities = []
        for edge in self.db.collection("hasMediafile").find(
            {"_from": mediafile["_id"]}
        ):
            linked_entities.append(
                {
                    "entity_id": edge["_from"],
                    "primary_mediafile": edge["is_primary"],
                    "primary_thumbnail": edge["is_primary_thumbnail"],
                }
            )
        return linked_entities

    def get_metadata_values_for_collection_item_by_key(self, collection, key):
        if key not in ["type"]:
            aql = """
                FOR c IN @@collection
                    FILTER c.metadata != NULL
                    FOR metadata IN c.metadata
                        FILTER metadata.key == @key
                        RETURN DISTINCT(metadata.value)
            """
        else:
            aql = """
                FOR c IN @@collection
                    FILTER c.@key != NULL
                    RETURN DISTINCT(@key)
            """
        bind = {"@collection": collection, "key": key}
        results = self.db.aql.execute(aql, bind_vars=bind)
        return list(results)

    def handle_mediafile_deleted(self, parents):
        for item in parents:
            if item["primary_mediafile"] or item["primary_thumbnail"]:
                self.__set_new_primary(
                    item["entity_id"],
                    item["primary_mediafile"],
                    item["primary_thumbnail"],
                )

    def handle_mediafile_status_change(self, mediafile):
        for edge in self.db.collection("hasMediafile").find({"_to": mediafile["_id"]}):
            primary_items = self.__get_primary_items(edge["_from"])
            if mediafile_is_public(mediafile):
                if not primary_items["primary_mediafile"]:
                    edge["is_primary"] = True
                    self.db.update_document(edge)
                if not primary_items["primary_thumbnail"]:
                    edge["is_primary_thumbnail"] = True
                    self.db.update_document(edge)
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
                    self.db.update_document(edge)
                    self.__set_new_primary(
                        edge["_from"],
                        change_primary_mediafile,
                        change_primary_thumbnail,
                    )

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        self.delete_collection_item_relations(collection, id, content, parent)
        return self.add_relations_to_collection_item(collection, id, content, parent)

    def patch_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ):
        try:
            item = self.db.collection(collection).update(
                {"_id": self.__get_id_for_collection_item(collection, id)} | content,
                return_new=True,
                check_rev=False,
            )["new"]
        except DocumentUpdateError as ex:
            if ex.error_code == 1210:
                raise NonUniqueException(ex.error_message)
            raise ex
        signal_child_relation_changed(app.rabbit, collection, item["_id"])
        return item

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        if mediafile:
            parents = self.get_mediafile_linked_entities(mediafile)
        for item in parents:
            entity = self.db.document(item["entity_id"])
            signal_entity_changed(app.rabbit, entity)

    def save_item_to_collection(
        self,
        collection,
        content,
        only_return_id=False,
        create_sortable_metadata=True,
    ):
        if not content.get("_key"):
            content["_key"] = self._get_autogenerated_id_for_item(content)
        content["identifiers"] = self._get_autogenerated_identifiers_for_item(content)
        try:
            ret = self.db.insert_document(
                collection, content, return_new=not only_return_id
            )
            return ret["_key"] if only_return_id else ret["new"]
        except DocumentInsertError as ex:
            if ex.error_code == 1210:
                raise NonUniqueException(ex.error_message)
            raise ex

    def set_primary_field_collection_item(self, collection, id, mediafile_id, field):
        item_id = self.__get_id_for_collection_item(collection, id)
        new_primary_id = f"mediafiles/{mediafile_id}"
        for edge in self.db.collection("hasMediafile").find({"_from": item_id}):
            if edge["_to"] != new_primary_id and field in edge and edge[field]:
                edge[field] = False
                self.db.update_document(edge)
            elif edge["_to"] == new_primary_id and field in edge and not edge[field]:
                edge[field] = True
                self.db.update_document(edge)

    def update_collection_item_relations(self, collection, id, content, parent=True):
        item_id = self.__get_id_for_collection_item(collection, id)
        for relation in self.entity_relations:
            for edge in self.db.aql.execute(
                f"FOR i IN {relation} FILTER i._from == '{item_id}' OR i._to == '{item_id}' RETURN i"
            ):
                self.db.delete_document(edge)
        return self.add_relations_to_collection_item(collection, id, content, parent)

    def update_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ):
        try:
            item = self.db.collection(collection).replace(
                {"_id": self.__get_id_for_collection_item(collection, id)} | content,
                return_new=True,
                check_rev=False,
            )["new"]
        except DocumentReplaceError as ex:
            if ex.error_code == 1210:
                raise NonUniqueException(ex.error_message)
            raise ex
        signal_child_relation_changed(app.rabbit, collection, item["_id"])
        return item

    def update_parent_relation_values(self, collection, parent_id):
        entity = self.get_item_from_collection_by_id(collection, parent_id)

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
            for edge in self.db.aql.execute(
                f'FOR i IN {edge_type} FILTER i._from == \'{entity["_id"]}\' OR i._to == \'{entity["_id"]}\' RETURN i'
            ):
                if edge["key"] == entity["_id"] and edge["value"] != new_value:
                    edge["value"] = new_value
                    self.db.update_document(edge)
                    changed_ids.add(entity["_key"])
        if len(changed_ids):
            signal_edge_changed(app.rabbit, changed_ids)

    def get_existing_collections(self):
        return self.collections
