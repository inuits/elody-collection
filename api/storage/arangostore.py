from arango import (
    DocumentInsertError,
    DocumentReplaceError,
    DocumentUpdateError,
)
from arango.client import ArangoClient
from arango.http import DefaultHTTPClient
from configuration import get_object_configuration_mapper
from copy import deepcopy
from elody.exceptions import NonUniqueException
from elody.util import (
    custom_json_dumps,
    mediafile_is_public,
    signal_child_relation_changed,
    signal_edge_changed,
    signal_entity_changed,
)
from logging_elody.log import log
from os import getenv
from policy_factory import get_user_context
from rabbit import get_rabbit
from storage.genericstore import GenericStorageManager
from time import sleep


class ArangoStorageManager(GenericStorageManager):
    def __init__(self):
        self.http_client = DefaultHTTPClient(pool_maxsize=100)
        self.arango_db_name = getenv("ARANGO_DB_NAME")
        self.default_graph_name = getenv("DEFAULT_GRAPH", "assets")
        self.collections = [
            "abstracts",
            "box_visits",
            "entities",
            "history",
            "jobs",
            "key_value_store",
            "mediafiles",
            "users",
        ]
        self.entity_relations = [
            "box",
            "box_stories",
            "components",
            "contains",
            "definedBy",
            "defines",
            "frames",
            "hasAsset",
            "hasJob",
            "hasMediafile",
            "hasParentJob",
            "hasTenant",
            "hasTestimony",
            "hasUser",
            "isAssetFor",
            "isIn",
            "isJobOf",
            "isMediafileFor",
            "isParentJobOf",
            "isTenantFor",
            "isTestimonyFor",
            "isUserFor",
            "parent",
            "stories",
            "story_box",
            "story_box_visits",
            "visited",
        ]
        self.definitions = {
            "hasAsset": ("entities", "entities"),
            "hasJob": (["entities", "mediafiles"], "jobs"),
            "hasMediafile": ("entities", "mediafiles"),
            "hasParentJob": ("jobs", "jobs"),
            "hasTenant": ("users", "entities"),
            "hasUser": ("entities", "users"),
            "isAssetFor": ("entities", "entities"),
            "isJobOf": ("jobs", ["entities", "mediafiles"]),
            "isMediafileFor": ("mediafiles", "entities"),
            "isParentJobOf": ("jobs", "jobs"),
            "isTenantFor": ("entities", "users"),
            "isUserFor": ("users", "entities"),
            "stories": ("box_visits", "entities"),
            "story_box": ("box_visits", "entities"),
            "story_box_visits": ("entities", "box_visits"),
        }
        self.edges = self.entity_relations
        self.client = ArangoClient(
            hosts=getenv("ARANGO_DB_HOST"),
            serializer=custom_json_dumps,
            http_client=self.http_client,
        )
        self.sys_db = self.client.db(
            "_system",
            username=getenv("ARANGO_DB_USERNAME"),
            password=getenv("ARANGO_DB_PASSWORD"),
        )
        self.db = None
        self.id_cache = {}
        self.__create_database_if_not_exists()

    def __create_database_if_not_exists(self):
        if not self.sys_db.has_database(self.arango_db_name):
            self.sys_db.create_database(self.arango_db_name)
        self.db = self.client.db(
            self.arango_db_name,
            username=getenv("ARANGO_DB_USERNAME"),
            password=getenv("ARANGO_DB_PASSWORD"),
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
            fr, to = self.definitions.get(edge, ("entities", "entities"))
            graph.create_edge_definition(
                edge_collection=edge,
                from_vertex_collections=[fr] if not isinstance(fr, list) else fr,
                to_vertex_collections=[to] if not isinstance(to, list) else to,
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
        if identifier.find("/") != -1 and (item_id := self.id_cache.get(identifier)):
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
            "asset": [
                "components",
                "hasMediafile",
                "hasTenant",
                "hasTestimony",
                "isAssetFor",
                "isIn",
                "parent",
            ],
            "box": ["box_stories"],
            "box_visit": ["stories", "visited", "story_box"],
            "consists_of": ["parent", "components"],
            "frame": ["stories", "components"],
            "job": ["hasJob", "hasParentJob", "isJobOf", "isParentJobOf"],
            "mediafile": ["isMediafileFor"],
            "museum": ["defines"],
            "set": ["hasUser", "hasAsset", "isIn"],
            "story": ["frames", "box", "story_box_visits"],
            "tenant": ["isTenantFor", "definedBy"],
            "testimony": ["isTestimonyFor"],
            "thesaurus": [],
            "user": ["hasTenant"],
            "download": ["hasAsset", "hasMediafile"],
        }.get(type, ["components"])
        return [x for x in relations if not exclude or x not in exclude]

    def __invalidate_id_cache_for_item(self, item_id):
        self.id_cache = {
            key: value for key, value in self.id_cache.items() if value != item_id
        }

    def __map_entity_relation(self, relation):
        return {
            "belongsToParent": "hasChild",
            "box": "box_stories",
            "box_stories": "box",
            "components": "parent",
            "contains": "isIn",
            "definedBy": "defines",
            "frames": "stories",
            "hasAsset": "isAssetFor",
            "hasChild": "belongsToParent",
            "hasJob": "isJobOf",
            "hasMediafile": "isMediafileFor",
            "hasParentJob": "isParentJobOf",
            "hasTenant": "isTenantFor",
            "hasTestimony": "isTestimonyFor",
            "hasUser": "isUserFor",
            "isAssetFor": "hasAsset",
            "isIn": "contains",
            "isJobOf": "hasJob",
            "isMediafileFor": "hasMediafile",
            "isParentJobOf": "hasParentJob",
            "isTestimonyFor": "hasTestimony",
            "isUserFor": "hasUser",
            "parent": "components",
            "stories": "frames",
        }.get(relation)

    def __remove_edges(self, item_id, relation, edge_collection):
        for edge in self.db.aql.execute(
            f"FOR i IN {edge_collection} FILTER i._from == '{item_id}' OR i._to == '{item_id}' RETURN i"
        ):
            if (
                edge["_from"] == relation["key"]
                or edge["_to"] == relation["key"]
                or edge["_from"].split("/")[1] == relation["key"]
                or edge["_to"].split("/")[1] == relation["key"]
            ):
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
        self, collection, id, mediafile_id, mediafile_public, relation_properties=[]
    ):
        if not (item_id := self.__get_id_for_collection_item(collection, id)):
            return None
        if relation_properties and not isinstance(relation_properties, list):
            relation_properties = [relation_properties]
        data = {
            "_from": item_id,
            "_to": mediafile_id,
            "is_primary": mediafile_public,
            "is_primary_thumbnail": mediafile_public,
        }
        relation_properties_copy = deepcopy(relation_properties)
        for relation in relation_properties_copy:
            if is_downloadset := relation.get("is_downloadset"):
                data["is_downloadset"] = is_downloadset
                relation_properties.remove(relation)
        if mediafile_public:
            for edge in self.db.collection("hasMediafile").find({"_from": item_id}):
                if "is_primary" in edge and edge["is_primary"]:
                    data["is_primary"] = False
                if "is_primary_thumbnail" in edge and edge["is_primary_thumbnail"]:
                    data["is_primary_thumbnail"] = False
        self.db.graph(self.default_graph_name).edge_collection("hasMediafile").insert(
            data
        )
        self.db.graph(self.default_graph_name).edge_collection("isMediafileFor").insert(
            {
                "_from": data["_to"],
                "_to": data["_from"],
                "is_primary": data["is_primary"],
                "is_primary_thumbnail": data["is_primary_thumbnail"],
                "is_downloadset": data.get("is_downloadset"),
            }
        )
        self.add_relations_to_collection_item(
            collection,
            id,
            [
                relation
                for relation in relation_properties
                if relation["type"] != "isMediafileFor"
            ],
        )
        return self.db.document(mediafile_id)

    def add_mediafile_to_parent(self, parent_id, mediafile_id):
        if not (item_id := self.__get_id_for_collection_item("mediafiles", id)):
            return None
        data = {
            "_from": item_id,
            "_to": mediafile_id,
        }
        self.db.graph(self.default_graph_name).edge_collection("hasChild").insert(data)
        return self.db.document(mediafile_id)

    def add_relations_to_collection_item(
        self, collection, id, relations, parent=True, dst_collection=None
    ):
        aql = f"""
            FOR item IN {collection}
                FILTER '{id.split("/")[-1]}' IN item.identifiers
                OR item._key == '{id.split("/")[-1]}'
                RETURN item
        """
        try:
            item_id = next(self.db.aql.execute(aql))["_id"]
        except StopIteration:
            return None
        for relation in relations:
            data = {}
            if relation["key"].find("/") == -1:
                if definition := self.definitions.get(relation["type"]):
                    relation["key"] = definition[1] + "/" + relation["key"]
                else:
                    raise Exception(f"No definition for {relation['type']}")
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
        return self.strip_relations(relations)

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

    def get_collection_item_mediafiles(
        self, collection, id, skip=0, limit=0, asc=1, sort="order"
    ):
        item_id = self.__get_id_for_collection_item(collection, id)
        mediafiles = list()
        edges = list(self.db.collection("hasMediafile").find({"_from": item_id}))
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
        self,
        collection,
        id,
        include_sub_relations=False,
        exclude=None,
        entity={},
        strip_relations=True,
    ):
        if not exclude:
            exclude = []
        if not entity:
            entity = self.get_item_from_collection_by_id(collection, id)
        if collection == "mediafiles":
            entity["type"] = "mediafile"
        relevant_relations = self.__get_relevant_relations(entity.get("type"), exclude)
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
        return self.strip_relations(relations) if strip_relations else relations

    def get_entities(
        self,
        skip=0,
        limit=20,
        skip_relations=0,
        filters=None,
        order_by=None,
        ascending=True,
    ):
        fields = filters
        return self.get_items_from_collection(
            "entities", skip, limit, fields, None, order_by, ascending
        )

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
        aql = f"""
            FOR item IN {collection}
                FILTER '{id.split("/")[-1]}' IN item.identifiers
                OR item._key == '{id.split("/")[-1]}'
                RETURN item
        """
        try:
            item = next(self.db.aql.execute(aql))
        except StopIteration:
            return None
        if collection == "mediafiles":
            item["type"] = "mediafile"
        if item:
            relations = self.get_collection_item_relations(collection, id, entity=item)
            item["relations"] = relations
        if item["type"] == "asset":
            iiif_presentation = getenv("IMAGE_API_URL_EXT", "")
            if iiif_presentation.find("/iiif/image") > -1 and item.get("object_id"):
                iiif_presentation = iiif_presentation.replace(
                    "/iiif/image", f"/iiif/presentation/v2/manifest/{item['object_id']}"
                )
            if item.get("metadata"):
                item["metadata"].append(
                    {"key": "iiif_presentation", "value": iiif_presentation}
                )
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
        filter_conditions = []
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

        # QUICK FIX TO MATCH CERTAIN FILTER
        if "metadata" in filters:
            metadata_filters = filters["metadata"]
            for key, value in metadata_filters.items():
                if key == "$elemMatch":
                    filter_conditions.append(
                        f'FILTER LENGTH(FOR m IN c.metadata FILTER m.key == "{value["key"]}" AND m.value == "{value["value"]}" RETURN 1) > 0'
                    )

        filter_query = " AND ".join(filter_conditions) if filter_conditions else ""
        aql = f"""
            FOR c IN @@collection
                {"FILTER c._key IN @ids" if "ids" in filters else ""}
                {"FILTER c.user == @user_or_public OR NOT c.private" if "user_or_public" in filters else ""}
                {filter_query}
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
        item_results = []
        for item in items["results"]:
            relations = self.get_collection_item_relations(collection, id, entity=item)
            item["relations"] = relations
            item_results.append(item)
        items["results"] = item_results
        return items

    def get_mediafile_linked_entities(self, mediafile, linked_entities=[]):
        relations = self.get_collection_item_relations("mediafiles", mediafile["_id"])
        for relation in relations:
            if relation.get("type") == "_from":
                linked_entities.append(
                    {
                        "entity_id": relation["key"],
                        "primary_mediafile": relation.get("is_primary"),
                        "primary_thumbnail": relation.get("is_primary_thumbnail"),
                    }
                )
            if relation.get("type") == "belongsToParent":
                return self.get_mediafile_linked_entities(
                    self.get_item_from_collection_by_id(
                        "mediafiles", relation.get("key"), linked_entities
                    )
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
        signal_child_relation_changed(get_rabbit(), collection, item["_id"])
        return item

    def patch_item_from_collection_v2(
        self, collection, item, content, spec, *, run_post_crud_hook=True
    ):
        item = item.get("storage_format", item)
        config = get_object_configuration_mapper().get(item["type"])
        if not collection:
            collection = config.crud()["collection"]
        scope = config.crud().get("spec_scope", {}).get(spec, None)
        object_lists = config.document_info()["object_lists"]
        pre_crud_hook = config.crud()["pre_crud_hook"]
        post_crud_hook = config.crud()["post_crud_hook"]
        if not self._does_request_changes(item, content):
            return item
        for key, value in content.items():
            if value == "[protected content]":
                continue
            if not scope or key in scope:
                if key in object_lists:
                    if key != "relations":
                        for value_element in value:
                            for item_element in item[key]:
                                if (
                                    item_element[object_lists[key]]
                                    == value_element[object_lists[key]]
                                ):
                                    item[key].remove(item_element)
                                    break
                            else:
                                item_element = None
                            pre_crud_hook(
                                crud="update",
                                object_list_elements={
                                    "item_element": item_element,
                                    "value_element": value_element,
                                },
                            )
                    item[key].extend(value)
                else:
                    item[key] = value
        try:
            pre_crud_hook(
                crud="update", document=item, get_user_context=get_user_context
            )
            item = self.db.collection(collection).update(
                item, return_new=True, check_rev=False
            )["new"]
            if run_post_crud_hook:
                post_crud_hook(
                    crud="update",
                    document=item,
                    storage=self,
                    get_user_context=get_user_context,
                    get_rabbit=get_rabbit,
                )
            signal_child_relation_changed(get_rabbit(), collection, item["_id"])
        except DocumentInsertError as error:
            log.exception(f"{error.__class__.__name__}: {error}", item, exc_info=error)
            if error.error_code == 1210:
                raise NonUniqueException(error.error_message)
            raise error
        log.info("Successfully patched item", item)
        return item

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        if mediafile:
            parents = self.get_mediafile_linked_entities(mediafile)
        for item in parents:
            entity = self.db.document(item["entity_id"])
            signal_entity_changed(get_rabbit(), entity)

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

    def save_item_to_collection_v2(
        self, collection, items, *, is_history=False, run_post_crud_hook=True
    ):
        if not isinstance(items, list):
            items = items.get("storage_format", items)
            items = [items]
        item = {}
        try:
            for item in items:
                item["_key"] = item.pop("_id")
                if not is_history:
                    # self.__verify_uniqueness(item)
                    pass
                config = get_object_configuration_mapper().get(item["type"])
                item_relations = item.pop("relations", [])
                self.db.insert_document(
                    config.crud()[
                        "collection" if not is_history else "collection_history"
                    ],
                    item,
                )
                if item_relations:
                    sleep(1)
                    self.add_relations_to_collection_item(
                        collection, item["_key"], item_relations
                    )
                if not is_history and run_post_crud_hook:
                    post_crud_hook = config.crud()["post_crud_hook"]
                    post_crud_hook(
                        crud="create",
                        document=item,
                        storage=self,
                        get_user_context=get_user_context,
                        get_rabbit=get_rabbit,
                    )
                log.info("Successfully saved item", item)
        except DocumentInsertError as error:
            log.exception(f"{error.__class__.__name__}: {error}", item, exc_info=error)
            if error.error_code == 1210:
                raise NonUniqueException(error.error_message)
            raise error
        return self.get_item_from_collection_by_id(collection, items[0]["_key"])

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

    def strip_relations(self, relations):
        for relation in relations:
            if relation["key"].find("/") > 0:
                relation["key"] = relation["key"].split("/")[1]
        return relations

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
        signal_child_relation_changed(get_rabbit(), collection, item["_id"])
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
            signal_edge_changed(get_rabbit(), changed_ids)

    def get_existing_collections(self):
        return self.collections
