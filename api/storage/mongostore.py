import re

from bson.codec_options import CodecOptions
from configuration import get_object_configuration_mapper
from datetime import datetime, timezone
from elody.error_codes import ErrorCode, get_error_code, get_write
from elody.exceptions import NonUniqueException
from elody.util import flatten_dict, mediafile_is_public, signal_entity_changed
from logging_elody.log import log
from migration.migrate import migrate
from os import getenv
from policy_factory import get_user_context
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError
from rabbit import get_rabbit
from serialization.serialize import serialize
from storage.genericstore import GenericStorageManager
from urllib.parse import quote_plus
from werkzeug.exceptions import BadRequest


class MongoStorageManager(GenericStorageManager):
    character_replace_map = {".": "="}

    def __init__(self):
        if getenv("DB_ENGINE", "mongo") != "mongo":
            return
        self.mongo_direct = int(getenv("MONGODB_DIRECT", 0))
        self.mongo_db_name = getenv("MONGODB_DB_NAME", "dams")
        self.mongo_hosts = getenv("MONGODB_HOSTS", "mongo").split(",")
        self.mongo_port = int(getenv("MONGODB_PORT", 27017))
        self.mongo_replica_set = getenv("MONGODB_REPLICA_SET")
        self.mongo_username = getenv("MONGODB_USERNAME")
        self.mongo_password = getenv("MONGODB_PASSWORD")
        self.allow_disk_use = getenv("MONGODB_ALLOW_DISK_USE", False) in [
            "True",
            "true",
            True,
        ]
        self.client = MongoClient(
            self.__create_mongo_connection_string(),
            directConnection=bool(self.mongo_direct),
        )
        self.db = self.client[self.mongo_db_name].with_options(
            CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        )
        self.db.entities.create_index("identifiers", unique=True)
        self.db.entities.create_index("object_id", unique=True, sparse=True)

    def __add_child_relations(self, id, relations, collection=None):
        for relation in relations:
            collection_for_this_iteration = (
                collection
                if collection
                else self._map_relation_to_collection(relation["type"])
            )
            dst_relation = relation.copy()
            dst_relation["type"] = self._map_entity_relation(relation["type"])
            dst_relation["key"] = id
            dst_id = relation["key"]
            dst_content = [dst_relation]
            self.add_sub_item_to_collection_item(
                collection_for_this_iteration, dst_id, "relations", dst_content
            )

    def __create_sortable_metadata(self, metadata):
        sort = dict()
        for metadata_object in metadata:
            sort_key = metadata_object["key"]
            if sort_key not in sort:
                sort[sort_key] = list()
            sort[sort_key].append(
                {key: value for key, value in metadata_object.items() if key != "key"}
            )
        return sort

    def __create_mongo_connection_string(self):
        connection_string = "mongodb://"
        if self.mongo_username and self.mongo_password:
            connection_string += (
                f"{quote_plus(self.mongo_username)}:{quote_plus(self.mongo_password)}@"
            )
        for i in range(len(self.mongo_hosts)):
            if self.mongo_port:
                connection_string += f"{self.mongo_hosts[i]}:{self.mongo_port}"
            else:
                connection_string += self.mongo_hosts[i]
            if i < len(self.mongo_hosts) - 1:
                connection_string += ","
        if self.mongo_username and self.mongo_password:
            connection_string += f"/?authSource={self.mongo_db_name}"
            if self.mongo_replica_set:
                connection_string += f"&replicaSet={self.mongo_replica_set}"
        return connection_string

    def _delete_impacted_relations(self, collection, id):
        relations = self.get_collection_item_sub_item(collection, id, "relations")
        relations = relations if relations else []
        for obj in relations:
            self.delete_collection_item_sub_item_key(
                self._map_relation_to_collection(obj["type"]),
                obj["key"],
                "relations",
                id,
            )

    def __get_filter_fields(self, fields):
        filter_fields = {}
        if fields is None:
            return {}
        for name, value in fields.items():
            if value is None:
                filter_fields[name] = "null"
            else:
                filter_fields[name] = value
        return filter_fields

    def __get_id_query(self, id):
        return {"$or": [{"_id": id}, {"identifiers": id}]}

    def __get_metatdata_query(self, key, value):
        return {
            "metadata": {
                "$elemMatch": {"key": key, "value": {"$regex": value, "$options": "i"}}
            }
        }

    def __get_ids_query(self, ids):
        return {"$or": [{"_id": {"$in": ids}}, {"identifiers": {"$in": ids}}]}

    def __get_items_from_collection_by_ids(self, collection, ids, sort=None, asc=True):
        items = dict()
        documents = self.db[collection].find(self.__get_ids_query(ids))
        if sort:
            documents.sort(
                self.get_sort_field(sort),
                ASCENDING if asc else DESCENDING,
            )
        items["count"] = self.db[collection].count_documents(self.__get_ids_query(ids))
        items["results"] = list()
        for document in documents:
            items["results"].append(
                self._prepare_mongo_document(document, True, collection)
            )
        return items

    def __replace_dictionary_keys(self, data, reversed):
        if type(data) is dict:
            new_dict = dict()
            for key, value in data.items():
                if type(value) is list:
                    new_value = list()
                    for object in value:
                        new_value.append(
                            self.__replace_dictionary_keys(object, reversed)
                        )
                else:
                    new_value = self.__replace_dictionary_keys(value, reversed)
                for original_char, replace_char in self.character_replace_map.items():
                    if reversed:
                        new_dict[key.replace(replace_char, original_char)] = new_value
                    else:
                        new_dict[key.replace(original_char, replace_char)] = new_value
            return new_dict
        return data

    def __set_new_primary(self, raw_entity, mediafile=False, thumbnail=False):
        entity_id = raw_entity["_id"]
        relations = self.get_collection_item_relations("entities", entity_id)
        for relation in relations:
            if "is_primary" not in relation and "is_primary_thumbnail" not in relation:
                continue
            potential_mediafile = self.get_item_from_collection_by_id(
                "mediafiles", relation["key"]
            )
            if not mediafile_is_public(potential_mediafile):
                continue
            if mediafile:
                self.set_primary_field_collection_item(
                    "entities", entity_id, potential_mediafile["_id"], "is_primary"
                )
            if thumbnail:
                self.set_primary_field_collection_item(
                    "entities",
                    entity_id,
                    potential_mediafile["_id"],
                    "is_primary_thumbnail",
                )
            break

    def __verify_uniqueness(self, item):
        try:
            resolve_collections = get_user_context().bag.get("collection_resolver")
        except:
            resolve_collections = None
        if not resolve_collections:
            return

        collections = resolve_collections()
        for collection in collections:
            documents = list(
                self.db[collection].find({"identifiers": {"$in": item["identifiers"]}})
            )
            if documents:
                duplicate_keys = set(documents[0]["identifiers"]) & set(
                    item["identifiers"]
                )
                raise BadRequest(
                    f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} Entity with following identifiers already exists: {', '.join(list(duplicate_keys))}"
                )

    def _map_entity_relation(self, relation):
        relations = {
            "authored": "authoredBy",
            "authoredBy": "authored",
            "belongsTo": "hasMediafile",
            "BelongsToParent": "hasChild",
            "components": "parent",
            "contains": "isIn",
            "definedBy": "defines",
            "defines": "definedBy",
            "hasChild": "belongsToParent",
            "hasMediafile": "belongsTo",
        }
        if mapped_relation := relations.get(relation):
            return mapped_relation
        match_is_for = re.match(r"^is(.*)For$", relation)
        match_has = re.match(r"^has(.*)$", relation)
        if match_is_for:
            entity_type = match_is_for.group(1)
            return f"has{entity_type}"
        if match_has:
            entity_type = match_has.group(1)
            return f"is{entity_type}For"

    def _map_relation_to_collection(self, relation):
        return {
            "authored": "entities",
            "authoredBy": "entities",
            "belongsTo": "entities",
            "belongsToParent": "mediafiles",
            "components": "entities",
            "contains": "entities",
            "hasMediafile": "mediafiles",
            "hasChild": "mediafiles",
            "isIn": "entities",
            "parent": "entities",
        }.get(relation, "entities")

    def _prepare_mongo_document(
        self, document, reversed, collection, create_sortable_metadata=True
    ):
        if "data" in document:
            document["data"] = self.__replace_dictionary_keys(
                document["data"], reversed
            )
        if "metadata" not in document:
            return document
        if collection == "mediafiles":
            document["type"] = "mediafile"
        if not reversed and create_sortable_metadata:
            document["sort"] = self.__create_sortable_metadata(document["metadata"])
        return serialize(
            migrate(document), type=document.get("type"), to_format="elody"
        )

    def add_mediafile_to_collection_item(
        self, collection, id, mediafile_id, mediafile_public, relation_properties=None
    ):
        if not relation_properties:
            relation_properties = dict()
        primary_mediafile = mediafile_public
        primary_thumbnail = mediafile_public
        if mediafile_public:
            relations = self.get_collection_item_relations(collection, id)
            for relation in relations:
                if relation.get("is_primary", False):
                    primary_mediafile = False
                if relation.get("is_primary_thumbnail", False):
                    primary_thumbnail = False
        count = self.get_collection_item_mediafiles_count(id)
        self.add_relations_to_collection_item(
            collection,
            id,
            [
                {
                    **{
                        "key": mediafile_id,
                        "label": "hasMediafile",
                        "type": "hasMediafile",
                        "is_primary": primary_mediafile,
                        "is_primary_thumbnail": primary_thumbnail,
                        "metadata": [
                            {
                                "key": "order",
                                "value": count + 1,
                            }
                        ],
                        "sort": {
                            "order": [
                                {
                                    "value": count + 1,
                                }
                            ]
                        },
                    },
                    **relation_properties,
                }
            ],
            True,
            "mediafiles",
        )
        return self.db["mediafiles"].find_one(self.__get_id_query(mediafile_id))

    def add_mediafile_to_parent(
        self,
        parent_id,
        item_id,
    ):
        count = self.count_relation_items("mediafiles", parent_id)
        self.add_relations_to_collection_item(
            "mediafiles",
            parent_id,
            [
                {
                    "key": item_id,
                    "label": "hasChild",
                    "type": "hasChild",
                    "metadata": [
                        {
                            "key": "order",
                            "value": count + 1,
                        }
                    ],
                    "sort": {
                        "order": [
                            {
                                "value": count + 1,
                            }
                        ]
                    },
                }
            ],
            True,
            "mediafiles",
        )
        return self.get_item_from_collection_by_id("mediafiles", item_id)

    def add_relations_to_collection_item(
        self, collection, id, relations, parent=True, dst_collection=None
    ):
        self.add_sub_item_to_collection_item(collection, id, "relations", relations)
        self.__add_child_relations(id, relations, dst_collection)
        return relations

    def add_sub_item_to_collection_item(self, collection, id, sub_item, content):
        result = self.db[collection].update_one(
            self.__get_id_query(id), {"$addToSet": {sub_item: {"$each": content}}}
        )
        return content if result.modified_count else None

    def check_health(self):
        self.db.command("ping")
        return True

    def delete_collection_item_relations(self, collection, id, relations, parent=True):
        for relation in relations:
            impacted_ids = [id, relation["key"]]
            types = [relation["type"], self._map_entity_relation(relation["type"])]
            self.db[collection].update_many(
                self.__get_ids_query(impacted_ids),
                {
                    "$pull": {
                        "relations": {
                            "key": {"$in": impacted_ids},
                            "type": {"$in": types},
                        }
                    }
                },
            )
            if parent:
                self.delete_collection_item_sub_item_key(
                    self._map_relation_to_collection(relation["type"]),
                    relation["key"],
                    "relations",
                    id,
                )

    def delete_item(self, item):
        item = item.get("storage_format", item)
        config = get_object_configuration_mapper().get(item["type"])
        pre_crud_hook = config.crud()["pre_crud_hook"]
        post_crud_hook = config.crud()["post_crud_hook"]
        timestamp = datetime.now(timezone.utc)
        try:
            pre_crud_hook(
                crud="delete",
                timestamp=timestamp,
                document=item,
                get_user_context=get_user_context,
            )
            self.db[config.crud()["collection"]].delete_one(
                self.__get_id_query(item["_id"])
            )
            post_crud_hook(
                crud="delete",
                document=item,
                storage=self,
                get_user_context=get_user_context,
                get_rabbit=get_rabbit,
            )
            log.info("Successfully deleted item", item)
        except Exception as error:
            log.exception(f"{error.__class__.__name__}: {error}", item, exc_info=error)
            raise error

    def delete_item_from_collection(self, collection, id):
        self._delete_impacted_relations(collection, id)
        self.db[collection].delete_one(self.__get_id_query(id))

    def delete_data_from_collection_item(self, collection, item, content, spec):
        item = item.get("storage_format", item)
        config = get_object_configuration_mapper().get(item["type"])
        scope = config.crud().get("spec_scope", {}).get(spec, None)
        object_lists = config.document_info()["object_lists"]
        pre_crud_hook = config.crud()["pre_crud_hook"]
        post_crud_hook = config.crud()["post_crud_hook"]
        timestamp = datetime.now(timezone.utc)
        for key, value in content.items():
            if not scope or key in scope:
                if key in object_lists:
                    for value_element in value:
                        for item_element in item[key]:
                            if (
                                item_element[object_lists[key]]
                                == value_element[object_lists[key]]
                            ):
                                item[key].remove(item_element)
                else:
                    del item[key]
        try:
            pre_crud_hook(
                crud="update",
                timestamp=timestamp,
                document=item,
                get_user_context=get_user_context,
            )
            self.db[collection].replace_one(self.__get_id_query(item["_id"]), item)
            post_crud_hook(
                crud="update",
                document=item,
                storage=self,
                get_user_context=get_user_context,
                get_rabbit=get_rabbit,
            )
            log.info("Successfully deleted data from item", item)
        except DuplicateKeyError as error:
            log.exception(f"{error.__class__.__name__}: {error}", item, exc_info=error)
            if error.code == 11000:
                raise NonUniqueException(f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} {error.details}")
            raise error
        return self._prepare_mongo_document(item, False, collection, False)

    def drop_all_collections(self):
        self.db.entities.drop()
        self.db.jobs.drop()
        self.db.mediafiles.drop()

    def get_collection_item_mediafiles(
        self, collection, id, skip=0, limit=0, asc=1, sort="order"
    ):
        item = self.get_item_from_collection_by_id(collection, id)
        mediafiles = []
        documents = self.db["mediafiles"].find(
            {"relations.key": id}, skip=skip, limit=limit
        )
        documents.sort(
            self.get_sort_field(sort, True),
            ASCENDING if asc else DESCENDING,
        )
        for document in documents:
            mediafiles.append(self._prepare_mongo_document(document, True, collection))
        if sort == "order":
            mediafiles_sort = []
            for relation in item.get("relations", []):
                if relation.get("type") == "hasMediafile":
                    for metadata in relation.get("metadata", []):
                        sort_order = ""
                        if metadata["key"] == "order":
                            sort_order = metadata["value"]
                        if isinstance(sort_order, (int, float)):
                            data = {"id": relation.get("key"), "sort_order": sort_order}
                            mediafiles_sort.append(data)
            sort_dict = {item["id"]: item["sort_order"] for item in mediafiles_sort}
            # Sort the results using the sort dictionary
            sorted_results = sorted(
                mediafiles,
                key=lambda x: sort_dict.get(x["_id"], len(mediafiles) + 1),
                reverse=asc,
            )
            return sorted_results
        return mediafiles

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude=None
    ):
        relations = self.get_collection_item_sub_item(collection, id, "relations")
        return relations if relations else []

    def get_entities(
        self,
        skip=0,
        limit=20,
        skip_relations=0,
        filters=None,
        order_by=None,
        ascending=True,
    ):
        return self.get_items_from_collection(
            "entities",
            skip=skip,
            limit=limit,
            fields=None,
            filters=filters,
            sort=order_by,
            asc=ascending,
        )

    def get_history_for_item(self, collection, id, timestamp=None, all_entries=None):
        query = {
            "$and": [
                {"collection": collection},
                {"$or": [{"object._id": id}, {"object.identifiers": id}]},
            ]
        }
        if timestamp:
            results = self.db.history.aggregate(
                [
                    {
                        "$match": {
                            "$and": [
                                {"collection": collection},
                                {
                                    "$or": [
                                        {"object._id": id},
                                        {"object.identifiers": id},
                                    ]
                                },
                            ]
                        }
                    },
                    {
                        "$project": {
                            "_id": 1,
                            "identifiers": 1,
                            "object": 1,
                            "relations": 1,
                            "timestamp": 1,
                            "difference": {
                                "$abs": {
                                    "$subtract": [
                                        datetime.fromisoformat(timestamp),
                                        {
                                            "$dateFromString": {
                                                "dateString": "$timestamp"
                                            }
                                        },
                                    ]
                                }
                            },
                        }
                    },
                    {"$sort": {"difference": 1}},
                    {"$limit": 1},
                ],
                allowDiskUse=self.allow_disk_use,
            )
            result = list(results)[0]
            del result["difference"]
            return result
        elif all_entries:
            return list(self.db["history"].find(query, sort=[("timestamp", -1)]))
        else:
            return self.db["history"].find_one(query, sort=[("timestamp", -1)])

    def get_mediafile_linked_entities(self, mediafile, linked_entities=[]):
        relations = self.get_collection_item_relations("mediafiles", mediafile["_id"])
        for relation in relations:
            if relation.get("type") == "belongsTo":
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
                        "mediafiles", relation.get("key")
                    ),
                    linked_entities,
                )
        return linked_entities

    def get_item_from_collection_by_id(self, collection, id):
        if document := self.db[collection].find_one(self.__get_id_query(id)):
            return self._prepare_mongo_document(document, True, collection)
        return None

    def get_item_from_collection_by_metadata(self, collection, key, value):
        if document := self.db[collection].find_one(
            self.__get_metatdata_query(key, value)
        ):
            return self._prepare_mongo_document(document, True, collection)
        return None

    def count_items_from_collection(self, collection, fields=None, filters=[]):
        if fields or filters:
            query = {
                "$and": [
                    self.__get_filter_fields(filters),
                    self.__get_filter_fields(fields),
                ]
            }
            return self.db[collection].count_documents(query)
        else:
            return self.db[collection].count_documents({})

    def get_items_from_collection(
        self,
        collection,
        skip=0,
        limit=20,
        fields=None,
        filters=[],
        sort=None,
        asc=True,
    ):
        if "ids" in filters:
            return self.__get_items_from_collection_by_ids(
                collection, filters["ids"], sort, asc
            )
        items = dict()
        if fields or filters:
            query = {
                "$and": [
                    self.__get_filter_fields(filters),
                    self.__get_filter_fields(fields),
                ]
            }
            documents = self.db[collection].find(
                query,
                skip=skip,
                limit=limit,
            )
            count = self.db[collection].count_documents(query)
        else:
            documents = self.db[collection].find(skip=skip, limit=limit)
            count = self.db[collection].count_documents({})
        if sort:
            documents.sort(
                self.get_sort_field(sort),
                ASCENDING if asc else DESCENDING,
            )
        items["count"] = count
        items["results"] = list()
        for document in documents:
            items["results"].append(
                self._prepare_mongo_document(document, True, collection)
            )
        return items

    def get_metadata_values_for_collection_item_by_key(self, collection, key):
        if key in ["type"]:
            return self.db[collection].distinct(key)
        distinct_values = list()
        aggregation = self.db[collection].aggregate(
            [
                {"$match": {"metadata.key": key}},
                {"$unwind": "$metadata"},
                {"$match": {"metadata.key": key}},
                {
                    "$group": {
                        "_id": None,
                        "distinctValues": {"$addToSet": "$metadata.value"},
                    }
                },
            ],
            allowDiskUse=self.allow_disk_use,
        )
        for result in aggregation:
            for distinct_value in result["distinctValues"]:
                distinct_values.append(distinct_value)
        return distinct_values

    def get_sort_field(self, field, relation_sort=False):
        if field not in [
            "_id",
            "date_created",
            "date_updated",
            "last_editor",
            "mimetype",
            "object_id",
            "type",
            "version",
            "filename",
            "original_filename",
        ]:
            # raise Exception(f"{'relations.hasMediafile.' if relation_sort else ''}metadata.{field}.value")
            return f"{'relations.' if relation_sort else ''}metadata.{field}.value"
        return field

    def handle_mediafile_deleted(self, parents):
        for item in parents:
            if item["primary_mediafile"] or item["primary_thumbnail"]:
                entity = self.get_item_from_collection_by_id(
                    "entities", item["entity_id"]
                )
                self.__set_new_primary(
                    entity, item["primary_mediafile"], item["primary_thumbnail"]
                )

    def handle_mediafile_status_change(self, mediafile):
        relations = self.get_collection_item_relations("mediafiles", mediafile["_id"])
        for relation in [x for x in relations if x["type"] == "belongsTo"]:
            primary_mediafile = relation.get("is_primary", False)
            primary_thumbnail = relation.get("is_primary_thumbnail", False)
            if primary_mediafile or primary_thumbnail:
                entity = self.get_item_from_collection_by_id(
                    "entities", relation["key"]
                )
                self.__set_new_primary(entity, primary_mediafile, primary_thumbnail)

    def patch_collection_item_relations(self, collection, id, content, parent=True):
        for item in content:
            self.delete_collection_item_sub_item_key(
                collection, id, "relations", item["key"]
            )
            self.delete_collection_item_sub_item_key(
                self._map_relation_to_collection(item["type"]),
                item["key"],
                "relations",
                id,
            )
        relations = self.get_collection_item_sub_item(collection, id, "relations")
        relations = [*relations, *content] if relations else content
        for relation in relations:
            if relation.get("metadata") is not None:
                relation["sort"] = self.__create_sortable_metadata(
                    [
                        item
                        for item in relation["metadata"]
                        if item.get("key") == "order"
                    ]
                )
        self.update_collection_item_sub_item(collection, id, "relations", relations)
        self.__add_child_relations(id, content)
        return content

    def patch_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ):
        content = self._prepare_mongo_document(
            content,
            False,
            collection,
            create_sortable_metadata=create_sortable_metadata,
        )
        try:
            self.db[collection].update_one(self.__get_id_query(id), {"$set": content})
        except DuplicateKeyError as ex:
            if ex.code == 11000:
                raise NonUniqueException(f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} {ex.details}")
            raise ex
        return self.get_item_from_collection_by_id(collection, id)

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
        timestamp = datetime.now(timezone.utc)
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
                                timestamp=timestamp,
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
                crud="update",
                timestamp=timestamp,
                document=item,
                get_user_context=get_user_context,
            )
            self.db[collection].replace_one(self.__get_id_query(item["_id"]), item)
            if run_post_crud_hook:
                post_crud_hook(
                    crud="update",
                    document=item,
                    storage=self,
                    get_user_context=get_user_context,
                    get_rabbit=get_rabbit,
                )
        except DuplicateKeyError as error:
            log.exception(f"{error.__class__.__name__}: {error}", item, exc_info=error)
            if error.code == 11000:
                raise NonUniqueException(f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} {error.details}")
            raise error
        log.info("Successfully patched item", item)
        return self._prepare_mongo_document(item, False, collection, False)

    def put_item_from_collection(self, collection, item, content, spec):
        config = get_object_configuration_mapper().get(item["type"])
        if not collection:
            collection = config.crud()["collection"]
        scope = config.crud().get("spec_scope", {}).get(spec, None)
        object_lists = config.document_info()["object_lists"]
        pre_crud_hook = config.crud()["pre_crud_hook"]
        post_crud_hook = config.crud()["post_crud_hook"]
        if not self._does_request_changes(item, content):
            return item
        timestamp = datetime.now(timezone.utc)
        if scope:
            for key, value in content.items():
                if key in scope:
                    if value == "[protected content]":
                        continue
                    if key in object_lists:
                        for value_element in value[object_lists[key]]:
                            pre_crud_hook(
                                crud="update",
                                timestamp=timestamp,
                                object_list_elements={"value_element": value_element},
                            )
                    item[key] = value
        else:
            item = content
        try:
            pre_crud_hook(
                crud="update",
                timestamp=timestamp,
                document=item,
                get_user_context=get_user_context,
            )
            self.db[collection].replace_one(self.__get_id_query(item["_id"]), item)
            post_crud_hook(
                crud="update",
                document=item,
                storage=self,
                get_user_context=get_user_context,
                get_rabbit=get_rabbit,
            )
        except DuplicateKeyError as error:
            log.exception(f"{error.__class__.__name__}: {error}", item, exc_info=error)
            if error.code == 11000:
                raise NonUniqueException(f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} {error.details}")
            raise error
        if scope:
            return self._prepare_mongo_document(item, False, collection, False)
        log.info("Successfully put item", item)
        return self.get_item_from_collection_by_id(collection, item["_id"])

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        if mediafile:
            parents = self.get_mediafile_linked_entities(mediafile)
        for item in parents:
            entity = self.get_item_from_collection_by_id("entities", item["entity_id"])
            signal_entity_changed(get_rabbit(), entity)

    def save_item_to_collection(
        self,
        collection,
        content,
        only_return_id=False,
        create_sortable_metadata=True,
    ):
        if not content.get("_id"):
            content["_id"] = self._get_autogenerated_id_for_item(content)
        content["identifiers"] = self._get_autogenerated_identifiers_for_item(content)
        content = self._prepare_mongo_document(
            content, False, collection, create_sortable_metadata
        )
        try:
            item_id = self.db[collection].insert_one(content).inserted_id
        except DuplicateKeyError as ex:
            if ex.code == 11000:
                raise NonUniqueException(f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} {ex.details}")
            raise ex
        return (
            item_id
            if only_return_id
            else self.get_item_from_collection_by_id(collection, item_id)
        )

    def save_item_to_collection_v2(
        self, collection, items, *, is_history=False, run_post_crud_hook=True
    ):
        if not isinstance(items, list):
            items = items.get("storage_format", items)
            items = [items]
        item = {}
        try:
            for item in items:
                if not is_history:
                    self.__verify_uniqueness(item)
                config = get_object_configuration_mapper().get(item["type"])
                self.db[
                    config.crud()[
                        "collection" if not is_history else "collection_history"
                    ]
                ].insert_one(item)
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
        except DuplicateKeyError as error:
            log.exception(f"{error.__class__.__name__}: {error}", item, exc_info=error)
            if error.code == 11000:
                raise NonUniqueException(f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} {error.details}")
            raise error
        return self.get_item_from_collection_by_id(collection, items[0]["_id"])

    def set_primary_field_collection_item(self, collection, id, mediafile_id, field):
        for src_id, dst_id in [
            (id, mediafile_id),
            (mediafile_id, id),
        ]:
            if src_id == mediafile_id:
                collection = "mediafiles"
            relations = self.get_collection_item_relations(collection, src_id)
            for relation in relations:
                if relation["key"] == dst_id:
                    relation[field] = True
                elif field in relation and relation[field]:
                    relation[field] = False
                    self.set_primary_field_other_relation(
                        self._map_relation_to_collection(relation["type"]),
                        relation["key"],
                        id,
                        field,
                        False,
                    )
            self.patch_item_from_collection(
                collection, src_id, {"relations": relations}
            )

    def set_primary_field_other_relation(
        self, collection, id, updated_relation_id, field, value
    ):
        relations = self.get_collection_item_relations(collection, id)
        for relation in relations:
            if relation["key"] == updated_relation_id:
                relation[field] = value
                break
        self.patch_item_from_collection(collection, id, {"relations": relations})

    def update_collection_item_relations(self, collection, id, content, parent=True):
        relations = self.get_collection_item_sub_item(collection, id, "relations")
        relations = relations if relations else []
        for item in relations:
            self.delete_collection_item_sub_item_key(
                collection, item["key"], "relations", id
            )
        self.update_collection_item_sub_item(collection, id, "relations", content)
        self.__add_child_relations(id, content)
        return content

    def update_item_from_collection(
        self, collection, id, content, create_sortable_metadata=True
    ):
        content = self._prepare_mongo_document(
            content,
            False,
            collection,
            create_sortable_metadata=create_sortable_metadata,
        )
        try:
            self.db[collection].replace_one(self.__get_id_query(id), content)
        except DuplicateKeyError as ex:
            if ex.code == 11000:
                raise NonUniqueException(f"{get_error_code(ErrorCode.DUPLICATE_ENTRY, get_write())} {ex.details}")
            raise ex
        return self.get_item_from_collection_by_id(collection, id)

    def get_collection_item_mediafiles_count(self, id):
        return self.db["mediafiles"].count_documents({"relations.key": id})

    def get_existing_collections(self):
        return self.db.list_collection_names()
