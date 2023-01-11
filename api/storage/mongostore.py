import os
import uuid
import util

from pymongo import MongoClient
from storage.genericstore import GenericStorageManager


class MongoStorageManager(GenericStorageManager):
    character_replace_map = {".": "="}

    def __init__(self):
        mongo_host = os.getenv("MONGO_DB_HOST", "mongo")
        mongo_port = int(os.getenv("MONGO_DB_PORT", 27017))
        mongo_db = os.getenv("MONGO_DB_NAME", "dams")
        client = MongoClient(mongo_host, mongo_port)
        self.db = client[mongo_db]

    def __add_child_relations(self, collection, id, relations):
        for relation in relations:
            dst_relation = relation.copy()
            dst_relation["type"] = self.__map_entity_relation(relation["type"])
            dst_relation["key"] = id
            dst_id = relation["key"]
            dst_content = [dst_relation]
            self.add_sub_item_to_collection_item(
                collection, dst_id, "relations", dst_content
            )

    def __get_entities_by_type_query(self, item_type):
        return {"type": item_type}

    def __get_id_query(self, id):
        return {"$or": [{"_id": id}, {"identifiers": id}]}

    def __get_ids_query(self, ids):
        return {"$or": [{"_id": {"$in": ids}}, {"identifiers": {"$in": ids}}]}

    def __get_items_from_collection_by_ids(self, collection, ids):
        items = dict()
        documents = self.db[collection].find(self.__get_multiple_id_query(ids))
        items["count"] = self.db[collection].count_documents(
            self.__get_multiple_id_query(ids)
        )
        items["results"] = list()
        for document in documents:
            items["results"].append(self._prepare_mongo_document(document, True))
        return items

    def __get_multiple_id_query(self, ids):
        return {"$or": [{"_id": {"$in": ids}}, {"identifiers": {"$in": ids}}]}

    def __map_entity_relation(self, relation):
        return {
            "authoredBy": "authored",
            "isIn": "contains",
            "hasMediafile": "belongsTo",
            "authored": "authoredBy",
            "belongsTo": "hasMediafile",
            "contains": "isIn",
        }.get(relation)

    def __map_relation_to_collection(self, relation):
        return {
            "authoredBy": "entities",
            "isIn": "entities",
            "hasMediafile": "mediafiles",
            "authored": "entities",
            "belongsTo": "entities",
            "contains": "entities",
        }.get(relation, "entities")

    def _prepare_mongo_document(self, document, reversed, id=None):
        if id:
            document["_id"] = id
            if "identifiers" not in document:
                document["identifiers"] = [id]
            else:
                document["identifiers"].insert(0, id)
        if "data" in document:
            document["data"] = self.__replace_dictionary_keys(
                document["data"], reversed
            )
        return document

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
            if not util.mediafile_is_public(potential_mediafile):
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

    def add_mediafile_to_collection_item(
        self, collection, id, mediafile_id, mediafile_public
    ):
        primary_mediafile = mediafile_public
        primary_thumbnail = mediafile_public
        if mediafile_public:
            relations = self.get_collection_item_relations(collection, id)
            for relation in relations:
                if relation.get("is_primary", False):
                    primary_mediafile = False
                if relation.get("is_primary_thumbnail", False):
                    primary_thumbnail = False
        relations = [
            {
                "key": mediafile_id,
                "label": "hasMediafile",
                "type": "hasMediafile",
                "is_primary": primary_mediafile,
                "is_primary_thumbnail": primary_thumbnail,
            }
        ]
        self.add_relations_to_collection_item(
            collection, id, relations, True, "mediafiles"
        )
        return self.db["mediafiles"].find_one(self.__get_id_query(mediafile_id))

    def add_relations_to_collection_item(
        self, collection, id, relations, parent=True, dst_collection=None
    ):
        self.add_sub_item_to_collection_item(collection, id, "relations", relations)
        if not dst_collection:
            dst_collection = collection
        self.__add_child_relations(dst_collection, id, relations)
        return relations

    def add_sub_item_to_collection_item(self, collection, id, sub_item, content):
        result = self.db[collection].update_one(
            self.__get_id_query(id), {"$addToSet": {sub_item: {"$each": content}}}
        )
        return content if result.modified_count else None

    def check_health(self):
        return self.db.command("ping")

    def delete_collection_item_relations(self, collection, id, relations, parent=True):
        for relation in relations:
            impacted_ids = [id, relation["key"]]
            types = [relation["type"], self.__map_entity_relation(relation["type"])]
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

    def delete_item_from_collection(self, collection, id):
        self.delete_impacted_relations(collection, id)
        self.db[collection].delete_one(self.__get_id_query(id))

    def delete_impacted_relations(self, collection, id, sub_item="relations"):
        all_sub_items = self.get_collection_item_sub_item(collection, id, sub_item)
        for obj in all_sub_items:
            self.delete_collection_item_sub_item_key(
                self.__map_relation_to_collection(obj["type"]), obj["key"], sub_item, id
            )

    def drop_all_collections(self):
        self.db.entities.drop()
        self.db.jobs.drop()
        self.db.mediafiles.drop()

    def get_collection_item_mediafiles(self, collection, id):
        mediafiles = []
        for mediafile in self.db["mediafiles"].find({"relations.key": id}):
            mediafiles.append(mediafile)
        return mediafiles

    def get_collection_item_relations(
        self, collection, id, include_sub_relations=False, exclude=None
    ):
        return self.get_collection_item_sub_item(collection, id, "relations")

    def get_entities(self, skip=0, limit=20, skip_relations=0, filters=None):
        if "ids" in filters:
            return self.__get_items_from_collection_by_ids("entities", filters["ids"])
        return self.get_items_from_collection("entities", skip, limit, filters)

    def get_item_from_collection_by_id(self, collection, id):
        if document := self.db[collection].find_one(self.__get_id_query(id)):
            return self._prepare_mongo_document(document, True)
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
        if fields and "type" in fields:
            documents = self.db[collection].find(
                self.__get_entities_by_type_query(fields["type"]),
                skip=skip,
                limit=limit,
            )
            count = self.db[collection].count_documents(
                self.__get_entities_by_type_query(fields["type"])
            )
        else:
            documents = self.db[collection].find(skip=skip, limit=limit)
            count = self.db[collection].count_documents({})
        items["count"] = count
        items["results"] = list()
        for document in documents:
            items["results"].append(self._prepare_mongo_document(document, True))
        return items

    def get_mediafile_linked_entities(self, mediafile):
        linked_entities = []
        for relation in self.get_collection_item_relations(
            "mediafiles", mediafile["_id"]
        ):
            linked_entities.append(
                {
                    "entity_id": relation["key"],
                    "primary_mediafile": relation["is_primary"],
                    "primary_thumbnail": relation["is_primary_thumbnail"],
                }
            )
        return linked_entities

    def get_metadata_values_for_collection_item_by_key(self, collection, key):
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
            ]
        )
        for result in aggregation:
            for distinct_value in result["distinctValues"]:
                distinct_values.append(distinct_value)
        return distinct_values

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
                collection, item["key"], "relations", id
            )
        relations = self.get_collection_item_sub_item(collection, id, "relations")
        self.update_collection_item_sub_item(
            collection, id, "relations", relations + content
        )
        self.__add_child_relations(collection, id, content)
        return content

    def patch_item_from_collection(self, collection, id, content):
        content = self._prepare_mongo_document(content, False)
        self.db[collection].update_one(self.__get_id_query(id), {"$set": content})
        return self.get_item_from_collection_by_id(collection, id)

    def reindex_mediafile_parents(self, mediafile=None, parents=None):
        if mediafile:
            parents = self.get_mediafile_linked_entities(mediafile)
        for item in parents:
            entity = self.get_item_from_collection_by_id("entities", item["entity_id"])
            util.signal_entity_changed(entity)

    def save_item_to_collection(self, collection, content):
        content = self._prepare_mongo_document(content, False, str(uuid.uuid4()))
        item_id = self.db[collection].insert_one(content).inserted_id
        return self.get_item_from_collection_by_id(collection, item_id)

    def set_primary_field_collection_item(
        self, collection, entity_id, mediafile_id, field
    ):
        for id, destination_id in [
            (entity_id, mediafile_id),
            (mediafile_id, entity_id),
        ]:
            if id == mediafile_id:
                collection = "mediafiles"
            relations = self.get_collection_item_relations(collection, id)
            for relation in relations:
                if relation["key"] == destination_id:
                    relation[field] = True
                elif field in relation and relation[field]:
                    relation[field] = False
            self.patch_item_from_collection(collection, id, {"relations": relations})

    def update_collection_item_relations(self, collection, id, content, parent=True):
        for item in self.get_collection_item_sub_item(collection, id, "relations"):
            self.delete_collection_item_sub_item_key(
                collection, item["key"], "relations", id
            )
        self.update_collection_item_sub_item(collection, id, "relations", content)
        self.__add_child_relations(collection, id, content)
        return content

    def update_item_from_collection(self, collection, id, content):
        content = self._prepare_mongo_document(content, False)
        self.db[collection].replace_one(self.__get_id_query(id), content)
        return self.get_item_from_collection_by_id(collection, id)
