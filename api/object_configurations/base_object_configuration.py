from abc import ABC, abstractmethod
from migration.base_object_migrator import BaseObjectMigrator


class BaseObjectConfiguration(ABC):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    @abstractmethod
    def crud(self):
        return {
            "collection": "entities",
            "collection_history": "history",
            "creator": lambda post_body, **kwargs: post_body,  # pyright: ignore
            "nested_matcher_builder": lambda object_lists, keys_info, value: self.__build_nested_matcher(
                object_lists, keys_info, value
            ),
            "post_crud_hook": lambda **kwargs: None,  # pyright: ignore
            "pre_crud_hook": lambda **kwargs: None,  # pyright: ignore
        }

    @abstractmethod
    def document_info(self):
        return {"object_lists": {"metadata": "key", "relations": "type"}}

    @abstractmethod
    def logging(self, flat_item, **kwargs):
        return {"info_labels": {}, "loki_indexed_info_labels": {}}

    @abstractmethod
    def migration(self):
        return BaseObjectMigrator(status="disabled")

    @abstractmethod
    def serialization(self, from_format, to_format):
        def serializer(item, **_):
            return item

        return serializer

    @abstractmethod
    def validation(self):
        def validator(http_method, content, **_):  # pyright: ignore
            pass

        return "function", validator

    def __build_nested_matcher(self, object_lists, keys_info, value, index=0):
        if index == 0 and not any(info["is_object_list"] for info in keys_info):
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                value = {"$exists": value == "ANY_MATCH"}
            return {".".join(info["key"] for info in keys_info): value}

        info = keys_info[index]

        if info["is_object_list"]:
            nested_matcher = self.__build_nested_matcher(
                object_lists, keys_info, value, index + 1
            )
            elem_match = {
                "$elemMatch": {
                    object_lists[info["key"]]: info["object_key"],
                    keys_info[index + 1]["key"]: nested_matcher,
                }
            }
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                del elem_match["$elemMatch"][keys_info[index + 1]["key"]]
                if value == "NONE_MATCH":
                    return {"NOR_MATCHER": {info["key"]: {"$all": [elem_match]}}}
            return elem_match if index > 0 else {info["key"]: {"$all": [elem_match]}}

        return value
