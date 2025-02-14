from copy import deepcopy
from elody.object_configurations.base_object_configuration import (
    BaseObjectConfiguration,
)


class NoneConfiguration(BaseObjectConfiguration):
    SCHEMA_TYPE = BaseObjectConfiguration.SCHEMA_TYPE
    SCHEMA_VERSION = BaseObjectConfiguration.SCHEMA_VERSION

    def crud(self):
        crud = {
            "nested_matcher_builder": lambda object_lists, keys_info, value, **kwargs: self.__build_nested_matcher(
                object_lists, keys_info, value, **kwargs
            )
        }
        return {**super().crud(), **crud}

    def document_info(self):
        return {"object_lists": {"metadata": "key", "relations": "type"}}

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return super().serialization(from_format, to_format)

    def validation(self):
        return super().validation()

    def __build_nested_matcher(
        self, object_lists, keys_info, value, *, index=0, **kwargs
    ):
        if index == 0 and not any(info["object_list"] for info in keys_info):
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                value = {"$exists": value == "ANY_MATCH"}
            matcher = {".".join(info["key"] for info in keys_info): value}
            if inner_exact_matches := kwargs.get("inner_exact_matches"):
                matcher.update(inner_exact_matches)
            return matcher

        info = keys_info[index]

        if info["object_list"]:
            elem_match = {
                "$elemMatch": {
                    object_lists[info["object_list"]]: info["object_key"],
                    **self.__build_nested_matcher(
                        object_lists, keys_info[index + 1 :], value, index=0, **kwargs
                    ),
                }
            }
            if value in ["ANY_MATCH", "NONE_MATCH"]:
                elem_match_with_exists_operator = deepcopy(elem_match)
                del elem_match["$elemMatch"][keys_info[index + 1]["key"]]
                if value == "NONE_MATCH":
                    return {
                        "NOR_MATCHER": [
                            {info["key"]: {"$all": [elem_match]}},
                            {info["key"]: {"$all": [elem_match_with_exists_operator]}},
                        ]
                    }
            return elem_match if index > 0 else {info["key"]: {"$all": [elem_match]}}

        raise Exception(f"Unable to build nested matcher. See keys_info: {keys_info}")
