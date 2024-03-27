import re as regex

from elody.util import interpret_flat_key
from filters_v2.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def exact(self, key, value, is_datetime_value):
        if isinstance(value, list):
            value = {"$in": value}
        elif is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=False)

        return self.__exact_contains_range_match(key, value)

    def contains(self, key, value):
        match_value = {"$regex": value, "$options": "i"}
        return self.__exact_contains_range_match(key, match_value)

    def min(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$gt": value})

    def max(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$lt": value})

    def min_included(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$gte": value})

    def max_included(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$lte": value})

    def in_between(self, key, min, max, is_datetime_value):
        if is_datetime_value:
            min = self.__get_datetime_query_value(min, range_match=True)
            max = self.__get_datetime_query_value(max, range_match=True)

        return self.__exact_contains_range_match(key, {"$gte": min, "$lte": max})

    def any(self, key):
        return self.__any_none_match(key, "$nin")

    def none(self, key):
        return self.__any_none_match(key, "$in")

    def __exact_contains_range_match(self, key: str, value):
        object_lists_config = BaseMatchers.get_object_lists_config()
        keys_info = interpret_flat_key(key, object_lists_config)
        return self.__build_nested_matcher(object_lists_config, keys_info, value)

    def __any_none_match(self, key: str, operator_to_match_none_values: str):
        value = {operator_to_match_none_values: [None, ""]}
        object_lists_config = BaseMatchers.get_object_lists_config()
        keys_info = interpret_flat_key(key, object_lists_config)
        return self.__build_nested_matcher(object_lists_config, keys_info, value)

    def __get_datetime_query_value(self, value, range_match: bool) -> dict | str:
        if not regex.match(BaseMatchers.datetime_pattern, value):
            raise ValueError(f"{value} is not a valid datetime")

        if range_match:
            return f"{value}"
        return {"$regex": f"^{value}"}

    def __build_nested_matcher(self, object_lists_config, keys_info, value, index=0):
        if index == 0 and not any(info["is_object_list"] for info in keys_info):
            return {".".join(info["key"] for info in keys_info): value}

        info = keys_info[index]

        if info["is_object_list"]:
            nested_matcher = self.__build_nested_matcher(
                object_lists_config, keys_info, value, index + 1
            )
            elem_match = {
                "$elemMatch": {
                    object_lists_config[info["key"]]: info["object_key"],
                    keys_info[index + 1]["key"]: nested_matcher,
                }
            }
            return elem_match if index > 0 else {info["key"]: {"$all": [elem_match]}}

        return value
