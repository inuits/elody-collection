from datetime import datetime
from elody.util import interpret_flat_key
from filters_v2.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def exact(self, key, value, is_datetime_value):
        if isinstance(value, list):
            value = {"$in": value}
        elif is_datetime_value:
            value = self.__get_datetime_query_value(value)
            return self.__contains_range_match(key, value)

        object_lists = BaseMatchers.get_object_lists()
        keys_info = interpret_flat_key(key, object_lists)
        build_nested_matcher = BaseMatchers.get_custom_nested_matcher_builder()
        return build_nested_matcher(object_lists, keys_info, value)

    def contains(self, key, value):
        match_value = {"$regex": value, "$options": "i"}
        return self.__contains_range_match(key, match_value)

    def min(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)

        return self.__contains_range_match(key, {"$gt": value})

    def max(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)

        return self.__contains_range_match(key, {"$lt": value})

    def min_included(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)

        return self.__contains_range_match(key, {"$gte": value})

    def max_included(self, key, value, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)

        return self.__contains_range_match(key, {"$lte": value})

    def in_between(self, key, min, max, is_datetime_value):
        if is_datetime_value:
            min = self.__get_datetime_query_value(min)
            max = self.__get_datetime_query_value(max)

        return self.__contains_range_match(key, {"$gte": min, "$lte": max})

    def any(self, key):
        return self.__any_none_match(key, "ANY_MATCH")

    def none(self, key):
        return self.__any_none_match(key, "NONE_MATCH")

    def __contains_range_match(self, key: str, value):
        object_lists = BaseMatchers.get_object_lists()
        keys_info = interpret_flat_key(key, object_lists)
        build_nested_matcher = BaseMatchers.get_base_nested_matcher_builder()
        return build_nested_matcher(object_lists, keys_info, value)

    def __any_none_match(self, key: str, value: str):
        object_lists = BaseMatchers.get_object_lists()
        keys_info = interpret_flat_key(key, object_lists)
        build_nested_matcher = BaseMatchers.get_base_nested_matcher_builder()
        return build_nested_matcher(object_lists, keys_info, value)

    def __get_datetime_query_value(self, value) -> datetime:
        return datetime.fromisoformat(value)
