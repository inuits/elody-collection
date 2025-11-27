from datetime import datetime
from elody.util import interpret_flat_key
from filters_v2.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def exact(
        self,
        key,
        value,
        is_datetime_value=False,
        aggregation="",
        inner_exact_matches={},
        list_operation="or",
    ):
        match list_operation:
            case "and":
                operator = "$all"
            case "or":
                operator = "$in"
            case _:
                operator = "$in"
        if isinstance(value, list):
            value = {operator: value}
        elif is_datetime_value:
            value = self.__get_datetime_query_value(value)
            return self.__contains_range_match(key, value)
        elif aggregation:
            return self.__aggregation_match(key, {"$eq": value}, aggregation)

        object_lists = BaseMatchers.get_object_lists()
        keys_info = interpret_flat_key(key, object_lists)
        build_nested_matcher = BaseMatchers.get_custom_nested_matcher_builder()
        return build_nested_matcher(
            object_lists, keys_info, value, inner_exact_matches=inner_exact_matches
        )

    def contains(self, key, value, inner_exact_matches={}):
        match_value = {"$regex": value, "$options": "i"}
        return self.__contains_range_match(key, match_value, inner_exact_matches)

    # TODO: Error checking on the regex options
    def regex(self, key, value, inner_exact_matches={}, options=""):
        match_value = {"$regex": value, "$options": options}
        return self.__contains_range_match(key, match_value, inner_exact_matches)

    def min(self, key, value, is_datetime_value=False, aggregation=""):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)
        elif aggregation:
            return self.__aggregation_match(key, {"$gt": value}, aggregation)

        return self.__contains_range_match(key, {"$gt": value})

    def max(self, key, value, is_datetime_value=False, aggregation=""):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)
        elif aggregation:
            return self.__aggregation_match(key, {"$lt": value}, aggregation)

        return self.__contains_range_match(key, {"$lt": value})

    def min_included(self, key, value, is_datetime_value=False, aggregation=""):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)
        elif aggregation:
            return self.__aggregation_match(key, {"$gte": value}, aggregation)

        return self.__contains_range_match(key, {"$gte": value})

    def max_included(self, key, value, is_datetime_value=False, aggregation=""):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value)
        elif aggregation:
            return self.__aggregation_match(key, {"$lte": value}, aggregation)

        return self.__contains_range_match(key, {"$lte": value})

    def in_between(self, key, min, max, is_datetime_value=False, aggregation=""):
        if is_datetime_value:
            min = self.__get_datetime_query_value(min)
            max = self.__get_datetime_query_value(max)
        elif aggregation:
            return self.__aggregation_match(
                key, {"$gte": min, "$lte": max}, aggregation
            )

        return self.__contains_range_match(key, {"$gte": min, "$lte": max})

    def any(self, key, inner_exact_matches={}):
        return self.__any_none_match(key, "ANY_MATCH", inner_exact_matches)

    def none(self, key):
        return self.__any_none_match(key, "NONE_MATCH")

    def geo(self, key, value):
        return {key: {"$geoWithin": {"$geometry": value}}}

    def __aggregation_match(self, key: str, value, aggregation: str):
        return {
            "$expr": {
                "$and": [
                    {
                        operator: [
                            {f"${aggregation}": {"$ifNull": [f"${key}", []]}},
                            value[operator],
                        ]
                    }
                    for operator in value.keys()
                ]
            }
        }

    def __contains_range_match(self, key: str, value, inner_exact_matches={}):
        object_lists = BaseMatchers.get_object_lists()
        keys_info = interpret_flat_key(key, object_lists)
        build_nested_matcher = BaseMatchers.get_base_nested_matcher_builder()
        return build_nested_matcher(
            object_lists, keys_info, value, inner_exact_matches=inner_exact_matches
        )

    def __any_none_match(self, key: str, value: str, inner_exact_matches={}):
        object_lists = BaseMatchers.get_object_lists()
        keys_info = interpret_flat_key(key, object_lists)
        build_nested_matcher = BaseMatchers.get_base_nested_matcher_builder()
        return build_nested_matcher(
            object_lists, keys_info, value, inner_exact_matches=inner_exact_matches
        )

    def __get_datetime_query_value(self, value) -> datetime:
        return datetime.fromisoformat(value)
