import re as regex

from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def id(self, key, values, parent_key):
        return self.__exact_contains_range_match(key, {"$in": values}, parent_key)

    def exact(self, key, value, parent_key, is_datetime_value):
        if isinstance(value, list):
            value = {"$in": value}
        elif is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=False)

        return self.__exact_contains_range_match(key, value, parent_key)

    def contains(self, key, value, parent_key):
        match_value = {"$regex": value, "$options": "i"}
        return self.__exact_contains_range_match(key, match_value, parent_key)

    def min(self, key, value, parent_key, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$gt": value}, parent_key)

    def max(self, key, value, parent_key, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$lt": value}, parent_key)

    def min_included(self, key, value, parent_key, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$gte": value}, parent_key)

    def max_included(self, key, value, parent_key, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__exact_contains_range_match(key, {"$lte": value}, parent_key)

    def in_between(self, key, min, max, parent_key, is_datetime_value):
        if is_datetime_value:
            min = self.__get_datetime_query_value(min, range_match=True)
            max = self.__get_datetime_query_value(max, range_match=True)

        return self.__exact_contains_range_match(
            key, {"$gte": min, "$lte": max}, parent_key
        )

    def any(self, key, parent_key):
        return self.__any_none_match(key, parent_key, "$nin")

    def none(self, key, parent_key):
        return self.__any_none_match(key, parent_key, "$in")

    def metadata_on_relation(self, key, value, parent_key, match_exact):
        if not match_exact:
            value = {"$regex": value, "$options": "i"}

        return {
            "$match": {
                parent_key: {
                    "$type": "array",
                    "$elemMatch": {key: value},
                }
            }
        }

    def __exact_contains_range_match(self, key: str, value, parent_key: str = ""):
        if parent_key:
            document_key, document_value = BaseMatchers.get_document_key_value(
                parent_key
            )
            if isinstance(value, str) or (
                isinstance(value, dict) and value.get("$regex")
            ):
                document_value = "value"

            return {
                "$match": {
                    "$or": [
                        {
                            parent_key: {
                                "$type": "array",
                                "$elemMatch": {
                                    document_key: key,
                                    document_value: value,
                                },
                            }
                        },
                        {
                            "$and": [
                                {parent_key: {"$type": "object"}},
                                {f"{parent_key}.{key}": value},
                            ]
                        },
                    ]
                }
            }

        return {"$match": {key: value}}

    def __any_none_match(
        self, key: str, parent_key: str, operator_to_match_none_values: str
    ):
        value = {operator_to_match_none_values: [None, ""]}

        if parent_key:
            document_key, document_value = BaseMatchers.get_document_key_value(
                parent_key
            )
            or_conditions = [
                {
                    parent_key: {
                        "$type": "array",
                        "$elemMatch": {document_key: key, document_value: value},
                    }
                },
                {
                    "$and": [
                        {parent_key: {"$type": "object"}},
                        {f"{parent_key}.{key}": {"$exists": True}},
                        {f"{parent_key}.{key}": value},
                    ]
                },
            ]
            if operator_to_match_none_values == "$in":
                or_conditions.append(
                    {
                        parent_key: {
                            "$type": "array",
                            "$not": {"$elemMatch": {"key": key}},
                        }
                    },
                )
        else:
            or_conditions = [{key: value}]

        return {"$match": {"$or": or_conditions}}

    def __get_datetime_query_value(self, value, range_match: bool) -> dict | str:
        if not regex.match(BaseMatchers.datetime_pattern, value):
            raise ValueError(f"{value} is not a valid datetime")

        if range_match:
            return f"{value}"
        return {"$regex": f"^{value}"}
