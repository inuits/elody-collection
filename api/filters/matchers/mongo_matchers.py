import re as regex

from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def id(self, key, values):
        match_values = []
        for value in values:
            match_values.append({key: {"$elemMatch": {"$eq": value}}})
        return {"$match": {"$or": match_values}}

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

        return self.__determine_range_relations_match(key, {"$gt": value}, parent_key)

    def max(self, key, value, parent_key, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__determine_range_relations_match(key, {"$lt": value}, parent_key)

    def min_included(self, key, value, parent_key, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__determine_range_relations_match(key, {"$gte": value}, parent_key)

    def max_included(self, key, value, parent_key, is_datetime_value):
        if is_datetime_value:
            value = self.__get_datetime_query_value(value, range_match=True)

        return self.__determine_range_relations_match(key, {"$lte": value}, parent_key)

    def in_between(self, key, min, max, parent_key, is_datetime_value):
        if is_datetime_value:
            min = self.__get_datetime_query_value(min, range_match=True)
            max = self.__get_datetime_query_value(max, range_match=True)

        return self.__determine_range_relations_match(
            key, {"$gte": min, "$lte": max}, parent_key
        )

    def any(self, key, parent_key):
        return self.__any_none_match(key, parent_key, "$nin")

    def none(self, key, parent_key):
        return self.__any_none_match(key, parent_key, "$in")

    def __exact_contains_range_match(self, key: str, value, parent_key: str = ""):
        if parent_key:
            return {
                "$match": {
                    "$or": [
                        {
                            parent_key: {
                                "$type": "array",
                                "$elemMatch": {"key": key, "value": value},
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

    def __determine_range_relations_match(
        self, key: str | list[str], value: dict, parent_key: str
    ):
        if isinstance(key, str):
            return self.__exact_contains_range_match(key, value, parent_key)
        return self.__relations_match(key, value)

    def __relations_match(self, keys: list[str], value: dict):
        relation_match = {"$match": {"relations.type": {"$in": keys}}}
        number_of_relations_calculator = {
            "$addFields": {
                "numberOfRelations": {
                    "$size": {
                        "$filter": {
                            "input": "$relations",
                            "as": "el",
                            "cond": {"$in": ["$$el.type", keys]},
                        }
                    }
                }
            }
        }
        min_max_match = {"$match": {"numberOfRelations": value}}
        return [relation_match, number_of_relations_calculator, min_max_match]

    def __any_none_match(
        self, key: str, parent_key: str, operator_to_match_none_values: str
    ):
        or_conditions = [
            {
                parent_key: {
                    "$type": "array",
                    "$elemMatch": {
                        "key": key,
                        "value": {operator_to_match_none_values: [None, ""]},
                    },
                }
            },
            {
                "$and": [
                    {parent_key: {"$type": "object"}},
                    {f"{parent_key}.{key}": {"$exists": True}},
                    {
                        f"{parent_key}.{key}": {
                            operator_to_match_none_values: [None, ""]
                        }
                    },
                ]
            },
        ]
        if operator_to_match_none_values == "$in":
            or_conditions.append(
                {parent_key: {"$type": "array", "$not": {"$elemMatch": {"key": key}}}},
            )

        return {"$match": {"$or": or_conditions}}

    def __get_datetime_query_value(self, value, range_match: bool) -> dict | str:
        if not regex.match(BaseMatchers.datetime_pattern, value):
            raise ValueError(f"{value} is not a valid datetime")

        date, time = value.split(" ")
        if range_match:
            return f"{date}T{time}"
        return {"$regex": f"^{date}(T| ){time}"}
