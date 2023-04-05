from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def id(self, key, values):
        match_values = []
        for value in values:
            match_values.append({key: {"$elemMatch": {"$eq": value}}})
        return {"$match": {"$or": match_values}}

    def exact(self, key, value, parent_key):
        if isinstance(value, list):
            return {"$match": {parent_key: {"$elemMatch": {key: {"$in": value}}}}}
        return self.__exact_contains_match(key, value, parent_key)

    def contains(self, key, value, parent_key):
        match_value = {"$regex": value, "$options": "i"}
        return self.__exact_contains_match(key, match_value, parent_key)

    def min(self, key, value, parent_key):
        return self.__determine_range_relations_match(key, {"$gt": value}, parent_key)

    def max(self, key, value, parent_key):
        return self.__determine_range_relations_match(key, {"$lt": value}, parent_key)

    def min_included(self, key, value, parent_key):
        return self.__determine_range_relations_match(key, {"$gte": value}, parent_key)

    def max_included(self, key, value, parent_key):
        return self.__determine_range_relations_match(key, {"$lte": value}, parent_key)

    def in_between(self, key, min, max, parent_key):
        return self.__determine_range_relations_match(
            key, {"$gte": min, "$lte": max}, parent_key
        )

    def any(self, key):
        return {"$match": {key: {"$exists": True, "$ne": None}}}

    def none(self, key):
        return {"$match": {"$or": [{key: {"$exists": False}}, {key: {"$eq": None}}]}}

    def __exact_contains_match(
        self, key: str, value: str | int | dict, parent_key: str = ""
    ):
        if parent_key:
            return {"$match": {parent_key: {"$elemMatch": {key: value}}}}
        return {"$match": {key: value}}

    def __determine_range_relations_match(
        self, key: str | list[str], value: dict, parent_key: str
    ):
        if isinstance(key, str):
            return self.__range_match(key, value, parent_key)
        return self.__relations_match(key, value)

    def __range_match(self, key: str, value: dict, parent_key: str):
        return {"$match": {parent_key: {"$elemMatch": {f"{key}_float": value}}}}

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
