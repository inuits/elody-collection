from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def id(self, key, values):
        match_values = []
        for value in values:
            match_values.append({key: {"$elemMatch": {"$eq": value}}})
        return {"$match": {"$or": match_values}}

    def exact(self, key, value, parent_key):
        return self.__exact_contains_match(key, value, parent_key)

    def contains(self, key, value, parent_key):
        match_value = {"$regex": value, "$options": "i"}
        return self.__exact_contains_match(key, match_value, parent_key)

    def after(self, key, value, parent_key):
        return self.__range_match(key, value, parent_key, "$gt")

    def before(self, key, value, parent_key):
        return self.__range_match(key, value, parent_key, "$lt")

    def after_or_equal(self, key, value, parent_key):
        return self.__range_match(key, value, parent_key, "$gte")

    def before_or_equal(self, key, value, parent_key):
        return self.__range_match(key, value, parent_key, "$lte")

    def in_between(self, key, after, before, parent_key):
        return self.__range_match(key, [after, before], parent_key, ["$gte", "$lte"])

    def any(self, key):
        return {"$match": {key: {"$exists": True, "$ne": None}}}

    def none(self, key):
        return {"$match": {"$or": [{key: {"$exists": False}}, {key: {"$eq": None}}]}}

    def __exact_contains_match(self, key: str, value: str | dict, parent_key: str = ""):
        if parent_key:
            return {"$match": {parent_key: {"$elemMatch": {key: value}}}}
        else:
            return {"$match": {key: value}}

    def __range_match(
        self,
        key: str,
        value: str | list[str | int],
        parent_key: str,
        operator: str | list[str],
    ):
        if isinstance(value, str) and isinstance(operator, str):
            return {
                "$match": {
                    parent_key: {"$elemMatch": {f"{key}_float": {operator: value}}}
                }
            }
        elif isinstance(value, list) and isinstance(operator, list):
            match_value = {}
            for i in range(len(operator)):
                match_value.update({operator[i]: value[i]})

            return {
                "$match": {parent_key: {"$elemMatch": {f"{key}_float": match_value}}}
            }

        raise TypeError(
            f"Parameters 'value: {value}' and 'operator: {operator}' should have the same type"
        )
