from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def id(self, key, value):
        values = value.split(BaseMatchers.separator)
        match_values = []
        for match_value in values:
            match_values.append({key: {"$elemMatch": {"$eq": match_value}}})
        return {"$match": {"$or": match_values}}

    def exact(self, key, value, parent_key):
        return self.__exact_contains_match(key, value, parent_key)

    def contains(self, key, value, parent_key):
        match_value = {"$regex": value, "$options": "i"}
        return self.__exact_contains_match(key, match_value, parent_key)

    def any(self, key):
        return {"$match": {key: {"$exists": True, "$ne": None}}}

    def none(self, key):
        return {"$match": {"$or": [{key: {"$exists": False}}, {key: {"$eq": None}}]}}

    def __exact_contains_match(self, key, value, parent_key):
        if parent_key:
            return {"$match": {parent_key: {"$elemMatch": {key: value}}}}
        else:
            return {"$match": {key: value}}
