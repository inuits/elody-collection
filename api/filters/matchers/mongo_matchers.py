from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def exact(self, key, value, parent_key):
        return {"$match": {parent_key: {"$elemMatch": {key: value}}}}

    def contains(self, key, value, parent_key):
        match_value = {"$regex": value, "$options": "i"}

        if not parent_key:
            return {"$match": {key: match_value}}
        else:
            return {"$match": {parent_key: {"$elemMatch": {key: match_value}}}}

    def any(self, key):
        return {"$match": {key: {"$exists": True, "$ne": None}}}

    def none(self, key):
        return {"$match": {"$or": [{key: {"$exists": False}}, {key: {"$eq": None}}]}}
