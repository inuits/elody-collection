from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def case_insensitive(self, key, value, sub_key):
        match_value = {"$regex": value, "$options": "i"}

        if not sub_key:
            return {"$match": {key: match_value}}
        else:
            return {"$match": {key: {"$elemMatch": {sub_key: match_value}}}}
