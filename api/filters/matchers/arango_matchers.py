from filters.matchers.base_matchers import BaseMatchers


class ArangoMatchers(BaseMatchers):
    def id(self, key, values):
        return f"FILTER LENGTH(INTERSECTION(doc.{key}, {values})) > 0"

    def exact(self, key, value, parent_key, is_datetime_value):
        raise NotImplemented

    def contains(self, key, value, parent_key):
        raise NotImplemented

    def min(self, key, value, parent_key, is_datetime_value):
        raise NotImplemented

    def max(self, key, value, parent_key, is_datetime_value):
        raise NotImplemented

    def min_included(self, key, value, parent_key, is_datetime_value):
        raise NotImplemented

    def max_included(self, key, value, parent_key, is_datetime_value):
        raise NotImplemented

    def in_between(self, key, min, max, parent_key, is_datetime_value):
        raise NotImplemented

    def any(self, key, parent_key):
        raise NotImplemented

    def none(self, key, parent_key):
        raise NotImplemented
