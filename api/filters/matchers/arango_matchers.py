from filters.matchers.base_matchers import BaseMatchers


class ArangoMatchers(BaseMatchers):
    def id(self, key, values):
        return f"FILTER LENGTH(INTERSECTION(doc.{key}, {values})) > 0"

    def exact(self, key, value, parent_key, is_datetime_value):
        return self.__exact_contains_match(key, value, parent_key, exact=True)

    def contains(self, key, value, parent_key):
        return self.__exact_contains_match(key, value, parent_key, exact=False)

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

    def __exact_contains_match(
        self, key: str, value, parent_key: str = "", *, exact: bool
    ):
        if exact:
            array_condition = f'"{value}" IN doc.{key}'
            equality_operator = "=="
            prefix, suffix = "", ""
        else:
            array_condition = f'CONTAINS(doc.{key}, "{value}")'
            equality_operator = "LIKE"
            prefix, suffix = 2 * "%"

        if parent_key:
            raise NotImplemented

        return f"""
            FILTER (
                IS_ARRAY(doc.{key}) AND {array_condition}
            ) OR (
                doc.{key} {equality_operator} "{prefix}{value}{suffix}"
            )
        """
