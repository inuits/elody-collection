from filters.matchers.base_matchers import BaseMatchers


class ArangoMatchers(BaseMatchers):
    def id(self, key, values):
        return f"FILTER LENGTH(INTERSECTION(doc.{key}, {values})) > 0"

    def exact(self, key, value, parent_key, is_datetime_value):
        return self.__exact_contains_match(key, value, parent_key, "==")

    def contains(self, key, value, parent_key):
        return self.__exact_contains_match(key, value, parent_key, "LIKE")

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
        return self.__value_match_with_parent_key_of_type_array(
            key, parent_key, "!=", [None, ""]
        )

    def none(self, key, parent_key):
        return self.__value_match_with_parent_key_of_type_array(
            key, parent_key, "==", [None, ""]
        )

    def __exact_contains_match(
        self, key: str, value, parent_key: str, equality_operator
    ):
        if parent_key:
            return self.__value_match_with_parent_key_of_type_array(
                key, parent_key, equality_operator, [value]
            )
        return self.__value_match_without_parent_key(key, value, equality_operator)

    def __get_prefix_and_suffix(self, equality_operator):
        return (2 * "%") if equality_operator == "LIKE" else ("", "")

    def __value_match_without_parent_key(self, key: str, value, equality_operator):
        if equality_operator == "LIKE":
            array_condition = f'CONTAINS(doc.{key}, "{value}")'
        else:
            array_condition = f'"{value}" IN doc.{key}'

        prefix, suffix = self.__get_prefix_and_suffix(equality_operator)
        return f"""
            FILTER (IS_ARRAY(doc.{key}) AND {array_condition})
                OR (doc.{key} {equality_operator} "{prefix}{value}{suffix}")
        """

    def __value_match_with_parent_key_of_type_array(
        self, key: str, parent_key: str, operator: str, values: list
    ):
        prefix, suffix = self.__get_prefix_and_suffix(operator)
        and_or_condition = "AND" if operator == "!=" else "OR"

        value_match = f'item.value {operator} "{prefix}{values[0]}{suffix}"'
        for i in range(1, len(values)):
            value_match += f' {and_or_condition} item.value {operator} "{prefix}{values[i]}{suffix}"'

        return f"""
            FILTER (
                IS_ARRAY(doc.{parent_key})
                AND (
                    LENGTH(
                        FOR item IN doc.{parent_key}
                            FILTER (
                                item.key == "{key}"
                                AND ({value_match})
                            )
                            RETURN item
                    ) > 0
                )
            )
        """
