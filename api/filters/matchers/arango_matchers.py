from filters.matchers.base_matchers import BaseMatchers


class ArangoMatchers(BaseMatchers):
    def id(self, key, values):
        return f"FILTER LENGTH(INTERSECTION(doc.{key}, {values})) > 0"

    def exact(self, key, value, parent_key, is_datetime_value):
        if parent_key:
            return self.__value_match_with_parent_key_of_type_array(
                key, parent_key, "==", [value]
            )
        return self.__value_match_without_parent_key(key, value, parent_key, exact=True)

    def contains(self, key, value, parent_key):
        return self.__value_match_without_parent_key(
            key, value, parent_key, exact=False
        )

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
        return self.__value_match_with_parent_key_of_type_array(key, parent_key, "!=")

    def none(self, key, parent_key):
        return self.__value_match_with_parent_key_of_type_array(key, parent_key, "==")

    def __value_match_without_parent_key(
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

    def __value_match_with_parent_key_of_type_array(
        self,
        key: str,
        parent_key: str,
        operator_to_match_none_values: str,
        values: list = [None, ""],
    ):
        and_or_condition = "AND" if operator_to_match_none_values == "!=" else "OR"
        value_match = f'item.value {operator_to_match_none_values} "{values[0]}"'
        for i in range(1, len(values)):
            value_match += f' {and_or_condition} item.value {operator_to_match_none_values} "{values[i]}"'

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
