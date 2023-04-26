from filters.matchers.base_matchers import BaseMatchers


class ArangoMatchers(BaseMatchers):
    def id(self, key, values):
        return f"FILTER LENGTH(INTERSECTION(doc.{key}, {values})) > 0"

    def exact(self, key, value, parent_key, is_datetime_value):
        return self.__exact_contains_match(
            key, value, parent_key, "IN" if isinstance(value, list) else "=="
        )

    def contains(self, key, value, parent_key):
        return self.__exact_contains_match(key, value, parent_key, "LIKE")

    def min(self, key, value, parent_key, is_datetime_value):
        raise NotImplemented

    def max(self, key, value, parent_key, is_datetime_value):
        raise NotImplemented

    def min_included(self, key, value, parent_key, is_datetime_value):
        if isinstance(key, str):
            return self.__value_match_with_parent_key_of_type_array(
                key, [value], parent_key, ">="
            )
        raise NotImplemented

    def max_included(self, key, value, parent_key, is_datetime_value):
        if isinstance(key, str):
            return self.__value_match_with_parent_key_of_type_array(
                key, [value], parent_key, "<="
            )
        raise NotImplemented

    def in_between(self, key, min, max, parent_key, is_datetime_value):
        return self.__value_match_with_parent_key_of_type_array(
            key, [min, max], parent_key, [">=", "<="]
        )

    def any(self, key, parent_key):
        return self.__value_match_with_parent_key_of_type_array(
            key, ["null", ""], parent_key, "!="
        )

    def none(self, key, parent_key):
        return self.__value_match_with_parent_key_of_type_array(
            key, ["null", ""], parent_key, "=="
        )

    def __exact_contains_match(
        self, key: str, value, parent_key: str, equality_operator: str
    ):
        if parent_key:
            return self.__value_match_with_parent_key_of_type_array(
                key, [value], parent_key, equality_operator
            )
        return self.__value_match_without_parent_key(key, value, equality_operator)

    def __value_match_without_parent_key(self, key: str, value, equality_operator: str):
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
        self, key: str, values: list, parent_key: str, operator: str | list[str]
    ):
        comparison_operators, logical_operator = self.__determine_query_operators(
            operator, values
        )
        prefix, suffix = self.__get_prefix_and_suffix(comparison_operators[0])

        value_match = (
            f'item.value {comparison_operators[0]} "{prefix}{values[0]}{suffix}"'
        )
        for i in range(0, len(values)):
            if not isinstance(values[i], str) or values[i] == "null":
                value_match = value_match.replace(f'"{values[i]}"', f"{values[i]}")
            try:
                value_match += f' {logical_operator} item.value {comparison_operators[i + 1]} "{prefix}{values[i + 1]}{suffix}"'
            except IndexError:
                break

        return f"""
            FILTER (
                IS_ARRAY(doc.{parent_key})
                AND (
                    LENGTH(
                        FOR item IN IS_ARRAY(doc.{parent_key}) ? doc.{parent_key} : []
                            FILTER (
                                item.key == "{key}"
                                AND ({value_match})
                            )
                            RETURN item
                    ) > 0
                )
            ) OR (
                HAS(doc.{parent_key}, "{key}")
                AND ({value_match.replace("item.value", f"doc.{parent_key}.{key}")})
            )
        """

    def __get_prefix_and_suffix(self, equality_operator: str):
        return (2 * "%") if equality_operator == "LIKE" else ("", "")

    def __determine_query_operators(self, operator: str | list[str], values: list):
        if isinstance(operator, str):
            return len(values) * [operator], "AND" if operator == "!=" else "OR"
        else:
            return operator, "AND"
