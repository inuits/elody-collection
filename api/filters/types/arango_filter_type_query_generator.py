from filters.matchers.matchers import BaseMatcher
from filters.types.base_filter_type_query_generator import BaseFilterTypeQueryGenerator
from typing import Type


class ArangoFilterTypeQueryGenerator(BaseFilterTypeQueryGenerator):
    def generate_query_for_id_filter_type(self, matchers, filter_criteria):
        return self.__apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            match_exact=filter_criteria.get("match_exact"),
        )

    def generate_query_for_text_filter_type(self, matchers, filter_criteria):
        root_fields = ["filename", "mimetype"]

        if filter_criteria["key"] in root_fields:
            return matchers["contains"]().match(
                filter_criteria["key"], filter_criteria["value"]
            )
        else:
            aql = ""
            if filter_criteria.get("label"):
                result = matchers["contains"]().match(
                    "label", filter_criteria["label"], "metadata"
                )
                if result and isinstance(result, str):
                    aql += result

            aql += self.__apply_matchers(
                matchers,
                filter_criteria["key"],
                filter_criteria["value"],
                "metadata",
                match_exact=filter_criteria.get("match_exact"),
            )

            return aql

    def generate_query_for_date_filter_type(self, matchers, filter_criteria):
        return self.__apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            "metadata",
            match_exact=True,
            is_datetime_value=True,
        )

    def generate_query_for_number_filter_type(self, matchers, filter_criteria):
        return self.__apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            "metadata",
            match_exact=True,
        )

    def generate_query_for_selection_filter_type(self, matchers, filter_criteria):
        return self.__apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            "metadata",
            match_exact=True,
        )

    def generate_query_for_boolean_filter_type(self, matchers, filter_criteria):
        return self.__apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            "metadata",
            match_exact=True,
        )

    def __apply_matchers(
        self,
        matchers: dict[str, Type[BaseMatcher]],
        key: str | list[str],
        value,
        parent_key: str = "",
        **kwargs
    ) -> str:
        for matcher in matchers.values():
            result = matcher().match(key, value, parent_key, **kwargs)
            if result and isinstance(result, str):
                return result

        return ""
