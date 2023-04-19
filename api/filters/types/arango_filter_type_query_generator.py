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
        raise NotImplemented

    def generate_query_for_date_filter_type(self, matchers, filter_criteria):
        raise NotImplemented

    def generate_query_for_number_filter_type(self, matchers, filter_criteria):
        raise NotImplemented

    def generate_query_for_selection_filter_type(self, matchers, filter_criteria):
        raise NotImplemented

    def generate_query_for_boolean_filter_type(self, matchers, filter_criteria):
        raise NotImplemented

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
