from abc import ABC, abstractmethod
from filters.matchers.matchers import BaseMatcher
from typing import Type


class BaseFilterTypeQueryGenerator(ABC):
    @abstractmethod
    def generate_query_for_id_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            filter_criteria.get("parent_key", ""),
            match_exact=filter_criteria.get("match_exact"),
        )

    @abstractmethod
    def generate_query_for_text_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        root_fields = ["mimetype"]
        if filter_criteria["key"] in root_fields:
            return matchers["contains"]().match(
                filter_criteria["key"], filter_criteria["value"]
            )

        return None

    @abstractmethod
    def generate_query_for_date_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            filter_criteria.get("parent_key", ""),
            match_exact=True,
            is_datetime_value=True,
        )

    @abstractmethod
    def generate_query_for_number_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            filter_criteria.get("parent_key", ""),
            match_exact=True,
        )

    @abstractmethod
    def generate_query_for_selection_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            filter_criteria.get("parent_key", ""),
            match_exact=filter_criteria.get("match_exact"),
        )

    @abstractmethod
    def generate_query_for_boolean_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            filter_criteria.get("parent_key", ""),
            match_exact=True,
        )

    @abstractmethod
    def generate_query_for_type_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            "type",
            filter_criteria["value"],
            "",
            match_exact=True,
        )

    @abstractmethod
    def generate_query_for_metadata_on_relation_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ):
        return self._apply_matchers(
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            "relations",
            match_exact=filter_criteria.get("match_exact"),
        )

    def _apply_matchers(
        self,
        matchers: dict[str, Type[BaseMatcher]],
        key: str | list[str],
        value,
        parent_key: str = "",
        **kwargs
    ):
        for matcher in matchers.values():
            result = matcher().match(key, value, parent_key, **kwargs)
            if result:
                return result
