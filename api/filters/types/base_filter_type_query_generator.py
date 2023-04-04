from abc import ABC, abstractmethod
from filters.matchers.matchers import BaseMatcher
from typing import Type


class BaseFilterTypeQueryGenerator(ABC):
    @abstractmethod
    def generate_query_for_id_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ) -> list:
        pass

    @abstractmethod
    def generate_query_for_text_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ) -> list:
        pass

    @abstractmethod
    def generate_query_for_date_filter_type(
        self, matchers: dict[str, Type[BaseMatcher]], filter_criteria: dict
    ) -> list:
        pass
