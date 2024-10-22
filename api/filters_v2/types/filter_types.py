from abc import ABC, abstractmethod
from elody.error_codes import ErrorCode, get_error_code, get_read, get_write
from filters_v2.filter_matcher_mapping import FilterMatcherMapping
from filters_v2.matchers.matchers import BaseMatcher
from filters_v2.types.base_filter_type_query_generator import (
    BaseFilterTypeQueryGenerator,
)
from filters_v2.types.mongo_filter_type_query_generator import (
    MongoFilterTypeQueryGenerator,
)
from os import getenv
from typing import Type


def get_filter(input_type: str):
    if input_type == "text":
        return TextFilterType()
    if input_type == "date":
        return DateFilterType()
    if input_type == "number":
        return NumberFilterType()
    if input_type == "selection":
        return SelectionFilterType()
    if input_type == "boolean":
        return BooleanFilterType()
    if input_type == "type":
        return TypeFilterType()

    raise ValueError(
        f"{get_error_code(ErrorCode.UNDEFINED_FILTER_FOR_INPUT_TYPE, get_read())} No filter defined for input type '{input_type}'"
    )


class BaseFilterType(ABC):
    def __init__(self):
        self.filter_type_engine: BaseFilterTypeQueryGenerator = {
            "arango": MongoFilterTypeQueryGenerator,
            "mongo": MongoFilterTypeQueryGenerator,
        }.get(
            getenv("DB_ENGINE", "mongo")
        )()  # type: ignore
        self.matchers: dict[str, Type[BaseMatcher]] = {}

    @abstractmethod
    def generate_query(self, filter_criteria: dict) -> dict:
        pass


class TextFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(FilterMatcherMapping.mapping["text"])

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_text_filter_type(
            self.matchers, filter_criteria
        )


class DateFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(FilterMatcherMapping.mapping["date"])

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_date_filter_type(
            self.matchers, filter_criteria
        )


class NumberFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(FilterMatcherMapping.mapping["number"])

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_number_filter_type(
            self.matchers, filter_criteria
        )


class SelectionFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(FilterMatcherMapping.mapping["selection"])

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_selection_filter_type(
            self.matchers, filter_criteria
        )


class BooleanFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(FilterMatcherMapping.mapping["boolean"])

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_boolean_filter_type(
            self.matchers, filter_criteria
        )


class TypeFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(FilterMatcherMapping.mapping["type"])

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_type_filter_type(
            self.matchers, filter_criteria
        )
