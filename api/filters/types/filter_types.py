import os

from abc import ABC, abstractmethod
from filters.matchers.matchers import BaseMatcher
from filters.matchers.matchers import (
    IdMatcher,
    ExactMatcher,
    ContainsMatcher,
    MinMatcher,
    MaxMatcher,
    MinIncludedMatcher,
    MaxIncludedMatcher,
    InBetweenMatcher,
    AnyMatcher,
    NoneMatcher,
)
from filters.types.base_filter_type_query_generator import BaseFilterTypeQueryGenerator
from filters.types.mongo_filter_type_query_generator import (
    MongoFilterTypeQueryGenerator,
)
from typing import Type


def get_filter(input_type: str):
    if input_type == "IdInput":
        return IdFilterType()
    if input_type == "TextInput":
        return TextFilterType()
    if input_type == "DateInput":
        return DateFilterType()
    if input_type == "NumberInput":
        return NumberFilterType()
    if input_type == "SelectionInput":
        return SelectionFilterType()
    if input_type == "BooleanInput":
        return BooleanFilterType()

    raise ValueError(f"No filter defined for input type '{input_type}'")


class BaseFilterType(ABC):
    def __init__(self):
        self.filter_type_engine: BaseFilterTypeQueryGenerator = {
            "arango": "ArangoFilterTypes",
            "mongo": MongoFilterTypeQueryGenerator,
        }.get(
            os.getenv("DB_ENGINE", "arango")
        )()  # type: ignore
        self.matchers: dict[str, Type[BaseMatcher]] = {}

    @abstractmethod
    def generate_query(self, filter_criteria: dict) -> list:
        pass


class IdFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(
            {
                "id": IdMatcher,
                "exact": ExactMatcher,
                "contains": ContainsMatcher,
            }
        )

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_id_filter_type(
            self.matchers, filter_criteria
        )


class TextFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(
            {
                "exact": ExactMatcher,
                "any": AnyMatcher,
                "none": NoneMatcher,
                "contains": ContainsMatcher,
            }
        )

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_text_filter_type(
            self.matchers, filter_criteria
        )


class DateFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(
            {
                "exact": ExactMatcher,
                "min": MinMatcher,
                "max": MaxMatcher,
                "in_between": InBetweenMatcher,
                "any": AnyMatcher,
                "none": NoneMatcher,
            }
        )

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_date_filter_type(
            self.matchers, filter_criteria
        )


class NumberFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(
            {
                "exact": ExactMatcher,
                "min_included": MinIncludedMatcher,
                "max_included": MaxIncludedMatcher,
                "in_between": InBetweenMatcher,
                "any": AnyMatcher,
                "none": NoneMatcher,
            }
        )

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_number_filter_type(
            self.matchers, filter_criteria
        )


class SelectionFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update(
            {
                "exact": ExactMatcher,
                "any": AnyMatcher,
                "none": NoneMatcher,
            }
        )

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_selection_filter_type(
            self.matchers, filter_criteria
        )


class BooleanFilterType(BaseFilterType):
    def __init__(self):
        super().__init__()
        self.matchers.update({"exact": ExactMatcher})

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_boolean_filter_type(
            self.matchers, filter_criteria
        )
