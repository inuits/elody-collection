import os

from abc import ABC, abstractmethod
from filters.matchers.matchers import BaseMatcher
from filters.matchers.matchers import (
    IdMatcher,
    ExactMatcher,
    ContainsMatcher,
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
    def generate_query(self, filter_criteria: dict):
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
                "contains": ContainsMatcher,
                "any": AnyMatcher,
                "none": NoneMatcher,
            }
        )

    def generate_query(self, filter_criteria: dict):
        return self.filter_type_engine.generate_query_for_text_filter_type(
            self.matchers, filter_criteria
        )
