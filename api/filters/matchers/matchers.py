import os

from abc import ABC, abstractmethod
from filters.matchers.base_matchers import BaseMatchers
from filters.matchers.mongo_matchers import MongoMatchers


class BaseMatcher(ABC):
    def __init__(self):
        self.matcher_engine: BaseMatchers = {
            "arango": "ArangoMatchers",
            "mongo": MongoMatchers,
        }.get(
            os.getenv("DB_ENGINE", "arango")
        )()  # type: ignore

    @abstractmethod
    def match(self, filter_request: dict):
        pass


class ExactMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, filter_request_body: dict):
        self.matcher_engine.exact_match(filter_request_body)
