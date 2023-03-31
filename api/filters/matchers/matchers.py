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
    def match(self, key: str, value: str, sub_key: str = "", **kwargs):
        pass


class CaseInsensitiveMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, sub_key, **_):
        self.matcher_engine.case_insensitive(key, value, sub_key)
