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
    def match(self, key: str, value: str, parent_key: str = "", **kwargs):
        pass


class ExactMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        self.matcher_engine.exact(key, value, parent_key)


class ContainsMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        self.matcher_engine.contains(key, value, parent_key)


class AnyMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        self.matcher_engine.any(key)
        del value
        del parent_key


class NoneMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        self.matcher_engine.none(key)
        del value
        del parent_key
