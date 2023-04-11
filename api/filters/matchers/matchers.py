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
    def match(
        self, key: str | list[str], value, parent_key: str = "", **kwargs
    ) -> dict | str | list | None:
        pass


class IdMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if key == "identifiers" and isinstance(value, list):
            return self.matcher_engine.id(key, value)

        del parent_key


class ExactMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key="", **kwargs):
        if (
            isinstance(key, str)
            and isinstance(value, (str, int, bool, list))
            and kwargs["match_exact"]
        ):
            return self.matcher_engine.exact(key, value, parent_key)


class ContainsMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key="", **_):
        if isinstance(key, str):
            return self.matcher_engine.contains(key, value, parent_key)


class MinMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **kwargs):
        if kwargs["min"] and not kwargs["max"] and not kwargs["included"]:
            return self.matcher_engine.min(key, value, parent_key)


class MaxMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **kwargs):
        if kwargs["max"] and not kwargs["min"] and not kwargs["included"]:
            return self.matcher_engine.max(key, value, parent_key)


class MinIncludedMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if (
            isinstance(value, dict)
            and value.get("min")
            and not value.get("max")
            and value.get("included")
        ):
            return self.matcher_engine.min_included(key, value["min"], parent_key)


class MaxIncludedMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if (
            isinstance(value, dict)
            and not value.get("min")
            and value.get("max")
            and value.get("included")
        ):
            return self.matcher_engine.max_included(key, value["max"], parent_key)


class InBetweenMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if isinstance(value, dict) and value.get("min") and value.get("max"):
            return self.matcher_engine.in_between(
                key, value["min"], value["max"], parent_key
            )


class AnyMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if isinstance(key, str) and value == "*":
            return self.matcher_engine.any(key, parent_key)


class NoneMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if isinstance(key, str) and value == "":
            return self.matcher_engine.none(key, parent_key)

        del parent_key
