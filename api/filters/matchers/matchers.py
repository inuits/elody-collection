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
        self, key: str, value, parent_key: str = "", **kwargs
    ) -> dict | str | None:
        pass


class IdMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, _, parent_key, **kwargs):
        if key == "identifiers" and isinstance(kwargs["ids"], list):
            return self.matcher_engine.id(key, kwargs["ids"])

        del parent_key


class ExactMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **kwargs):
        if isinstance(value, str) and kwargs["match_exact"]:
            return self.matcher_engine.exact(key, value, parent_key)


class ContainsMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        return self.matcher_engine.contains(key, value, parent_key)


class AfterMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **kwargs):
        if kwargs["after"] and not kwargs["before"] and not kwargs["or_equal"]:
            return self.matcher_engine.after(key, value, parent_key)


class BeforeMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **kwargs):
        if kwargs["before"] and not kwargs["after"] and not kwargs["or_equal"]:
            return self.matcher_engine.before(key, value, parent_key)


class AfterOrEqualMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **kwargs):
        if kwargs["after"] and not kwargs["before"] and kwargs["or_equal"]:
            return self.matcher_engine.after_or_equal(key, value, parent_key)


class BeforeOrEqualMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **kwargs):
        if kwargs["before"] and not kwargs["after"] and kwargs["or_equal"]:
            return self.matcher_engine.before_or_equal(key, value, parent_key)


class InBetweenMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, _, parent_key, **kwargs):
        if kwargs["after"] and kwargs["before"]:
            return self.matcher_engine.in_between(
                key, kwargs["after"], kwargs["before"], parent_key
            )


class AnyMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if value == "*":
            return self.matcher_engine.any(key)

        del parent_key


class NoneMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, parent_key, **_):
        if value == "":
            return self.matcher_engine.none(key)

        del parent_key
