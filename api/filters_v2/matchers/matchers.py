from abc import ABC, abstractmethod
from filters_v2.matchers.base_matchers import BaseMatchers
from filters_v2.matchers.mongo_matchers import MongoMatchers
from os import getenv


class BaseMatcher(ABC):
    def __init__(self):
        self.matcher_engine: BaseMatchers = {
            "mongo": MongoMatchers,
        }.get(
            getenv("DB_ENGINE", "mongo")
        )()  # type: ignore

    @abstractmethod
    def match(self, key: str | list[str], value, **kwargs) -> dict | str | list | None:
        pass


class ExactMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **kwargs):
        if (
            isinstance(key, str)
            and isinstance(value, (str, int, float, bool, list))
            and kwargs.get("match_exact", False)
        ):
            return self.matcher_engine.exact(
                key, value, kwargs.get("is_datetime_value", False)
            )


class ContainsMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **kwargs):
        if isinstance(key, str) and not kwargs.get("match_exact", False):
            return self.matcher_engine.contains(key, value)


class MinMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **kwargs):
        if (
            isinstance(key, str)
            and isinstance(value, dict)
            and value.get("min")
            and not value.get("max")
            and not value.get("included")
        ):
            return self.matcher_engine.min(
                key, value["min"], kwargs.get("is_datetime_value", False)
            )


class MaxMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **kwargs):
        if (
            isinstance(key, str)
            and isinstance(value, dict)
            and not value.get("min")
            and value.get("max")
            and not value.get("included")
        ):
            return self.matcher_engine.max(
                key, value["max"], kwargs.get("is_datetime_value", False)
            )


class MinIncludedMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **kwargs):
        if (
            isinstance(key, str)
            and isinstance(value, dict)
            and value.get("min")
            and not value.get("max")
            and value.get("included")
        ):
            return self.matcher_engine.min_included(
                key, value["min"], kwargs.get("is_datetime_value", False)
            )


class MaxIncludedMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **kwargs):
        if (
            isinstance(key, str)
            and isinstance(value, dict)
            and not value.get("min")
            and value.get("max")
            and value.get("included")
        ):
            return self.matcher_engine.max_included(
                key, value["max"], kwargs.get("is_datetime_value", False)
            )


class InBetweenMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **kwargs):
        if (
            isinstance(key, str)
            and isinstance(value, dict)
            and value.get("min")
            and value.get("max")
        ):
            return self.matcher_engine.in_between(
                key,
                value["min"],
                value["max"],
                kwargs.get("is_datetime_value", False),
            )


class AnyMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **_):
        if isinstance(key, str) and value == "*":
            return self.matcher_engine.any(key)


class NoneMatcher(BaseMatcher):
    def __init__(self):
        super().__init__()

    def match(self, key, value, **_):
        if isinstance(key, str) and value == "":
            return self.matcher_engine.none(key)
