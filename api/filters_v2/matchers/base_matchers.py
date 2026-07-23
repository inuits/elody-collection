from abc import ABC, ABCMeta, abstractmethod
from contextvars import ContextVar

from configuration import get_object_configuration_mapper

_collection_ctx = ContextVar("collection", default="entities")
_type_ctx = ContextVar("type", default="")
_force_base_ctx = ContextVar("force_base_nested_matcher_builder", default=False)


class ThreadSafeMeta(ABCMeta):
    """
    Intercepts access to BaseMatchers.collection (and others)
    and redirects them to the thread-safe ContextVars.
    Otherwise we need to rewrite some code in base-collection
    (and maybe clients) to use a getter function.
    """

    @property
    def collection(cls):
        return _collection_ctx.get()

    @property
    def type(cls):
        return _type_ctx.get()

    @property
    def force_base_nested_matcher_builder(cls):
        return _force_base_ctx.get()


class BaseMatchers(ABC, metaclass=ThreadSafeMeta):
    # NOTE: We removed the static variables (collection = "entities")
    # because the Metaclass now handles them dynamically.

    @classmethod
    def context(cls, collection: str, type_name: str = "", force_base: bool = False):
        """
        Use this in your Flask views to safely set the context for the block.
        Example:
            with BaseMatchers.context(collection="users"):
                ... do work ...
        """
        return MatcherContext(collection, type_name, force_base)

    @staticmethod
    def get_base_nested_matcher_builder():
        config = get_object_configuration_mapper().get("none")
        return config.crud()["nested_matcher_builder"]

    @staticmethod
    def get_custom_nested_matcher_builder():
        if BaseMatchers.force_base_nested_matcher_builder:
            return BaseMatchers.get_base_nested_matcher_builder()

        config = get_object_configuration_mapper().get(
            BaseMatchers.type or BaseMatchers.collection
        )
        return config.crud()["nested_matcher_builder"]

    @staticmethod
    def get_object_lists() -> dict:
        config = get_object_configuration_mapper().get(
            BaseMatchers.type or BaseMatchers.collection
        )
        return config.document_info().get("object_lists", {})

    @abstractmethod
    def exact(
        self,
        key: str,
        value: str | int | float | bool | list[str],
        is_datetime_value: bool = False,
        aggregation: str = "",
        inner_exact_matches: dict = {},
    ) -> dict:
        pass

    @abstractmethod
    def contains(self, key: str, value: str, inner_exact_matches: dict = {}) -> dict:
        pass

    @abstractmethod
    def contains_not(
        self, key: str, value: str, inner_exact_matches: dict = {}
    ) -> dict:
        pass

    @abstractmethod
    def regex(
        self, key: str, value: str, inner_exact_matches: dict = {}, options: str = ""
    ) -> dict:
        pass

    @abstractmethod
    def min(
        self,
        key: str,
        value: str | int,
        is_datetime_value: bool = False,
        aggregation: str = "",
    ) -> dict:
        pass

    @abstractmethod
    def max(
        self,
        key: str,
        value: str | int,
        is_datetime_value: bool = False,
        aggregation: str = "",
    ) -> dict:
        pass

    @abstractmethod
    def min_included(
        self,
        key: str,
        value: str | int,
        is_datetime_value: bool = False,
        aggregation: str = "",
    ) -> dict:
        pass

    @abstractmethod
    def max_included(
        self,
        key: str,
        value: str | int,
        is_datetime_value: bool = False,
        aggregation: str = "",
    ) -> dict:
        pass

    @abstractmethod
    def in_between(
        self,
        key: str,
        min: str | int,
        max: str | int,
        is_datetime_value: bool = False,
        aggregation: str = "",
    ) -> dict:
        pass

    @abstractmethod
    def any(self, key: str, inner_exact_matches: dict = {}) -> dict:
        pass

    @abstractmethod
    def none(self, key: str) -> dict:
        pass

    @abstractmethod
    def geo(self, key: str, value: dict) -> dict:
        pass


class MatcherContext:
    def __init__(self, collection, type_name: str, force_base):
        self.collection = collection
        self.type_name = type_name
        self.force_base = force_base
        self.tokens = {}

    def __enter__(self):
        # Set values for THIS THREAD only
        self.tokens["collection"] = _collection_ctx.set(self.collection)
        self.tokens["type"] = _type_ctx.set(self.type_name)
        self.tokens["force_base"] = _force_base_ctx.set(self.force_base)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Reset values to previous state (clean up)
        _collection_ctx.reset(self.tokens["collection"])
        _type_ctx.reset(self.tokens["type"])
        _force_base_ctx.reset(self.tokens["force_base"])
