from abc import ABC, abstractmethod
from configuration import get_object_configuration_mapper


class BaseMatchers(ABC):
    collection = "entities"
    force_base_nested_matcher_builder = False
    type = ""

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
        return config.document_info()["object_lists"]

    @abstractmethod
    def exact(
        self,
        key: str,
        value: str | int | float | bool | list[str],
        is_datetime_value: bool = False,
        aggregation: str = "",
    ) -> dict:
        pass

    @abstractmethod
    def contains(self, key: str, value: str) -> dict:
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
    def any(self, key: str) -> dict:
        pass

    @abstractmethod
    def none(self, key: str) -> dict:
        pass
