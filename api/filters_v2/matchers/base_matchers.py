import app

from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    datetime_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$"
    collection = "entities"

    @staticmethod
    def get_object_lists() -> dict:
        config = app.object_configuration_mapper.get(BaseMatchers.collection)
        return config.document_info()["object_lists"]

    @abstractmethod
    def exact(
        self,
        key: str,
        value: str | int | float | bool | list[str],
        is_datetime_value: bool = False,
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
    ) -> dict:
        pass

    @abstractmethod
    def max(
        self,
        key: str,
        value: str | int,
        is_datetime_value: bool = False,
    ) -> dict:
        pass

    @abstractmethod
    def min_included(
        self,
        key: str,
        value: str | int,
        is_datetime_value: bool = False,
    ) -> dict:
        pass

    @abstractmethod
    def max_included(
        self,
        key: str,
        value: str | int,
        is_datetime_value: bool = False,
    ) -> dict:
        pass

    @abstractmethod
    def in_between(
        self,
        key: str,
        min: str | int,
        max: str | int,
        is_datetime_value: bool = False,
    ) -> dict:
        pass

    @abstractmethod
    def any(self, key: str) -> dict:
        pass

    @abstractmethod
    def none(self, key: str) -> dict:
        pass
