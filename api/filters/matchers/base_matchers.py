import app

from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    datetime_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$"
    collection = "entities"

    @staticmethod
    def get_document_key_value(parent_key: str) -> tuple[str, str]:
        config = app.object_configuration_mapper.get(BaseMatchers.collection)
        document_key_value = config.filtering()["parent_keys"][parent_key]
        return document_key_value["key"], document_key_value["value"]

    @abstractmethod
    def id(self, key: str, values: list[str], parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def exact(
        self,
        key: str,
        value: str | int | float | bool | list[str],
        parent_key: str = "",
        is_datetime_value: bool = False,
    ) -> dict | str:
        pass

    @abstractmethod
    def contains(self, key: str, value: str, parent_key: str = "") -> dict | str:
        pass

    @abstractmethod
    def min(
        self,
        key: str,
        value: str | int,
        parent_key: str,
        is_datetime_value: bool = False,
    ) -> dict | str:
        pass

    @abstractmethod
    def max(
        self,
        key: str,
        value: str | int,
        parent_key: str,
        is_datetime_value: bool = False,
    ) -> dict | str:
        pass

    @abstractmethod
    def min_included(
        self,
        key: str,
        value: str | int,
        parent_key: str,
        is_datetime_value: bool = False,
    ) -> dict | str:
        pass

    @abstractmethod
    def max_included(
        self,
        key: str,
        value: str | int,
        parent_key: str,
        is_datetime_value: bool = False,
    ) -> dict | str:
        pass

    @abstractmethod
    def in_between(
        self,
        key: str,
        min: str | int,
        max: str | int,
        parent_key: str,
        is_datetime_value: bool = False,
    ) -> dict | str:
        pass

    @abstractmethod
    def any(self, key: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def none(self, key: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def metadata_on_relation(
        self,
        key: str,
        value: str | int | float | bool,
        parent_key: str,
        match_exact: bool,
    ) -> dict | str:
        pass
