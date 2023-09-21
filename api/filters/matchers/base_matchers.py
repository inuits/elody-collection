from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    datetime_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$"

    @staticmethod
    def get_document_key_value(parent_key: str) -> tuple[str, str]:
        document_key = "type" if parent_key == "relations" else "key"
        document_value = "key" if parent_key == "relations" else "value"
        return document_key, document_value

    @abstractmethod
    def id(self, key: str, values: list[str], parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def exact(
        self,
        key: str,
        value: str | int | bool | list[str],
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
