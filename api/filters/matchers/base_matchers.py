from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    @abstractmethod
    def id(self, key: str, values: list[str]) -> dict | str:
        pass

    @abstractmethod
    def exact(
        self, key: str, value: str | int | bool | list[str], parent_key: str = ""
    ) -> dict | str:
        pass

    @abstractmethod
    def contains(self, key: str, value: str, parent_key: str = "") -> dict | str:
        pass

    @abstractmethod
    def min(self, key: str | list[str], value: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def max(self, key: str | list[str], value: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def min_included(
        self, key: str | list[str], value: str, parent_key: str
    ) -> dict | str:
        pass

    @abstractmethod
    def max_included(
        self, key: str | list[str], value: str, parent_key: str
    ) -> dict | str:
        pass

    @abstractmethod
    def in_between(
        self, key: str, min: str | int, max: str | int, parent_key: str
    ) -> dict | str:
        pass

    @abstractmethod
    def any(self, key: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def none(self, key: str, parent_key: str) -> dict | str:
        pass
