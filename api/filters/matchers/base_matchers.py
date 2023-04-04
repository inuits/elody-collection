from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    @abstractmethod
    def id(self, key: str, values: list[str]) -> dict | str:
        pass

    @abstractmethod
    def exact(self, key: str, value: str, parent_key: str = "") -> dict | str:
        pass

    @abstractmethod
    def contains(self, key: str, value: str, parent_key: str = "") -> dict | str:
        pass

    @abstractmethod
    def after(self, key: str, value: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def before(self, key: str, value: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def after_or_equal(self, key: str, value: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def before_or_equal(self, key: str, value: str, parent_key: str) -> dict | str:
        pass

    @abstractmethod
    def in_between(
        self, key: str, after: str | int, before: str | int, parent_key: str
    ) -> dict | str:
        pass

    @abstractmethod
    def any(self, key: str) -> dict | str:
        pass

    @abstractmethod
    def none(self, key: str) -> dict | str:
        pass
