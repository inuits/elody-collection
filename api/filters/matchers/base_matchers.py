from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    separator = "|"

    @abstractmethod
    def id(self, key: str, value: str):
        pass

    @abstractmethod
    def exact(self, key: str, value: str, parent_key: str = ""):
        pass

    @abstractmethod
    def contains(self, key: str, value: str, parent_key: str = ""):
        pass

    @abstractmethod
    def any(self, key: str):
        pass

    @abstractmethod
    def none(self, key: str):
        pass
