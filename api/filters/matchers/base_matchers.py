from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    @abstractmethod
    def case_insensitive(self, key: str, value: str, sub_key: str = ""):
        pass
