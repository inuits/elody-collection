from abc import ABC, abstractmethod


class BaseObjectConfiguration(ABC):
    @abstractmethod
    def logging(self, _):
        pass

    @abstractmethod
    def migration(self):
        pass

    @abstractmethod
    def serialization(self, from_format, to_format):  # pyright: ignore
        pass

    @abstractmethod
    def validation(self):
        pass
