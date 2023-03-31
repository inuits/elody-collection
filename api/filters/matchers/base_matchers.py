from abc import ABC, abstractmethod


class BaseMatchers(ABC):
    @abstractmethod
    def exact_match(self, filter_request_body: dict):
        pass
