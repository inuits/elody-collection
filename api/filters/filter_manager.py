import os

from filters.arango_filters import ArangoFilters
from filters.mongo_filters import MongoFilters
from util import Singleton


class FilterManager(metaclass=Singleton):
    def __init__(self):
        self._init_filter_engines()

    def get_filter_engine(self):
        return self.filter_engine

    def _init_filter_engines(self):
        self.filter_engine = {
            "arango": ArangoFilters,
            "mongo": MongoFilters,
        }.get(os.getenv("DB_ENGINE", "arango"))()
