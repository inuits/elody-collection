import os

from elody.util import Singleton
from filters_v2.mongo_filters import MongoFilters


class FilterManager(metaclass=Singleton):
    def __init__(self):
        self.__init_filter_engines()

    def __init_filter_engines(self):
        self.filter_engine = {
            "mongo": MongoFilters,
        }.get(os.getenv("DB_ENGINE", "mongo"))()

    def get_filter_engine(self):
        return self.filter_engine
