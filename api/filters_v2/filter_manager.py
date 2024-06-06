from elody.util import Singleton
from filters.arango_filters import ArangoFilters
from filters_v2.mongo_filters import MongoFilters
from os import getenv


class FilterManager(metaclass=Singleton):
    def __init__(self):
        self.__init_filter_engines()

    def __init_filter_engines(self):
        filter_engine = {"mongo": MongoFilters, "arango": ArangoFilters}.get(
            getenv("DB_ENGINE", "mongo"), None
        )
        if filter_engine:
            self.filter_engine = filter_engine()

    def get_filter_engine(self):
        return self.filter_engine
