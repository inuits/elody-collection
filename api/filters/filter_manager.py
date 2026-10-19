from config_access import config
from elody.util import Singleton
from filters.arango_filters import ArangoFilters
from filters.mongo_filters import MongoFilters


class FilterManager(metaclass=Singleton):
    def __init__(self):
        self.__init_filter_engines()

    def __init_filter_engines(self):
        self.filter_engine = {
            "arango": ArangoFilters,
            "mongo": MongoFilters,
        }.get(config["DB_ENGINE"])()

    def get_filter_engine(self):
        return self.filter_engine
