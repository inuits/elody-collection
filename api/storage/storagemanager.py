from elody.util import Singleton
from os import getenv
from configuration import get_storage_mapper


class StorageManager(metaclass=Singleton):
    def __init__(self):
        self.storage_engine = getenv("DB_ENGINE", "arango")
        self._init_storage_managers()

    def _init_storage_managers(self):
        # Layers halen uit configuration.py
        self.storage_manager = get_storage_mapper().get(self.storage_engine)()

    def get_db_engine(self):
        return self.storage_manager
