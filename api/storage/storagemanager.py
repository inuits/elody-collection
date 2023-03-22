import os

from storage.arangostore import ArangoStorageManager
from storage.memorystore import MemoryStorageManager
from storage.mongostore import MongoStorageManager
from util import Singleton


class StorageManager(metaclass=Singleton):
    def __init__(self):
        self.storage_engine = os.getenv("DB_ENGINE", "arango")
        self._init_storage_managers()

    def _init_storage_managers(self):
        self.storage_manager = {
            "arango": ArangoStorageManager,
            "memory": MemoryStorageManager,
            "mongo": MongoStorageManager,
        }.get(self.storage_engine)()

    def get_db_engine(self):
        return self.storage_manager
