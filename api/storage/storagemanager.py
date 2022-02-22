import os

from storage.arangostore import ArangoStorageManager
from storage.memorystore import MemoryStorageManager
from storage.mongostore import MongoStorageManager
from singleton import Singleton

class StorageManager(metaclass=Singleton):
    def __init__(self):
        self.storage_engine = os.getenv("DB_ENGINE", "arango")
        self.__init_storage_managers()

    def get_db_engine(self):
        return self.storage_manager

    def __init_storage_managers(self):
        if self.storage_engine == "arango":
            self.storage_manager = ArangoStorageManager()
        elif self.storage_engine == "memory":
            self.storage_manager = MemoryStorageManager()
        elif self.storage_engine == "mongo":
            self.storage_manager = MongoStorageManager()
