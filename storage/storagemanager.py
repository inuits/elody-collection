import os
from dotenv import load_dotenv
from storage.arangostore import ArangoStorageManager
from storage.mongostore import MongoStorageManager
from storage.memorystore import MemoryStorageManager


class StorageManager:
    def __init__(self):
        self.storage_engine = os.getenv("DB_ENGINE")

    def get_db_engine(self):
        if self.storage_engine == "mongo":
            return MongoStorageManager()
        elif self.storage_engine == "arango":
            return ArangoStorageManager()
        elif self.storage_engine == "memory":
            return MemoryStorageManager()
