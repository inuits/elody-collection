import os
from dotenv import load_dotenv
from storage.arangostore import ArangoStorageManager
from storage.mongostore import MongoStorageManager

db_engines = {
    'mongo': MongoStorageManager(),
    'arango': ArangoStorageManager()
}

class StorageManager:
    def __init__(self):
        self.storage_engine = os.getenv('DB_ENGINE')

    def get_db_engine(self):
        return db_engines[self.storage_engine]
