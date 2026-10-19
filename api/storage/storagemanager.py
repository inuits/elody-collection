from config_access import config
from configuration import get_storage_mapper
from elody.util import Singleton


class StorageManager(metaclass=Singleton):
    def __init__(self):
        self.storage_engine = config["DB_ENGINE"]
        self._init_storage_managers()

    def _init_storage_managers(self):
        # Layers halen uit configuration.py
        self.storage_manager = get_storage_mapper().get(self.storage_engine)()

    def get_db_engine(self):
        return self.storage_manager
