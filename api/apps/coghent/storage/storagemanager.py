from apps.coghent.storage.coghentarangostore import CoghentArangoStorageManager
from singleton import Singleton
from storage.storagemanager import StorageManager


class CoghentStorageManager(StorageManager, metaclass=Singleton):
    def _init_storage_managers(self):
        if self.storage_engine == "arango":
            self.storage_manager = CoghentArangoStorageManager()
        else:
            self.storage_manager = None
