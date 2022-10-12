from apps.coghent.storage.arangostore import CoghentArangoStorageManager
from singleton import Singleton
from storage.storagemanager import StorageManager


class CoghentStorageManager(StorageManager, metaclass=Singleton):
    def _init_storage_managers(self):
        self.storage_manager = {"arango": CoghentArangoStorageManager}.get(
            self.storage_engine
        )()
