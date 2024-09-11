from storage.genericstore import GenericStorageManager


class HttpStorageManager(GenericStorageManager):
    def __init__(self):
        self.base_url = ""

    def get_items_from_collection(
        self,
        collection,
        skip=0,
        limit=20,
        fields=None,
        filters=[],
        sort=None,
        asc=True,
    ):
        pass

    def get_item_from_collection_by_id(self, collection, id):
        pass
