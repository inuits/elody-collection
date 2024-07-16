import os
import logging
from datetime import datetime, timezone
from elody.util import get_item_metadata_value, get_raw_id
from storage.storagemanager import StorageManager


class TtlChecker:
    def __init__(self):
        self.hard_delete: bool = os.getenv("HARD_DELETE", False)
        self.storage = StorageManager().get_db_engine()

    def __call__(self):
        collections = self.storage.get_existing_collections()
        for collection in collections:
            logging.info(f"DELETE ITEMS IN COLLECTION: {collection}")
            count = self.storage.count_items_from_collection(collection)
            # Fetch items in batches
            batch_size = 20
            for i in range(0, count, batch_size):
                result = self.storage.get_items_from_collection(
                    collection,
                    skip=i,
                    limit=min(batch_size, count - i),
                )
                items = result["results"]
                for item in items:
                    ttl = get_item_metadata_value(item, "ttl")
                    if ttl and self._is_expired(ttl):
                        item_id = get_raw_id(item)
                        logging.info(f"DELETE ITEM WITH ID: {item_id}")
                        self.storage.delete_item_from_collection(collection, item_id)
                        if self.hard_delete in [True, "True", "true"]:
                            self._delete_history_of_item(item_id, collection)

    def _delete_history_of_item(self, item_id, collection):
        history_items = self.storage.get_history_for_item(
            collection, item_id, all_entries=True
        )
        for history_item in history_items:
            history_item_id = get_raw_id(history_item)
            logging.info(f"DELETE ITEM FROM HISTORY WITH ID: {history_item_id}")
            self.storage.delete_item_from_collection("history", history_item_id)

    def _is_expired(self, ttl: float):
        return datetime.now(tz=timezone.utc).timestamp() >= float(ttl)
