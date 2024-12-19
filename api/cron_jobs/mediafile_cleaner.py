import logging

from os import getenv
from elody.util import get_raw_id, signal_mediafile_deleted
from rabbit import get_rabbit
from storage.storagemanager import StorageManager


class MediafileCleaner:
    def __init__(self):
        self.delete_empty_mediafiles: bool = getenv("DELETE_EMPTY_MEDIAFILES", True)
        self.storage = StorageManager().get_db_engine()

    def __call__(self):
        if self.delete_empty_mediafiles:
            mediafiles = self.storage.get_empty_mediafiles_with_no_relations()
            for mediafile in mediafiles:
                mediafile_id = get_raw_id(mediafile)
                linked_entities = self.storage.delete_item_from_collection(
                    "mediafiles", mediafile_id
                )
                signal_mediafile_deleted(get_rabbit(), mediafile, linked_entities)
                logging.info(f"DELETED MEDIAFILE WITH ID: {mediafile_id}")
