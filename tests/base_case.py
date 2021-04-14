import unittest
import json

from app import app
from storage.storagemanager import StorageManager


class BaseCase(unittest.TestCase):
    asset = json.dumps(
        {
            "identifiers": ["12345", "abcde"],
            "type": "asset",
            "metadata": [
                {"key": "title", "value": "Een schilderij", "lang": "nl"},
                {"key": "title", "value": "A painting", "lang": "en"},
                {
                    "key": "description",
                    "value": "Beschrijving van een schilderij",
                    "lang": "nl",
                },
                {
                    "key": "description",
                    "value": "Description of a painting",
                    "lang": "en",
                },
            ],
        }
    )

    def setUp(self):
        app.testing = True

        self.app = app.test_client()
        self.storage = StorageManager().get_db_engine()

    def tearDown(self):
        self.storage.drop_all_collections()

    def create_asset(self):
        return self.app.post(
            "/entities", headers={"Content-Type": "application/json"}, data=self.asset
        )

    def create_asset_get_id(self):
        return self.create_asset().json["_id"]
