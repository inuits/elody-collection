import unittest
import json

from app import app
from storage.storagemanager import StorageManager


class BaseCase(unittest.TestCase):
    entity = json.dumps(
        {
            "identifiers": ["12345", "abcde"],
            "type": "entity",
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

    mediafile = json.dumps(
        {
            "location": "http://dams-storage.inuits.io/download/test.jpg"
        }
    )

    def setUp(self):
        app.testing = True

        self.app = app.test_client()
        self.storage = StorageManager().get_db_engine()

    def tearDown(self):
        self.storage.drop_all_collections()

    def create_entity(self):
        return self.app.post(
            "/entities", headers={"Content-Type": "application/json"}, data=self.entity
        )

    def create_entity_get_id(self):
        return self.create_entity().json["_id"]

    def create_mediafile(self):
        return self.app.post(
            "/mediafiles", headers={"Content-Type": "application/json"}, data=self.mediafile
        )

    def invalid_input(self, response):
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Invalid input", response.json["message"])
        self.assertEqual(405, response.status_code)

    def not_found(self, response):
        self.assertEqual(1, len(response.json))
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual(404, response.status_code)
