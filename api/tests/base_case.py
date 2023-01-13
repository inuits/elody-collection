import json
import unittest

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

    invalid_entity = json.dumps(
        {
            "identifiers": "123",
            "metadata": "title",
            "data": "test",
        }
    )

    mediafile = json.dumps(
        {
            "identifiers": ["12345", "abcde"],
            "filename": "test.jpg",
            "original_file_location": "http://dams-storage.inuits.io/download/test.jpg",
        }
    )

    invalid_mediafile = json.dumps(
        {
            "identifiers": "12345",
            "original_file_location": [
                "http://dams-storage.inuits.io/download/test.jpg"
            ],
        }
    )

    filename = json.dumps(
        {
            "filename": "test.jpg",
        }
    )

    filename_with_metadata = json.dumps(
        {
            "filename": "test.jpg",
            "metadata": [
                {
                    "key": "rights",
                    "value": "CC-BY-4.0",
                    "lang": "en",
                },
                {
                    "key": "copyright",
                    "value": "Inuits",
                    "lang": "en",
                },
            ],
        }
    )

    def setUp(self):
        app.testing = True

        self.app = app.test_client()
        self.addCleanup(StorageManager().get_db_engine().drop_all_collections)

    def create_entity(self):
        return self.app.post(
            "/entities", headers={"Content-Type": "application/json"}, data=self.entity
        )

    def create_entity_get_id(self):
        return self._get_raw_id(self.create_entity().json)

    def create_mediafile(self):
        return self.app.post(
            "/mediafiles",
            headers={"Content-Type": "application/json"},
            data=self.mediafile,
        )

    def create_mediafile_get_id(self):
        return self._get_raw_id(self.create_mediafile().json)

    def _get_raw_id(self, item):
        return item.get("_key", item["_id"])

    def valid_mediafile(self, mediafile):
        self.assertEqual(str, type(self._get_raw_id(mediafile)))
        self.assertEqual(str, type(mediafile["filename"]))

    def invalid_input(self, response):
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Invalid input", response.json["message"])
        self.assertEqual(405, response.status_code)

    def not_found(self, response):
        self.assertEqual(1, len(response.json))
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual(404, response.status_code)
