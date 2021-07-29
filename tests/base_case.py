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

    mediafile = json.dumps(
        {
            "identifiers": ["12345", "abcde"],
            "location": "http://dams-storage.inuits.io/download/test.jpg",
        }
    )

    tenant = json.dumps(
        {
            "identifiers": ["12345", "abcde"],
            "type": "tenant",
            "name": "Een museum",
        }
    )

    filename = json.dumps(
        {
            "filename": "test.jpg",
        }
    )

    def setUp(self):
        app.testing = True

        self.app = app.test_client()
        self.storage = StorageManager().get_db_engine()
        self.addCleanup(self.storage.drop_all_collections)

    def create_entity(self):
        return self.app.post(
            "/entities", headers={"Content-Type": "application/json"}, data=self.entity
        )

    def create_entity_get_id(self):
        return self.create_entity().json["_id"]

    def create_mediafile(self):
        return self.app.post(
            "/mediafiles",
            headers={"Content-Type": "application/json"},
            data=self.mediafile,
        )

    def create_mediafile_get_id(self):
        return self.create_mediafile().json["_id"]

    def create_tenant(self):
        return self.app.post(
            "/tenants",
            headers={"Content-Type": "application/json"},
            data=self.tenant,
        )

    def create_tenant_get_id(self):
        return self.create_tenant().json["_id"]

    def valid_mediafile(self, mediafile):
        self.assertEqual(str, type(mediafile["_id"]))
        self.assertEqual(str, type(mediafile["location"]))

    def valid_tenant(self, tenant):
        self.assertEqual(str, type(tenant["_id"]))
        self.assertEqual(str, type(tenant["name"]))
        self.assertEqual("Een museum", tenant["name"])
        self.assertEqual(str, type(tenant["type"]))
        self.assertEqual("tenant", tenant["type"])

    def invalid_input(self, response):
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Invalid input", response.json["message"])
        self.assertEqual(405, response.status_code)

    def not_found(self, response):
        self.assertEqual(1, len(response.json))
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual(404, response.status_code)
