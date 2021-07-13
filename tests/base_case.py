import json
import os
import unittest

from app import app
from resources import importer
from storage.storagemanager import StorageManager
from unittest.mock import patch


def mocked_send(
    body,
    routing_key,
    exchange_name=None,
    exchange_type=None,
    headers=None,
    log_flag=None,
):
    if routing_key == "dams.import_start":
        importer.csv_import(body)
    return


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
            "type": "mediafile",
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

    def setUp(self):
        app.testing = True

        self.app = app.test_client()
        self.storage = StorageManager().get_db_engine()
        self.patcher = patch("app.ramq")
        self.mocked_ramq = self.patcher.start()
        self.mocked_ramq.send = mocked_send
        self.upload_folder = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "csv"
        )
        os.environ["UPLOAD_FOLDER"] = self.upload_folder

    def tearDown(self):
        self.storage.drop_all_collections()
        self.patcher.stop()

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
        self.assertEqual(str, type(mediafile["type"]))
        self.assertEqual("mediafile", mediafile["type"])

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
