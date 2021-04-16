import unittest
import json

from app import app
from storage.storagemanager import StorageManager


class BaseCase(unittest.TestCase):

    def setUp(self):
        app.testing = True

        self.app = app.test_client()
        self.storage = StorageManager().get_db_engine()

    def tearDown(self):
        self.storage.drop_all_collections()

    def invalid_input(self, response):
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Invalid input", response.json["message"])
        self.assertEqual(405, response.status_code)

    def not_found(self, response):
        self.assertEqual(1, len(response.json))
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual(404, response.status_code)
