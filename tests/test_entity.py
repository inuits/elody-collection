import unittest
import json

from tests.base_case import BaseCase


class EntityTest(BaseCase):
    def test_successful_entity_create(self):
        response = self.create_asset()

        self.assertEqual(str, type(response.json["_id"]))
        self.assertEqual(str, type(response.json["metadata"][0]["value"]))
        self.assertEqual(str, type(response.json["type"]))
        self.assertEqual(list, type(response.json["metadata"]))
        self.assertEqual(2, len(response.json["identifiers"]))
        self.assertEqual(4, len(response.json["metadata"]))
        self.assertEqual(201, response.status_code)

    def test_succesful_entity_get(self):
        _id = self.create_asset_get_id()

        response = self.app.get(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertEqual(str, type(response.json["_id"]))
        self.assertEqual(str, type(response.json["metadata"][0]["value"]))
        self.assertEqual(str, type(response.json["type"]))
        self.assertEqual(list, type(response.json["metadata"]))
        self.assertEqual(2, len(response.json["identifiers"]))
        self.assertEqual(4, len(response.json["metadata"]))
        self.assertEqual(200, response.status_code)

    def test_non_existant_entity_get(self):
        _id = "non-existant-id"

        response = self.app.get(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertEqual(1, len(response.json))
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual(404, response.status_code)

    def test_successful_entity_delete(self):
        _id = self.create_asset_get_id()

        response = self.app.delete(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)
