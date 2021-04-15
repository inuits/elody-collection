import unittest
import json

from tests.base_case import BaseCase


class EntityTest(BaseCase):
    def test_successful_entity_create(self):
        response = self.create_entity()

        self.assertEqual(str, type(response.json["_id"]))
        self.assertEqual(str, type(response.json["metadata"][0]["value"]))
        self.assertEqual(str, type(response.json["type"]))
        self.assertEqual(list, type(response.json["metadata"]))
        self.assertEqual(2, len(response.json["identifiers"]))
        self.assertEqual(4, len(response.json["metadata"]))
        self.assertEqual(201, response.status_code)

    def test_invalid_input_entity_create(self):
        entity = "<entity><title>Schilderij</title><entity>"

        response = self.app.post(
            "/entities", headers={"Content-Type": "application/json"}, data=entity
        )

        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Invalid input", response.json["message"])
        self.assertEqual(405, response.status_code)

    def test_invalid_content_type_entity_create(self):
        response = self.app.post(
            "/entities",
            headers={"Content-Type": "multipart/form-data"},
            data=self.entity,
        )

        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Invalid input", response.json["message"])
        self.assertEqual(405, response.status_code)

    def test_successful_entity_get(self):
        _id = self.create_entity_get_id()

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

    def test_successful_entity_list_get(self):
        _id1 = self.create_entity_get_id()
        _id2 = self.create_entity_get_id()

        response = self.app.get(
            "/entities", headers={"Content-Type": "application/json"}
        )

        self.assertEqual(2, response.json["count"])
        self.assertEqual(20, response.json["limit"])
        self.assertEqual(2, len(response.json["results"]))
        self.assertFalse("next" in response.json)
        self.assertFalse("previous" in response.json)
        self.assertEqual(
            "Een schilderij", response.json["results"][0]["metadata"][0]["value"]
        )
        self.assertEqual(
            "Een schilderij", response.json["results"][1]["metadata"][0]["value"]
        )
        self.assertEqual(200, response.status_code)

    def test_successful_entity_list_get_pagination(self):
        skip = 0
        limit = 20
        count = 40

        ids = list()
        for i in range(count):
            ids.append(self.create_entity_get_id())

        response = self.app.get(
            "/entities", headers={"Content-Type": "application/json"}
        )

        self.assertEqual(count, response.json["count"])
        self.assertEqual(limit, response.json["limit"])
        self.assertEqual(limit, len(response.json["results"]))
        self.assertTrue("next" in response.json)
        self.assertFalse("previous" in response.json)
        for i in range(limit):
            entity = response.json["results"][i]
            self.assertEqual("Een schilderij", entity["metadata"][0]["value"])
            self.assertEqual(ids[i], entity["_id"])
        self.assertEqual(200, response.status_code)

    def test_successful_entity_list_get_pagination_query_params(self):
        skip = 10
        limit = 15
        count = 40

        ids = list()
        for i in range(count):
            ids.append(self.create_entity_get_id())

        response = self.app.get(
            "/entities?skip={}&limit={}".format(skip, limit),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(count, response.json["count"])
        self.assertEqual(limit, response.json["limit"])
        self.assertEqual(limit, len(response.json["results"]))
        self.assertTrue("next" in response.json)
        self.assertTrue("previous" in response.json)
        print(ids)
        for i in range(limit):
            entity = response.json["results"][i]
            self.assertEqual("Een schilderij", entity["metadata"][0]["value"])
            print(entity["_id"])
            self.assertEqual(ids[i + skip], entity["_id"])
        self.assertEqual(200, response.status_code)

    def test_successful_entity_metadata_get(self):
        _id = self.create_entity_get_id()

        response = self.app.get(
            "/entities/{}/metadata".format(_id),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(4, len(response.json))
        self.assertEqual(str, type(response.json[0]["value"]))
        self.assertEqual("Een schilderij", response.json[0]["value"])
        self.assertEqual(200, response.status_code)

    def test_successful_entity_metadata_key_get(self):
        _id = self.create_entity_get_id()

        response = self.app.get(
            "/entities/{}/metadata/title".format(_id),
            headers={"Content-Type": "application/json"},
        )

        print(response.json)
        self.assertEqual(2, len(response.json))
        self.assertEqual(str, type(response.json[0]["value"]))
        self.assertEqual("Een schilderij", response.json[0]["value"])
        self.assertEqual(200, response.status_code)

    def test_successful_entity_put(self):
        _id = self.create_entity_get_id()

        update = json.dumps(
            {
                "_id": _id,
                "identifiers": ["2021"],
                "type": "entity",
                "metadata": [{"key": "title", "value": "Update", "lang": "nl"}],
            }
        )

        response = self.app.put(
            "/entities/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.assertEqual(str, type(response.json["_id"]))
        self.assertEqual(str, type(response.json["metadata"][0]["value"]))
        self.assertEqual("Update", response.json["metadata"][0]["value"])
        self.assertEqual(str, type(response.json["type"]))
        self.assertEqual(list, type(response.json["metadata"]))
        self.assertEqual(1, len(response.json["identifiers"]))
        self.assertEqual(1, len(response.json["metadata"]))
        self.assertEqual(201, response.status_code)

    def test_successful_entity_patch(self):
        response = self.create_entity()
        _id = response.json["_id"]

        self.assertEqual(str, type(response.json["metadata"][0]["value"]))
        self.assertEqual("Een schilderij", response.json["metadata"][0]["value"])
        self.assertEqual(str, type(response.json["type"]))
        self.assertEqual(list, type(response.json["metadata"]))
        self.assertEqual(2, len(response.json["identifiers"]))
        self.assertEqual(4, len(response.json["metadata"]))
        self.assertEqual(201, response.status_code)

        update = json.dumps(
            {
                "metadata": [{"key": "title", "value": "Update", "lang": "nl"}],
            }
        )

        response = self.app.patch(
            "/entities/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.assertEqual(str, type(response.json["metadata"][0]["value"]))
        self.assertEqual("Update", response.json["metadata"][0]["value"])
        self.assertEqual(str, type(response.json["type"]))
        self.assertEqual(list, type(response.json["metadata"]))
        self.assertEqual(2, len(response.json["identifiers"]))
        self.assertEqual(1, len(response.json["metadata"]))
        self.assertEqual(201, response.status_code)

    def test_successful_entity_delete(self):
        _id = self.create_entity_get_id()

        response = self.app.delete(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)
