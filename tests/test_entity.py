import unittest
import json

from tests.base_case import BaseCase


class EntityTest(BaseCase):
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

    def test_successful_entity_create(self):
        response = self.create_entity()

        self.valid_entity(response.json, 2, 4)
        self.assertEqual(201, response.status_code)

    def test_invalid_input_entity_create(self):
        entity = "<entity><title>Schilderij</title><entity>"

        response = self.app.post(
            "/entities", headers={"content-type": "application/json"}, data=entity
        )

        self.invalid_input(response)

    def test_invalid_content_type_entity_create(self):
        response = self.app.post(
            "/entities",
            headers={"Content-Type": "multipart/form-data"},
            data=self.entity,
        )

        self.invalid_input(response)

    def test_successful_entity_get(self):
        _id = self.create_entity_get_id()

        response = self.app.get(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.valid_entity(response.json, 2, 4)
        self.assertEqual(200, response.status_code)

    def test_non_existant_entity_get(self):
        response = self.app.get(
            "/entities/non-existant-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)

    def test_successful_entity_list_get(self):
        self.entity_list(2, 20, 0)

    def test_successful_entity_list_get_pagination(self):
        self.entity_list(40, 20, 0)

    def test_successful_entity_list_get_pagination_query_params(self):
        self.entity_list(40, 15, 10)

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

        self.assertEqual(2, len(response.json))
        self.assertEqual(str, type(response.json[0]["value"]))
        self.assertEqual("Een schilderij", response.json[0]["value"])
        self.assertEqual(200, response.status_code)

    def test_successful_entity_put(self):
        response = self.create_entity()
        _id = response.json["_id"]

        self.valid_entity(response.json, 2, 4)
        self.assertEqual(201, response.status_code)

        update = json.dumps(
            {
                "_id": _id,
                "identifiers": ["2021"],
                "type": "entity",
                "metadata": [{"key": "title", "value": "Een schilderij", "lang": "nl"}],
            }
        )

        response = self.app.put(
            "/entities/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.valid_entity(response.json, 1, 1)
        self.assertEqual(201, response.status_code)

    def test_successful_entity_patch(self):
        response = self.create_entity()
        _id = response.json["_id"]

        self.valid_entity(response.json, 2, 4)
        self.assertEqual(201, response.status_code)

        update = json.dumps(
            {
                "metadata": [{"key": "title", "value": "Een schilderij", "lang": "nl"}],
            }
        )

        response = self.app.patch(
            "/entities/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.valid_entity(response.json, 2, 1)
        self.assertEqual(201, response.status_code)

    def test_successful_entity_delete(self):
        _id = self.create_entity_get_id()

        response = self.app.delete(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)

    def create_entity(self):
        return self.app.post(
            "/entities", headers={"Content-Type": "application/json"}, data=self.entity
        )

    def create_entity_get_id(self):
        return self.create_entity().json["_id"]

    def valid_entity(self, entity, identifier_count, metadata_count):
        self.assertEqual(str, type(entity["_id"]))
        self.assertEqual(str, type(entity["metadata"][0]["value"]))
        self.assertEqual(str, type(entity["type"]))
        self.assertEqual(list, type(entity["metadata"]))
        self.assertEqual("Een schilderij", entity["metadata"][0]["value"])
        self.assertEqual(identifier_count, len(entity["identifiers"]))
        self.assertEqual(metadata_count, len(entity["metadata"]))

    def entity_list(self, count, limit, skip):
        ids = list()
        for i in range(count):
            ids.append(self.create_entity_get_id())

        response = self.app.get(
            "/entities?skip={}&limit={}".format(skip, limit), headers={"Content-Type": "application/json"}
        )

        self.assertEqual(count, response.json["count"])
        self.assertEqual(limit, response.json["limit"])
        self.assertEqual(min(count, limit), len(response.json["results"]))
        self.assertEqual(skip + limit < count, "next" in response.json)
        self.assertEqual(skip > 0, "previous" in response.json)
        for i in range(min(count, limit)):
            entity = response.json["results"][i]
            self.assertEqual(ids[i + skip], entity["_id"])
            self.valid_entity(entity, 2, 4)
        self.assertEqual(200, response.status_code)
