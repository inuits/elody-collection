import json
import os

from tests.base_case import BaseCase
from unittest.mock import patch, MagicMock


@patch("app.rabbit", new=MagicMock())
class EntityTest(BaseCase):
    def test_successful_entity_create(self):
        response = self.create_entity()

        self.valid_entity(response.json, 3, 4)
        self.assertEqual(201, response.status_code)

    def test_successful_entity_metadata_create(self):
        _id = self.create_entity_get_id()

        metadata = json.dumps(
            [
                {
                    "key": "type",
                    "value": "schilderij",
                    "lang": "nl",
                }
            ]
        )

        response = self.app.post(
            "/entities/{}/metadata".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=metadata,
        )

        self.assertEqual(1, len(response.json))
        self.assertEqual(list, type(response.json))
        for key in response.json[0]:
            self.assertEqual(str, type(response.json[0][key]))
        self.assertEqual("schilderij", response.json[0]["value"])
        self.assertEqual(201, response.status_code)

    def test_non_existent_entity_metadata_create(self):
        metadata = json.dumps(
            {
                "key": "type",
                "value": "schilderij",
                "lang": "nl",
            }
        )

        response = self.app.post(
            "/entities/non-existent-id/metadata",
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=metadata,
        )

        self.not_found(response)

    def test_invalid_input_entity_create(self):
        entity = "<entity><title>Schilderij</title><entity>"

        response = self.app.post(
            "/entities",
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=entity,
        )

        self.invalid_input(
            response,
            "The browser (or proxy) sent a request that this server could not understand.",
            400,
        )

    def test_invalid_content_type_entity_create(self):
        response = self.app.post(
            "/entities",
            headers={**self.headers, **{"Content-Type": "multipart/form-data"}},
            data=self.entity,
        )

        self.invalid_input(
            response,
            "Did not attempt to load JSON data because the request Content-Type was not 'application/json'.",
        )

    def test_invalid_entity_create(self):
        response = self.app.post(
            "/entities",
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=self.invalid_entity,
        )

        self.check_invalid_entity(response)

    def test_successful_entity_mediafile_create(self):
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles/create".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=self.filename,
        )

        self.assertEqual(str, type(response.json))
        self.assertTrue(
            response.json.startswith("{}/upload/".format(os.environ["STORAGE_API_URL"]))
        )
        self.assertEqual(90, len(response.json))
        self.assertEqual(201, response.status_code)

        response = self.app.get(
            "/entities/{}/mediafiles?non_public=1".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(1, len(response.json))
        for mediafile in response.json:
            if "entities" in mediafile:
                self.assertEqual(list, type(mediafile["entities"]))
                self.assertEqual(3, len(mediafile["entities"]))
            self.valid_mediafile(mediafile)
            self.assertTrue(
                mediafile["original_file_location"].startswith(
                    "https://dams-storage-api.inuits.io/download/"
                )
            )
            self.assertTrue(mediafile["original_file_location"].endswith("test.jpg"))
            self.assertTrue(
                mediafile["thumbnail_file_location"].startswith(
                    "https://dams-image-api.inuits.io/iiif/3/"
                )
            )
            self.assertTrue(
                mediafile["thumbnail_file_location"].endswith("default.jpg")
            )

    def test_successful_entity_mediafile_create_with_metadata(self):
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles/create".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=self.filename_with_metadata,
        )

        self.assertEqual(str, type(response.json))
        self.assertTrue(
            response.json.startswith("{}/upload/".format(os.environ["STORAGE_API_URL"]))
        )
        self.assertEqual(90, len(response.json))
        self.assertEqual(201, response.status_code)

        response = self.app.get(
            "/entities/{}/mediafiles?non_public=1".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(1, len(response.json))
        for mediafile in response.json:
            if "entities" in mediafile:
                self.assertEqual(list, type(mediafile["entities"]))
                self.assertEqual(3, len(mediafile["entities"]))
            self.assertEqual(list, type(mediafile["metadata"]))
            self.assertEqual(2, len(mediafile["metadata"]))
            self.assertEqual("rights", mediafile["metadata"][0]["key"])
            self.assertEqual("Inuits", mediafile["metadata"][1]["value"])
            self.valid_mediafile(mediafile)
            self.assertTrue(
                mediafile["original_file_location"].startswith(
                    "https://dams-storage-api.inuits.io/download/"
                )
            )
            self.assertTrue(mediafile["original_file_location"].endswith("test.jpg"))
            self.assertTrue(
                mediafile["thumbnail_file_location"].startswith(
                    "https://dams-image-api.inuits.io/iiif/3/"
                )
            )
            self.assertTrue(
                mediafile["thumbnail_file_location"].endswith("default.jpg")
            )

    def test_entity_mediafile_create_without_filename(self):
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles/create".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps({}),
        )

        self.invalid_input(response, None, 400)

    def test_create_mediafile_from_non_existent_entity(self):
        response = self.app.post(
            "/entities/non-existent-id/mediafiles/create",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.not_found(response)

    def test_successful_entity_get(self):
        _id = self.create_entity_get_id()

        response = self.app.get(
            "/entities/{}".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.valid_entity(response.json, 3, 4)
        self.assertEqual(200, response.status_code)

    def test_non_existent_entity_get(self):
        response = self.app.get(
            "/entities/non-existent-id",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.not_found(response)

    def test_successful_entity_list_get(self):
        self.entity_list(2, 20, 0)

    def test_successful_entity_list_first_entities(self):
        self.entity_list(40, 20, 0)

    def test_successful_entity_list_middle_entities(self):
        self.entity_list(40, 15, 10)

    def test_successful_entity_list_last_entities(self):
        self.entity_list(40, 10, 30)

    def test_successful_entity_list_no_entities(self):
        self.entity_list(0, 20, 0)

    def test_successful_entity_ids_get(self):
        _id1 = self.create_entity_get_id()
        _id2 = self.create_entity_get_id()
        _id3 = self.create_entity_get_id()

        ids = [_id1, _id2, _id3]

        response = self.app.get(
            "/entities?ids={}".format(",".join(ids)),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(len(ids), response.json["count"])
        self.assertEqual(len(ids), len(response.json["results"]))
        returned_ids = []
        for entity in response.json["results"]:
            self.valid_entity(entity, 3, 4)
            returned_ids.append(self._get_raw_id(entity))
        self.assertListEqual(sorted(ids), sorted(returned_ids))
        self.assertEqual(200, response.status_code)

    def test_entity_ids_get_one_non_existant_id(self):
        valid_asset_count = 2
        _id1 = self.create_entity_get_id()
        _id2 = self.create_entity_get_id()
        _id3 = "non_existant_id"

        ids = [_id1, _id2, _id3]

        response = self.app.get(
            "/entities?ids={}".format(",".join(ids)),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(valid_asset_count, response.json["count"])
        self.assertEqual(valid_asset_count, len(response.json["results"]))
        returned_ids = []
        for entity in response.json["results"]:
            self.valid_entity(entity, 3, 4)
            returned_ids.append(self._get_raw_id(entity))
        self.assertListEqual(sorted(ids[:2]), sorted(returned_ids))
        self.assertEqual(200, response.status_code)

    def test_successful_entity_metadata_get(self):
        _id = self.create_entity_get_id()

        response = self.app.get(
            "/entities/{}/metadata".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(4, len(response.json))
        self.assertEqual(str, type(response.json[0]["value"]))
        self.assertEqual("Een schilderij", response.json[0]["value"])
        self.assertEqual(200, response.status_code)

    def test_non_existent_entity_metadata_get(self):
        response = self.app.get(
            "/entities/non-existent-id/metadata",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.not_found(response)

    def test_successful_entity_metadata_key_get(self):
        _id = self.create_entity_get_id()

        response = self.app.get(
            "/entities/{}/metadata/title".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(2, len(response.json))
        self.assertEqual(str, type(response.json[0]["value"]))
        self.assertEqual("Een schilderij", response.json[0]["value"])
        self.assertEqual(200, response.status_code)

    def test_non_existent_entity_metadata_key_get(self):
        response = self.app.get(
            "/entities/non-existent-id/metadata/title",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.not_found(response)

    def test_successful_entity_put(self):
        response = self.create_entity()
        _id = response.json["_id"]

        self.valid_entity(response.json, 3, 4)
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
            "/entities/{}".format(self._get_raw_id(response.json)),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=update,
        )

        self.valid_entity(response.json, 1, 1)
        self.assertEqual(201, response.status_code)

    def test_non_existent_entity_put(self):
        update = json.dumps(
            {
                "_id": "non-existent-id",
                "identifiers": ["2021"],
                "type": "entity",
                "metadata": [{"key": "title", "value": "Een schilderij", "lang": "nl"}],
            }
        )

        response = self.app.put(
            "/entities/non-existent-id",
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=update,
        )

        self.not_found(response)

    def test_successful_entity_metadata_put(self):
        _id = self.create_entity_get_id()

        metadata = json.dumps(
            [
                {
                    "key": "type",
                    "value": "schilderij",
                    "lang": "nl",
                },
                {
                    "key": "type",
                    "value": "painting",
                    "lang": "en",
                },
            ]
        )

        response = self.app.put(
            "/entities/{}/metadata".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=metadata,
        )

        self.assertEqual(2, len(response.json))
        self.assertEqual(str, type(response.json[0]["value"]))
        self.assertEqual("schilderij", response.json[0]["value"])
        self.assertEqual(201, response.status_code)

    def test_non_existent_entity_metadata_put(self):
        metadata = json.dumps(
            [
                {
                    "key": "type",
                    "value": "schilderij",
                    "lang": "nl",
                },
                {
                    "key": "type",
                    "value": "painting",
                    "lang": "en",
                },
            ]
        )

        response = self.app.put(
            "/entities/non-existent-id/metadata",
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=metadata,
        )

        self.not_found(response)

    def test_successful_entity_patch(self):
        response = self.create_entity()

        self.valid_entity(response.json, 3, 4)
        self.assertEqual(201, response.status_code)

        update = json.dumps(
            {
                "metadata": [{"key": "title", "value": "Een schilderij", "lang": "nl"}],
            }
        )

        response = self.app.patch(
            "/entities/{}".format(self._get_raw_id(response.json)),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=update,
        )

        self.valid_entity(response.json, 3, 1)
        self.assertEqual(201, response.status_code)

    def test_non_existent_entity_patch(self):
        update = json.dumps(
            {
                "metadata": [{"key": "title", "value": "Een schilderij", "lang": "nl"}],
            }
        )

        response = self.app.patch(
            "/entities/non-existent-id",
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=update,
        )

        self.not_found(response)

    def test_successful_entity_delete(self):
        _id = self.create_entity_get_id()

        response = self.app.delete(
            "/entities/{}".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)

    def test_non_existent_entity_delete(self):
        response = self.app.delete(
            "/entities/non-existent-id",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.not_found(response)

    def test_successful_entity_metadata_key_delete(self):
        _id = self.create_entity_get_id()

        response = self.app.delete(
            "/entities/{}/metadata/title".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)

        response = self.app.get(
            "/entities/{}/metadata".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(2, len(response.json))
        self.assertEqual(str, type(response.json[0]["value"]))
        self.assertEqual("Beschrijving van een schilderij", response.json[0]["value"])
        self.assertEqual(str, type(response.json[0]["key"]))
        self.assertEqual("description", response.json[0]["key"])
        self.assertEqual(200, response.status_code)

    def test_non_existent_entity_metadata_key_delete(self):
        response = self.app.delete(
            "/entities/non-existent-id/metadata/title",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.not_found(response)

    def test_add_mediafile_to_entity(self):
        mediafile = self.create_mediafile()
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(mediafile.json),
        )

        for created_mediafile in response.json:
            self.valid_mediafile(created_mediafile)
            if "entities" in created_mediafile:
                self.assertEqual(list, type(created_mediafile["entities"]))
                self.assertEqual(3, len(created_mediafile["entities"]))
        self.assertEqual(201, response.status_code)

    def test_add_mediafile_to_non_existent_entity(self):
        mediafile = self.create_mediafile()

        response = self.app.post(
            "/entities/non-existent-id/mediafiles",
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(mediafile.json),
        )

        self.not_found(response)

    def test_get_mediafile_from_entity(self):
        mediafile = self.create_mediafile()
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(mediafile.json),
        )

        response = self.app.get(
            "/entities/{}/mediafiles".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        for mediafile in response.json:
            self.valid_mediafile(mediafile)
            if "entities" in mediafile:
                self.assertEqual(list, type(mediafile["entities"]))
                self.assertEqual(3, len(mediafile["entities"]))
        self.assertEqual(200, response.status_code)

    def test_get_multiple_mediafile_from_entity(self):
        _id = self.create_entity_get_id()
        mediafile_count = 5

        for i in range(mediafile_count):
            mediafile = self.create_mediafile()

            response = self.app.post(
                "/entities/{}/mediafiles".format(_id),
                headers={**self.headers, **{"Content-Type": "application/json"}},
                data=json.dumps(mediafile.json),
            )

        response = self.app.get(
            "/entities/{}/mediafiles?non_public=1".format(_id),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(mediafile_count, len(response.json))
        for mediafile in response.json:
            self.valid_mediafile(mediafile)
            if "entities" in mediafile:
                self.assertEqual(list, type(mediafile["entities"]))
                self.assertEqual(3, len(mediafile["entities"]))
        self.assertEqual(200, response.status_code)

    def test_get_mediafile_from_non_existent_entity(self):
        response = self.app.get(
            "/entities/non-existent-id/mediafiles",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.not_found(response)

    def test_add_entity_with_data(self):
        response = self.create_entity()
        updated_entity = response.json
        updated_entity["data"] = {
            "MensgemaaktObject.dimensie": [
                {
                    "@type": "Dimensie",
                    "Dimensie.beschrijving": "Dimensie van geheel",
                    "Dimensie.type": "hoogte",
                    "Dimensie.waarde": "8",
                    "Dimensie.eenheid": "cm",
                },
            ]
        }

        response = self.app.put(
            "/entities/{}".format(self._get_raw_id(updated_entity)),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(updated_entity),
        )

        self.valid_entity(response.json, 3, 4)
        self.assertEqual(dict, type(response.json["data"]))
        self.assertTrue("MensgemaaktObject.dimensie" in response.json["data"].keys())
        self.assertEqual(201, response.status_code)

    def test_add_relations_to_entity(self):
        entities = []
        for i in range(3):
            entities.append(self.create_entity().json)
        relations = [
            {"key": entities[1]["_id"], "type": "authoredBy"},
            {"key": entities[2]["_id"], "type": "isIn"},
        ]

        response = self.app.post(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(2, len(response.json))
        self.assertEqual(relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[1])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(
            [{"key": entities[0]["_id"], "type": "authored"}], response.json
        )

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[2])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(
            [{"key": entities[0]["_id"], "type": "contains"}], response.json
        )

    def test_update_entity_relations(self):
        entities = []
        for i in range(3):
            entities.append(self.create_entity().json)
        relations = [
            {"key": entities[1]["_id"], "type": "authoredBy"},
            {"key": entities[2]["_id"], "type": "isIn"},
        ]

        response = self.app.post(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)

        new_entity = self.create_entity().json
        new_relations = [{"key": new_entity["_id"], "type": "authoredBy"}]

        response = self.app.put(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(new_relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(new_entity)),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(
            [{"key": entities[0]["_id"], "type": "authored"}], response.json
        )

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[1])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(0, len(response.json))

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[2])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(0, len(response.json))

    def test_patch_entity_relations_no_overwrite(self):
        entities = []
        for i in range(2):
            entities.append(self.create_entity().json)
        relations = [{"key": entities[1]["_id"], "type": "authoredBy"}]

        response = self.app.post(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)

        new_entity = self.create_entity().json
        new_relations = [{"key": new_entity["_id"], "type": "isIn"}]

        response = self.app.patch(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(new_relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(2, len(response.json))
        self.assertEqual(relations + new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(new_entity)),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(
            [{"key": entities[0]["_id"], "type": "contains"}], response.json
        )

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[1])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(
            [{"key": entities[0]["_id"], "type": "authored"}], response.json
        )

    def test_patch_entity_relations_overwrite(self):
        entities = []
        for i in range(3):
            entities.append(self.create_entity().json)
        relations = [
            {"key": entities[1]["_id"], "type": "authoredBy"},
            {"key": entities[2]["_id"], "type": "authoredBy"},
        ]

        response = self.app.post(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)

        updated_relations = [{"key": entities[2]["_id"], "type": "isIn"}]

        response = self.app.patch(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(updated_relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(updated_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[0])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(2, len(response.json))
        self.assertEqual([relations[0]] + updated_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[1])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(
            [{"key": entities[0]["_id"], "type": "authored"}], response.json
        )

        response = self.app.get(
            "/entities/{}/relations".format(self._get_raw_id(entities[2])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(
            [{"key": entities[0]["_id"], "type": "contains"}], response.json
        )

    def test_get_entities_from_collection_by_type(self):
        entities = []
        for i in range(3):
            entities.append(self.create_entity().json)

        response = self.app.get(
            "/entities?type=entity",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(dict, type(response.json))
        self.assertEqual(list, type(response.json["results"]))
        self.assertEqual(3, len(response.json["results"]))

        entities[-1]["type"] = "asset"
        response = self.app.put(
            "/entities/{}".format(self._get_raw_id(entities[-1])),
            headers={**self.headers, **{"Content-Type": "application/json"}},
            data=json.dumps(entities[-1]),
        )
        self.assertEqual(201, response.status_code)

        response = self.app.get(
            "/entities?type=asset",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(dict, type(response.json))
        self.assertEqual(list, type(response.json["results"]))
        self.assertEqual(1, len(response.json["results"]))

        response = self.app.get(
            "/entities?type=person",
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )
        self.assertEqual(dict, type(response.json))
        self.assertEqual(list, type(response.json["results"]))
        self.assertEqual(0, len(response.json["results"]))

    def valid_entity(self, entity, identifier_count, metadata_count):
        self.assertEqual(str, type(self._get_raw_id(entity)))
        self.assertEqual(str, type(entity["metadata"][0]["value"]))
        self.assertEqual(str, type(entity["type"]))
        self.assertEqual(list, type(entity["metadata"]))
        self.assertEqual("Een schilderij", entity["metadata"][0]["value"])
        self.assertEqual(identifier_count, len(entity["identifiers"]))
        self.assertEqual(metadata_count, len(entity["metadata"]))

    def check_invalid_entity(self, response):
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual(
            "Entity doesn't have a valid format. '123' is not of type 'array'",
            response.json["message"],
        )
        self.assertEqual(400, response.status_code)

    def entity_list(self, count, limit, skip):
        ids = list()
        for i in range(count):
            ids.append(self.create_entity_get_id())

        response = self.app.get(
            "/entities?skip={}&limit={}".format(skip, limit),
            headers={**self.headers, **{"Content-Type": "application/json"}},
        )

        self.assertEqual(count, response.json["count"])
        self.assertEqual(limit, response.json["limit"])
        self.assertEqual(min(count, limit), len(response.json["results"]))
        self.assertEqual(skip + limit < count, "next" in response.json)
        self.assertEqual(skip > 0, "previous" in response.json)
        for i in range(min(count, limit)):
            entity = response.json["results"][i]
            self.assertEqual(ids[i + skip], self._get_raw_id(entity))
            self.valid_entity(entity, 3, 4)
        self.assertEqual(200, response.status_code)
