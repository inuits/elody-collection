import json

from tests.base_case import BaseCase


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
            headers={"content-type": "application/json"},
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
            headers={"content-type": "application/json"},
            data=metadata,
        )

        self.not_found(response)

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

    def test_invalid_entity_create(self):
        response = self.app.post(
            "/entities",
            headers={"content-type": "application/json"},
            data=self.invalid_entity,
        )

        self.check_invalid_entity(response)

    def test_successful_entity_mediafile_create(self):
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles/create".format(_id),
            headers={"Content-Type": "application/json"},
            data=self.filename,
        )

        self.assertEqual(str, type(response.json))
        self.assertTrue(
            response.json.startswith("https://dams-storage-api.inuits.io/upload/")
        )
        self.assertEqual(87, len(response.json))
        self.assertEqual(201, response.status_code)

        response = self.app.get(
            "/entities/{}/mediafiles".format(_id),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(1, len(response.json))
        for mediafile in response.json:
            self.assertEqual(list, type(mediafile["entities"]))
            self.assertEqual(3, len(mediafile["entities"]))

    def test_successful_entity_mediafile_create_with_metadata(self):
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles/create".format(_id),
            headers={"Content-Type": "application/json"},
            data=self.filename_with_metadata,
        )

        self.assertEqual(str, type(response.json))
        self.assertTrue(
            response.json.startswith("https://dams-storage-api.inuits.io/upload/")
        )
        self.assertEqual(87, len(response.json))
        self.assertEqual(201, response.status_code)

        response = self.app.get(
            "/entities/{}/mediafiles".format(_id),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(1, len(response.json))
        for mediafile in response.json:
            self.assertEqual(list, type(mediafile["entities"]))
            self.assertEqual(3, len(mediafile["entities"]))
            self.assertEqual(list, type(mediafile["metadata"]))
            self.assertEqual(2, len(mediafile["metadata"]))
            self.assertEqual("rights", mediafile["metadata"][0]["key"])
            self.assertEqual("Inuits", mediafile["metadata"][1]["value"])

    def test_entity_mediafile_create_without_filename(self):
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles/create".format(_id),
            headers={"Content-Type": "application/json"},
            data=json.dumps({}),
        )

        self.invalid_input(response)

    def test_create_mediafile_from_non_existent_entity(self):
        response = self.app.post(
            "/entities/non-existent-id/mediafiles/create",
            headers={"Content-Type": "application/json"},
        )

        self.not_found(response)

    def test_successful_entity_get(self):
        _id = self.create_entity_get_id()

        response = self.app.get(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.valid_entity(response.json, 3, 4)
        self.assertEqual(200, response.status_code)

    def test_non_existent_entity_get(self):
        response = self.app.get(
            "/entities/non-existent-id", headers={"Content-Type": "application/json"}
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
        valid_asset_count = 3
        _id1 = self.create_entity_get_id()
        _id2 = self.create_entity_get_id()
        _id3 = self.create_entity_get_id()

        ids = [_id1, _id2, _id3]

        response = self.app.get(
            "/entities?ids={}".format(",".join(ids)),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(valid_asset_count, response.json["count"])
        self.assertEqual(valid_asset_count, len(response.json["results"]))
        for i in range(valid_asset_count):
            entity = response.json["results"][i]
            self.assertEqual(ids[i], entity["_id"])
            self.valid_entity(entity, 3, 4)
        self.assertEqual(200, response.status_code)

    def test_entity_ids_get_one_non_existant_id(self):
        valid_asset_count = 2
        _id1 = self.create_entity_get_id()
        _id2 = self.create_entity_get_id()
        _id3 = "non_existant_id"

        ids = [_id1, _id2, _id3]

        response = self.app.get(
            "/entities?ids={}".format(",".join(ids)),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(valid_asset_count, response.json["count"])
        self.assertEqual(valid_asset_count, len(response.json["results"]))
        for i in range(valid_asset_count):
            entity = response.json["results"][i]
            self.assertEqual(ids[i], entity["_id"])
            self.valid_entity(entity, 3, 4)
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

    def test_non_existent_entity_metadata_get(self):
        response = self.app.get(
            "/entities/non-existent-id/metadata",
            headers={"Content-Type": "application/json"},
        )

        self.not_found(response)

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

    def test_non_existent_entity_metadata_key_get(self):
        response = self.app.get(
            "/entities/non-existent-id/metadata/title",
            headers={"Content-Type": "application/json"},
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
            "/entities/{}".format(_id),
            headers={"Content-Type": "application/json"},
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
            headers={"Content-Type": "application/json"},
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
            headers={"content-type": "application/json"},
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
            headers={"content-type": "application/json"},
            data=metadata,
        )

        self.not_found(response)

    def test_successful_entity_patch(self):
        response = self.create_entity()
        _id = response.json["_id"]

        self.valid_entity(response.json, 3, 4)
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
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.not_found(response)

    def test_successful_entity_delete(self):
        _id = self.create_entity_get_id()

        response = self.app.delete(
            "/entities/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)

    def test_non_existent_entity_delete(self):
        response = self.app.delete(
            "/entities/non-existent-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)

    def test_successful_entity_metadata_key_delete(self):
        _id = self.create_entity_get_id()

        response = self.app.delete(
            "/entities/{}/metadata/title".format(_id),
            headers={"Content-Type": "application/json"},
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)

        response = self.app.get(
            "/entities/{}/metadata".format(_id),
            headers={"Content-Type": "application/json"},
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
            headers={"Content-Type": "application/json"},
        )

        self.not_found(response)

    def test_add_mediafile_to_entity(self):
        mediafile = self.create_mediafile()
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles".format(_id),
            headers={"Content-Type": "application/json"},
            data=json.dumps(mediafile.json),
        )

        self.valid_mediafile(response.json)
        self.assertEqual(list, type(response.json["entities"]))
        self.assertEqual(3, len(response.json["entities"]))
        self.assertEqual(201, response.status_code)

    def test_add_mediafile_to_non_existent_entity(self):
        mediafile = self.create_mediafile()

        response = self.app.post(
            "/entities/non-existent-id/mediafiles",
            headers={"Content-Type": "application/json"},
            data=json.dumps(mediafile.json),
        )

        self.not_found(response)

    def test_get_mediafile_from_entity(self):
        mediafile = self.create_mediafile()
        _id = self.create_entity_get_id()

        response = self.app.post(
            "/entities/{}/mediafiles".format(_id),
            headers={"Content-Type": "application/json"},
            data=json.dumps(mediafile.json),
        )

        response = self.app.get(
            "/entities/{}/mediafiles".format(_id),
            headers={"Content-Type": "application/json"},
        )

        for mediafile in response.json:
            self.valid_mediafile(mediafile)
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
                headers={"Content-Type": "application/json"},
                data=json.dumps(mediafile.json),
            )

        response = self.app.get(
            "/entities/{}/mediafiles".format(_id),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(mediafile_count, len(response.json))
        for mediafile in response.json:
            self.valid_mediafile(mediafile)
            self.assertEqual(list, type(mediafile["entities"]))
            self.assertEqual(3, len(mediafile["entities"]))
        self.assertEqual(200, response.status_code)

    def test_get_mediafile_from_non_existent_entity(self):
        response = self.app.get(
            "/entities/non-existent-id/mediafiles",
            headers={"Content-Type": "application/json"},
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
            "/entities/{}".format(updated_entity["_id"]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(updated_entity),
        )

        self.valid_entity(response.json, 3, 4)
        self.assertEqual(dict, type(response.json["data"]))
        self.assertTrue("MensgemaaktObject.dimensie" in response.json["data"].keys())
        self.assertEqual(201, response.status_code)

    def test_add_relations_to_entity(self):
        ids = []
        for i in range(3):
            ids.append(self.create_entity_get_id())
        relations = [
            {"key": ids[1], "type": "authoredBy"},
            {"key": ids[2], "type": "isIn"},
        ]

        response = self.app.post(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(2, len(response.json))
        self.assertEqual(relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[1]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual([{"key": ids[0], "type": "authored"}], response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[2]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual([{"key": ids[0], "type": "contains"}], response.json)

    def test_update_entity_relations(self):
        ids = []
        for i in range(3):
            ids.append(self.create_entity_get_id())
        relations = [
            {"key": ids[1], "type": "authoredBy"},
            {"key": ids[2], "type": "isIn"},
        ]

        response = self.app.post(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)

        new_id = self.create_entity_get_id()
        new_relations = [{"key": new_id, "type": "authoredBy"}]

        response = self.app.put(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(new_relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(new_id),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual([{"key": ids[0], "type": "authored"}], response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[1]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(0, len(response.json))

        response = self.app.get(
            "/entities/{}/relations".format(ids[2]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(0, len(response.json))

    def test_patch_entity_relations_no_overwrite(self):
        ids = []
        for i in range(2):
            ids.append(self.create_entity_get_id())
        relations = [{"key": ids[1], "type": "authoredBy"}]

        response = self.app.post(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)

        new_id = self.create_entity_get_id()
        new_relations = [{"key": new_id, "type": "isIn"}]

        response = self.app.patch(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(new_relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(2, len(response.json))
        self.assertEqual(relations + new_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(new_id),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual([{"key": ids[0], "type": "contains"}], response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[1]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual([{"key": ids[0], "type": "authored"}], response.json)

    def test_patch_entity_relations_overwrite(self):
        ids = []
        for i in range(3):
            ids.append(self.create_entity_get_id())
        relations = [
            {"key": ids[1], "type": "authoredBy"},
            {"key": ids[2], "type": "authoredBy"},
        ]

        response = self.app.post(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(relations),
        )
        self.assertEqual(201, response.status_code)

        updated_relations = [{"key": ids[2], "type": "isIn"}]

        response = self.app.patch(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
            data=json.dumps(updated_relations),
        )
        self.assertEqual(201, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual(updated_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[0]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(2, len(response.json))
        self.assertEqual([relations[0]] + updated_relations, response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[1]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual([{"key": ids[0], "type": "authored"}], response.json)

        response = self.app.get(
            "/entities/{}/relations".format(ids[2]),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(list, type(response.json))
        self.assertEqual(1, len(response.json))
        self.assertEqual([{"key": ids[0], "type": "contains"}], response.json)

    def valid_entity(self, entity, identifier_count, metadata_count):
        self.assertEqual(str, type(entity["_id"]))
        self.assertEqual(str, type(entity["metadata"][0]["value"]))
        self.assertEqual(str, type(entity["type"]))
        self.assertEqual(list, type(entity["metadata"]))
        self.assertEqual("Een schilderij", entity["metadata"][0]["value"])
        self.assertEqual(identifier_count, len(entity["identifiers"]))
        self.assertEqual(metadata_count, len(entity["metadata"]))

    def check_invalid_entity(self, response):
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Entity doesn't have a valid format", response.json["message"])
        self.assertEqual(400, response.status_code)

    def entity_list(self, count, limit, skip):
        ids = list()
        for i in range(count):
            ids.append(self.create_entity_get_id())

        response = self.app.get(
            "/entities?skip={}&limit={}".format(skip, limit),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(count, response.json["count"])
        self.assertEqual(limit, response.json["limit"])
        self.assertEqual(min(count, limit), len(response.json["results"]))
        self.assertEqual(skip + limit < count, "next" in response.json)
        self.assertEqual(skip > 0, "previous" in response.json)
        for i in range(min(count, limit)):
            entity = response.json["results"][i]
            self.assertEqual(ids[i + skip], entity["_id"])
            self.valid_entity(entity, 3, 4)
        self.assertEqual(200, response.status_code)
