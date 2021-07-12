import unittest
import json

from tests.base_case import BaseCase


class MediafileTest(BaseCase):
    def test_successful_mediafile_create(self):
        response = self.create_mediafile()

        self.valid_mediafile(response.json)
        self.assertEqual(201, response.status_code)

    def test_invalid_input_mediafile_create(self):
        mediafile = "<mediafile><location>http://dams-storage.inuits.io/1234-abcd</location><mediafile>"

        response = self.app.post(
            "/mediafiles", headers={"content-type": "application/json"}, data=mediafile
        )

        self.invalid_input(response)

    def test_invalid_content_type_mediafile_create(self):
        response = self.app.post(
            "/mediafiles",
            headers={"Content-Type": "multipart/form-data"},
            data=self.mediafile,
        )

        self.invalid_input(response)

    def test_successful_mediafile_get(self):
        _id = self.create_mediafile_get_id()

        response = self.app.get(
            "/mediafiles/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.valid_mediafile(response.json)
        self.assertEqual(200, response.status_code)

    def test_non_existant_mediafile_get(self):
        response = self.app.get(
            "/mediafiles/non-existant-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)

    def test_successful_mediafile_put(self):
        _id = self.create_mediafile_get_id()

        update = json.dumps(
            {
                "_id": _id,
                "identifiers": ["12345", "abcde"],
                "type": "mediafile",
                "location": "http://dams-storage.inuits.io/download/test.jpg",
                "format": "jpg",
            }
        )

        response = self.app.put(
            "/mediafiles/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.valid_mediafile(response.json)
        self.assertEqual(5, len(response.json))
        self.assertEqual(str, type(response.json["format"]))
        self.assertEqual("jpg", response.json["format"])
        self.assertEqual(201, response.status_code)

    def test_non_existant_mediafile_put(self):
        update = json.dumps(
            {
                "_id": "non-existant-id",
                "location": "http://dams-storage.inuits.io/download/test.jpg",
                "format": "jpg",
            }
        )

        response = self.app.put(
            "/mediafiles/non-existant-id",
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.not_found(response)

    def test_successful_mediafile_patch(self):
        _id = self.create_mediafile_get_id()

        update = json.dumps(
            {
                "format": "jpg",
            }
        )

        response = self.app.patch(
            "/mediafiles/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.valid_mediafile(response.json)
        self.assertEqual(5, len(response.json))
        self.assertEqual(str, type(response.json["format"]))
        self.assertEqual("jpg", response.json["format"])
        self.assertEqual(201, response.status_code)

    def test_non_existant_mediafile_patch(self):
        update = json.dumps(
            {
                "format": "jpg",
            }
        )

        response = self.app.patch(
            "/mediafiles/non-existant-id",
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.not_found(response)

    def test_successful_mediafile_delete(self):
        _id = self.create_mediafile_get_id()

        response = self.app.delete(
            "/mediafiles/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)

    def test_non_existant_mediafile_delete(self):
        response = self.app.delete(
            "/mediafiles/non-existant-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)
