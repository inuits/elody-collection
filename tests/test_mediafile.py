import json

from tests.base_case import BaseCase


class MediafileTest(BaseCase):
    def test_successful_mediafile_create(self):
        response = self.create_mediafile()

        self.valid_mediafile(response.json)
        self.assertEqual(201, response.status_code)

    def test_invalid_input_mediafile_create(self):
        mediafile = "<mediafile><original_file_location>http://dams-storage.inuits.io/1234-abcd</original_file_location><mediafile>"

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

    def test_invalid_mediafile_create(self):
        response = self.app.post(
            "/mediafiles",
            headers={"content-type": "application/json"},
            data=self.invalid_mediafile,
        )

        self.check_invalid_mediafile(response)

    def test_successful_mediafile_get(self):
        _id = self.create_mediafile_get_id()

        response = self.app.get(
            "/mediafiles/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.valid_mediafile(response.json)
        self.assertEqual(200, response.status_code)

    def test_non_existent_mediafile_get(self):
        response = self.app.get(
            "/mediafiles/non-existent-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)

    def test_successful_mediafile_put(self):
        _id = self.create_mediafile_get_id()

        update = json.dumps(
            {
                "_id": _id,
                "identifiers": ["12345", "abcde"],
                "type": "mediafile",
                "filename": "test.jpg",
                "original_file_location": "http://dams-storage.inuits.io/download/test.jpg",
                "format": "jpg",
            }
        )

        response = self.app.put(
            "/mediafiles/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.valid_mediafile(response.json)
        self.assertEqual(6, len(response.json))
        self.assertEqual(str, type(response.json["format"]))
        self.assertEqual("jpg", response.json["format"])
        self.assertEqual(201, response.status_code)

    def test_non_existent_mediafile_put(self):
        update = json.dumps(
            {
                "_id": "non-existent-id",
                "original_file_location": "http://dams-storage.inuits.io/download/test.jpg",
                "format": "jpg",
            }
        )

        response = self.app.put(
            "/mediafiles/non-existent-id",
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

    def test_non_existent_mediafile_patch(self):
        update = json.dumps(
            {
                "format": "jpg",
            }
        )

        response = self.app.patch(
            "/mediafiles/non-existent-id",
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

    def test_non_existent_mediafile_delete(self):
        response = self.app.delete(
            "/mediafiles/non-existent-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)

    def test_successful_mediafile_list_get(self):
        self.mediafile_list(2, 20, 0)

    def test_successful_mediafile_list_first_mediafiles(self):
        self.mediafile_list(40, 20, 0)

    def test_successful_mediafile_list_middle_mediafiles(self):
        self.mediafile_list(40, 15, 10)

    def test_successful_mediafile_list_last_mediafiles(self):
        self.mediafile_list(40, 10, 30)

    def test_successful_mediafile_list_no_mediafiles(self):
        self.mediafile_list(0, 20, 0)

    def mediafile_list(self, count, limit, skip):
        ids = list()
        for i in range(count):
            ids.append(self.create_mediafile_get_id())

        response = self.app.get(
            "/mediafiles?skip={}&limit={}".format(skip, limit),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(count, response.json["count"])
        self.assertEqual(limit, response.json["limit"])
        self.assertEqual(min(count, limit), len(response.json["results"]))
        self.assertEqual(skip + limit < count, "next" in response.json)
        self.assertEqual(skip > 0, "previous" in response.json)
        for i in range(min(count, limit)):
            mediafile = response.json["results"][i]
            self.assertEqual(ids[i + skip], mediafile["_id"])
            self.valid_mediafile(mediafile)
        self.assertEqual(200, response.status_code)

    def check_invalid_mediafile(self, response):
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual(
            "Mediafile doesn't have a valid format\n'filename' is a required property",
            response.json["message"],
        )
        self.assertEqual(400, response.status_code)
