import json
import os

from tests.base_case import BaseCase


class ImporterTest(BaseCase):
    upload_folder = os.getenv("UPLOAD_FOLDER", "/mnt/media-import")

    def import_csv(self, path):
        upload_data = json.dumps(
            {
                "upload_folder": path,
            }
        )
        response = self.app.post(
            "/importer/start",
            headers={"Content-Type": "application/json"},
            data=upload_data,
        )
        self.assertEqual(
            response.json["data"]["upload_folder"],
            os.path.join(self.upload_folder, path),
        )

    def get_from_db(self, endpoint):
        response = self.app.get(
            endpoint,
            headers={"Content-Type": "application/json"},
        )
        return response

    def test_get_directories(self):
        response = self.get_from_db("/importer/directories")
        self.assertEqual(list, type(response.json))
        self.assertEqual(
            response.json,
            [
                str(x[0]).removeprefix(self.upload_folder)
                for x in os.walk(self.upload_folder)
            ],
        )

    def test_import_no_csv(self):
        self.import_csv("Empty")
        response = self.get_from_db("/entities")
        self.assertEqual(200, response.status_code)
        self.assertFalse(response.json["count"])
        self.assertFalse(response.json["results"])

    def test_import_malformed_csv(self):
        self.import_csv("Industrie Museum")
        response = self.get_from_db("/entities")
        self.assertEqual(200, response.status_code)
        self.assertFalse(response.json["count"])
        self.assertFalse(response.json["results"])
