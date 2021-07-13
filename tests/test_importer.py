import json
import os
from unittest.mock import patch

from tests.base_case import BaseCase


class ImporterTest(BaseCase):
    def import_csv(self, folder):
        upload_data = json.dumps(
            {
                "upload_folder": folder,
            }
        )
        response = self.app.post(
            "/importer/start",
            headers={"Content-Type": "application/json"},
            data=upload_data,
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(
            response.json["data"]["upload_folder"],
            os.path.join(self.upload_folder, folder),
        )

    def get_from_db(self, endpoint):
        response = self.app.get(
            endpoint,
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
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
        self.import_csv("empty")
        response = self.get_from_db("/entities")
        self.assertFalse(response.json["count"])
        self.assertFalse(response.json["results"])

    def test_import_malformed_columns(self):
        self.import_csv("malformed_columns")
        response = self.get_from_db("/entities")
        self.assertFalse(response.json["count"])
        self.assertFalse(response.json["results"])

    def test_import_malformed_rows(self):
        self.import_csv("malformed_rows")
        response = self.get_from_db("/entities")
        self.assertFalse(response.json["count"])
        self.assertFalse(response.json["results"])

    @patch("workers.importer.Importer.upload_file")
    def test_import_colums_not_capitalized(self, mocked_upload_file):
        self.import_csv("column_casing")
        response = self.get_from_db("/entities")
        self.assertTrue(response.json["count"] == 2)
        self.assertTrue(response.json["results"])
        response = self.get_from_db("/mediafiles")
        self.assertTrue(response.json["count"] == 1)
        self.assertTrue(response.json["results"])
