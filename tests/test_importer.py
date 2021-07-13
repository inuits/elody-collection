import json
import os

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

    def validate_db_response(self, endpoint, expected_count):
        response = self.get_from_db(endpoint)
        self.assertTrue(response.json["count"] == expected_count)
        self.assertTrue(len(response.json["results"]) == response.json["count"])
        for obj in response.json["results"]:
            self.validate_object(endpoint[1:], obj)

    def validate_object(self, type, obj):
        if type == "entities":
            self.valid_entity(obj)
        elif type == "mediafiles":
            self.valid_mediafile(obj)

    def valid_entity(self, entity):
        self.assertEqual(str, type(entity["_id"]))
        self.assertEqual(str, type(entity["type"]))

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

    def test_import_bad_files(self):
        self.import_csv("empty")
        self.validate_db_response("/entities", 0)
        self.validate_db_response("/mediafiles", 0)
        self.import_csv("malformed_columns")
        self.validate_db_response("/entities", 0)
        self.validate_db_response("/mediafiles", 0)
        self.import_csv("malformed_rows")
        self.validate_db_response("/entities", 0)
        self.validate_db_response("/mediafiles", 0)

    def test_import_csv_columns_casing(self):
        self.import_csv("column_casing")
        self.validate_db_response("/entities", 2)
        self.validate_db_response("/mediafiles", 2)
