import json
import os

import requests

from tests.base_case import BaseCase


class ImporterTest(BaseCase):
    def setUp(self):
        super().setUp()
        self.collection_api_url = os.getenv(
            "COLLECTION_API_URL", "http://localhost:8000"
        )
        os.environ["UPLOAD_SOURCE"] = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "csv"
        )
        self.addCleanup(self.drop_collections)

    def import_csv(self, folder):
        upload_source = os.getenv("UPLOAD_SOURCE", "/mnt/media-import")
        response = self.send_post_request("/importer/start", {"upload_folder": folder})
        self.assertEqual(
            response.json()["data"]["upload_folder"],
            os.path.join(upload_source, folder),
        )

    def drop_collections(self):
        self.send_post_request("/importer/drop", {})

    def send_post_request(self, endpoint, content, success=True):
        response = requests.post(
            "{}{}".format(self.collection_api_url, endpoint),
            headers={"Content-Type": "application/json"},
            data=json.dumps(content),
        )
        if success:
            self.assertEqual(201, response.status_code)
        else:
            self.assertEqual(404, response.status_code)
        return response

    def send_get_request(self, endpoint, success=True):
        response = requests.get(
            "{}{}".format(self.collection_api_url, endpoint),
            headers={"Content-Type": "application/json"},
        )
        if success:
            self.assertEqual(200, response.status_code)
        else:
            self.assertEqual(404, response.status_code)
        return response

    def run_test(self, folder, entity_count, mediafile_count):
        self.import_csv(folder)
        self.validate_db_response("/entities", "entity", entity_count)
        self.validate_db_response("/mediafiles", "mediafile", mediafile_count)

    def validate_db_response(self, endpoint, entity_type, count):
        response = self.send_get_request(endpoint)
        self.assertEqual(response.json()["count"], count)
        self.assertEqual(len(response.json()["results"]), response.json()["count"])
        for obj in response.json()["results"]:
            self.validate_object(entity_type, obj)

    def validate_entity(self, entity):
        self.assertEqual(str, type(entity["_id"]))
        self.assertEqual(str, type(entity["type"]))
        if "metadata" in entity:
            self.validate_metadata(entity["metadata"])

    def validate_metadata(self, metadata):
        self.assertEqual(list, type(metadata))
        self.assertTrue(len(metadata) > 0)
        for elem in metadata:
            if ("key", "rights") in elem.items():
                self.assertEqual(str, type(elem["value"]))
            elif ("key", "copyright") in elem.items():
                self.assertEqual(str, type(elem["value"]))

    def validate_object(self, obj_type, obj):
        if obj_type == "entity":
            self.validate_entity(obj)
        elif obj_type == "mediafile":
            self.valid_mediafile(obj)

    def test_get_directories(self):
        upload_source = os.getenv("UPLOAD_SOURCE", "/mnt/media-import")
        response = self.send_get_request("/importer/directories")
        self.assertEqual(list, type(response.json()))
        self.assertEqual(
            response.json(),
            [str(x[0]).removeprefix(upload_source) for x in os.walk(upload_source)],
        )

    def test_import_bad_files(self):
        self.run_test("empty", 0, 0)
        self.run_test("malformed_columns", 0, 0)
        self.run_test("malformed_rows", 0, 0)

    # mediafile_count is wrong in the following tests as we don't check for duplicate files yet

    def test_import_csv_columns_casing(self):
        self.run_test("column_casing", 2, 2)

    def test_import_csv_path_types(self):
        self.run_test("path_types", 3, 3)

    def test_import_csv_metadata(self):
        self.run_test("metadata", 4, 7)
