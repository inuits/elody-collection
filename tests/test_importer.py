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
        response = self.send_post_request("/importer/start", upload_data)
        self.assertEqual(
            response.json["data"]["upload_folder"],
            os.path.join(self.upload_location, folder),
        )

    def send_post_request(self, endpoint, json_data):
        response = self.app.post(
            endpoint,
            headers={"Content-Type": "application/json"},
            data=json_data,
        )
        self.assertEqual(201, response.status_code)
        return response

    def send_get_request(self, endpoint):
        response = self.app.get(
            endpoint,
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(200, response.status_code)
        return response

    def run_test(self, folder, entity_count, mediafile_count):
        self.import_csv(folder)
        self.validate_db_response("/entities", "entity", entity_count)
        self.validate_db_response("/mediafiles", "mediafile", mediafile_count)

    def validate_db_response(self, endpoint, entity_type, count):
        response = self.send_get_request(endpoint)
        self.assertEqual(response.json["count"], count)
        self.assertEqual(len(response.json["results"]), response.json["count"])
        for obj in response.json["results"]:
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
        response = self.send_get_request("/importer/directories")
        self.assertEqual(list, type(response.json))
        self.assertEqual(
            response.json,
            [
                str(x[0]).removeprefix(self.upload_location)
                for x in os.walk(self.upload_location)
            ],
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

    def set_upload_sources(self, upload_sources_json, upload_sources):
        response = self.send_post_request("/importer/sources", upload_sources_json)
        self.assertEqual(list, type(response.json))
        self.assertEqual(upload_sources, response.json)

    def validate_upload_sources(self, upload_sources):
        response = self.send_get_request("/importer/sources")
        self.assertEqual(list, type(response.json))
        self.assertEqual(upload_sources, response.json)

    def set_upload_location(self, upload_location_json, upload_location):
        response = self.send_post_request("/importer/location", upload_location_json)
        self.assertEqual(str, type(response.json))
        self.assertEqual(upload_location, response.json)

    def validate_upload_location(self, upload_location):
        response = self.send_get_request("/importer/location")
        self.assertEqual(str, type(response.json))
        self.assertEqual(upload_location, response.json)

    def test_upload_sources(self):
        self.validate_upload_sources([os.getenv("UPLOAD_LOCATION", "/mnt/media-import")])

        upload_sources = ["/mnt/upload_source", "/mnt/ntfs_share"]
        upload_sources_json = json.dumps(
            {
                "upload_sources": upload_sources,
            }
        )
        self.set_upload_sources(upload_sources_json, upload_sources)
        self.validate_upload_sources(upload_sources)
        self.validate_upload_location("")

        # Try to overwrite
        new_upload_sources = ["/mnt/new_upload_source", "/mnt/new_ntfs_share"]
        new_upload_sources_json = json.dumps(
            {
                "upload_sources": new_upload_sources,
            }
        )
        self.set_upload_sources(new_upload_sources_json, new_upload_sources)
        self.validate_upload_sources(new_upload_sources)
        self.validate_upload_location("")

    def test_upload_location(self):
        self.validate_upload_location(os.getenv("UPLOAD_LOCATION", "/mnt/media-import"))

        upload_sources = ["/mnt/upload_source", "/mnt/ntfs_share"]
        upload_sources_json = json.dumps(
            {
                "upload_sources": upload_sources,
            }
        )
        self.set_upload_sources(upload_sources_json, upload_sources)
        upload_location = "/mnt/upload_source"
        upload_location_json = json.dumps(
            {
                "upload_location": upload_location,
            }
        )
        self.set_upload_location(upload_location_json, upload_location)
        self.validate_upload_location(upload_location)

        new_upload_location = "/mnt/nfts_share"
        new_upload_location_json = json.dumps(
            {
                "upload_location": new_upload_location,
            }
        )
        self.set_upload_location(new_upload_location_json, new_upload_location)
        self.validate_upload_location(new_upload_location)

        # Setting new sources clears location
        self.set_upload_sources(upload_sources_json, upload_sources)
        self.validate_upload_location("")
