import json
import os

from tests.base_case import BaseCase


class ImporterTest(BaseCase):
    upload_folder = os.getenv("UPLOAD_FOLDER", "/mnt/media-import")

    def test_get_directories(self):
        response = self.app.get(
            "/importer/directories",
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(list, type(response.json))
        self.assertEqual(
            response.json,
            [
                str(x[0]).removeprefix(self.upload_folder)
                for x in os.walk(self.upload_folder)
            ],
        )

    def test_start_import_no_csv(self):
        upload_data = json.dumps(
            {
                "upload_folder": "Empty",
            }
        )

        response = self.app.post(
            "/importer/start",
            headers={"Content-Type": "application/json"},
            data=upload_data,
        )

        self.assertEqual(
            response.json["data"]["upload_folder"],
            os.path.join(self.upload_folder, "Empty"),
        )
