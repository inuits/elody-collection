import json

from tests.base_case import BaseCase


class JobStatusTest(BaseCase):
    content_type = headers = {"Content-Type": "application/json"}
    single_upload = json.dumps(
        {
            "job_info": "test Job",
            "job_type": "single",
            "asset": {"file_name": "/images/file.png"},
        }
    )
    multiple_upload = json.dumps(
        {
            "job_info": "test Job",
            "job_type": "single",
            "asset": [
                {"file_name": "/images/file.png"},
                {"file_name": "/images/file1.png"},
                {"file_name": "/images/file2.png"},
                {"file_name": "/images/file3.png"},
                {"file_name": "/images/file4.png"},
            ],
        }
    )

    single_asset = json.dumps({"file_name": "file.png"})
    multiple_asset = json.dumps(
        [
            {"file_name": "/images/file.png"},
            {"file_name": "/images/file1.png"},
            {"file_name": "/images/file2.png"},
            {"file_name": "/images/file3.png"},
            {"file_name": "/images/file4.png"},
        ]
    )

    def setUp(self):
        super().setUp()
        self.insert_single_id = ""
        self.insert_multiple_id = ""

    def valid_id(self, id):
        self.assertEqual(str, type(id))

    def job_not_found(self, res):
        self.assertEqual(404, res.status_code)
        self.assertEqual(1, len(res.json))

    def test_upload_single_file(self):
        res = self.app(f"/jobs/upload/single", data=self.single_upload)
        self.insert_single_id = res.json("_id")
        self.assertEqual(201, res.status_code)
        self.assertEqual(str, res.json["message"])

    def test_upload_multiple(self):
        res = self.app(f"/jobs/upload/multiple", data=self.multiple_upload)
        self.assertEqual(201, res.status_code)

    # Test Job not Found given an arbitrary job ID
    def test_job_not_found(self):
        res = self.app.get(
            "/jobs/unknown-job-id",
        )
        self.job_not_found(res)

    # Success get_job_by_id
    def test_success_get_job_by_id(self):
        """ Run test_upload_single() or test_upload_multiple before calling this method"""
        res = self.app.get(f"/jobs/{self.insert_single_id}", headers=self.content_type)
        self.assertEqual(200, res.status_code)

    def get_job_by_asset(self, asset):
        asset = "single" if isinstance(asset, list) else "multiple"
        res = self.app.get(
            f'/jobs/{self.single_asset if asset == "single" else self.multiple_asset}/{asset}',
            headers=self.content_type,
        )
        return res.json()

    def test_get_job_by_single_asset(self):
        response = self.get_job_by_asset(self.single_asset)
        self.assertEqual(200, response.status_code)
        self.assertEqual(str, response.json["message"])

    def test_get_job_by_multiple_asset(self):
        response = self.get_job_by_asset(self.multiple_asset)
        self.assertEqual(200, response.status_code)
        self.assertEqual(str, response.json["message"])
