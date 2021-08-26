import json

from tests.base_case import BaseCase


class JobTest(BaseCase):
    def test_successful_job_create(self):
        response = self.create_job()
        self.valid_job(response.json)
        self.assertEqual(201, response.status_code)

    def test_invalid_job_create(self):
        response = self.app.post(
            "/jobs", headers={"content-type": "application/json"}, data=self.invalid_job
        )
        self.assertEqual(str, type(response.json["message"]))
        self.assertEqual("Job doesn't have a valid format", response.json["message"])
        self.assertEqual(400, response.status_code)

    def test_invalid_content_type_job_create(self):
        response = self.app.post(
            "/jobs",
            headers={"Content-Type": "multipart/form-data"},
            data=self.job,
        )

        self.invalid_input(response)

    def test_successful_job_get(self):
        _id = self.create_job_get_id()

        response = self.app.get(
            "/jobs/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.valid_job(response.json)
        self.assertEqual(200, response.status_code)

    def test_non_existent_job_get(self):
        response = self.app.get(
            "/jobs/non-existent-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)

    def test_successful_job_put(self):
        _id = self.create_job_get_id()

        update = json.dumps(
            {
                "_id": _id,
                "job_id": "0920943iu32i43k32iiu53",
                "job_type": "download",
                "job_info": "Updated info",
                "status": "in-progress",
                "start_time": "25-08-2021-04:30:00"
            }
        )

        response = self.app.put(
            "/jobs/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.valid_job(response.json)
        self.assertEqual(6, len(response.json))
        self.assertEqual(str, type(response.json["job_info"]))
        self.assertEqual("Updated info", response.json["job_info"])
        self.assertEqual(201, response.status_code)

    def test_non_existent_job_put(self):
        update = json.dumps(
            {
                "_id": "non-existent-id"
            }
        )

        response = self.app.put(
            "/jobs/non-existent-id",
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.not_found(response)
    #
    def test_successful_job_patch(self):
        _id = self.create_job_get_id()

        update = json.dumps(
            {
                "job_info": "Patched info",
            }
        )

        response = self.app.patch(
            "/jobs/{}".format(_id),
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.valid_job(response.json)
        self.assertEqual(6, len(response.json))
        self.assertEqual(str, type(response.json["job_info"]))
        self.assertEqual("Patched info", response.json["job_info"])
        self.assertEqual(201, response.status_code)

    def test_non_existent_job_patch(self):
        update = json.dumps(
            {
                "job_info": "lala",
            }
        )

        response = self.app.patch(
            "/jobs/non-existent-id",
            headers={"Content-Type": "application/json"},
            data=update,
        )

        self.not_found(response)

    def test_successful_job_delete(self):
        _id = self.create_job_get_id()

        response = self.app.delete(
            "/jobs/{}".format(_id), headers={"Content-Type": "application/json"}
        )

        self.assertFalse(response.data)
        self.assertEqual(204, response.status_code)

    def test_non_existent_tenant_delete(self):
        response = self.app.delete(
            "/tenants/non-existent-id", headers={"Content-Type": "application/json"}
        )

        self.not_found(response)
