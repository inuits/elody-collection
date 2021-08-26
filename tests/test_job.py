import json

from tests.base_case import BaseCase


class JobTest(BaseCase):
    def test_successful_job_create(self):
        response = self.create_job()

        self.valid_job(response.json)
        self.assertEqual(201, response.status_code)

    # def test_invalid_job_create(self):
    #
    #     response = self.app.post(
    #         "/tenants", headers={"content-type": "application/json"}, data=self.invalid_job
    #     )
    #     print(response.json)
    #     self.assertEqual(str, type(response.json["message"]))
    #     self.assertEqual("Job doesn't have a valid format", response.json["message"])
    #     self.assertEqual(400, response.status_code)

    # def test_invalid_input_tenant_create(self):
    #     tenant = "<tenant><location>http://dams-storage.inuits.io/1234-abcd</location><tenant>"
    #
    #     response = self.app.post(
    #         "/tenants", headers={"content-type": "application/json"}, data=tenant
    #     )
    #
    #     self.invalid_input(response)
    #
    # def test_invalid_content_type_tenant_create(self):
    #     response = self.app.post(
    #         "/tenants",
    #         headers={"Content-Type": "multipart/form-data"},
    #         data=self.tenant,
    #     )
    #
    #     self.invalid_input(response)
    #
    # def test_successful_tenant_get(self):
    #     _id = self.create_tenant_get_id()
    #
    #     response = self.app.get(
    #         "/tenants/{}".format(_id), headers={"Content-Type": "application/json"}
    #     )
    #
    #     self.valid_tenant(response.json)
    #     self.assertEqual(200, response.status_code)
    #
    # def test_non_existent_tenant_get(self):
    #     response = self.app.get(
    #         "/tenants/non-existent-id", headers={"Content-Type": "application/json"}
    #     )
    #
    #     self.not_found(response)
    #
    # def test_successful_tenant_put(self):
    #     _id = self.create_tenant_get_id()
    #
    #     update = json.dumps(
    #         {
    #             "_id": _id,
    #             "identifiers": ["12345", "abcde"],
    #             "type": "tenant",
    #             "name": "Een museum",
    #             "city": "Gent",
    #         }
    #     )
    #
    #     response = self.app.put(
    #         "/tenants/{}".format(_id),
    #         headers={"Content-Type": "application/json"},
    #         data=update,
    #     )
    #
    #     self.valid_tenant(response.json)
    #     self.assertEqual(5, len(response.json))
    #     self.assertEqual(str, type(response.json["city"]))
    #     self.assertEqual("Gent", response.json["city"])
    #     self.assertEqual(201, response.status_code)
    #
    # def test_non_existent_tenant_put(self):
    #     update = json.dumps(
    #         {
    #             "_id": "non-existent-id",
    #             "identifiers": ["12345", "abcde"],
    #             "type": "tenant",
    #             "name": "Een museum",
    #             "city": "Gent",
    #         }
    #     )
    #
    #     response = self.app.put(
    #         "/tenants/non-existent-id",
    #         headers={"Content-Type": "application/json"},
    #         data=update,
    #     )
    #
    #     self.not_found(response)
    #
    # def test_successful_tenant_patch(self):
    #     _id = self.create_tenant_get_id()
    #
    #     update = json.dumps(
    #         {
    #             "city": "Gent",
    #         }
    #     )
    #
    #     response = self.app.patch(
    #         "/tenants/{}".format(_id),
    #         headers={"Content-Type": "application/json"},
    #         data=update,
    #     )
    #
    #     self.valid_tenant(response.json)
    #     self.assertEqual(5, len(response.json))
    #     self.assertEqual(str, type(response.json["city"]))
    #     self.assertEqual("Gent", response.json["city"])
    #     self.assertEqual(201, response.status_code)
    #
    # def test_non_existent_tenant_patch(self):
    #     update = json.dumps(
    #         {
    #             "city": "Gent",
    #         }
    #     )
    #
    #     response = self.app.patch(
    #         "/tenants/non-existent-id",
    #         headers={"Content-Type": "application/json"},
    #         data=update,
    #     )
    #
    #     self.not_found(response)
    #
    # def test_successful_tenant_delete(self):
    #     _id = self.create_tenant_get_id()
    #
    #     response = self.app.delete(
    #         "/tenants/{}".format(_id), headers={"Content-Type": "application/json"}
    #     )
    #
    #     self.assertFalse(response.data)
    #     self.assertEqual(204, response.status_code)
    #
    # def test_non_existent_tenant_delete(self):
    #     response = self.app.delete(
    #         "/tenants/non-existent-id", headers={"Content-Type": "application/json"}
    #     )
    #
    #     self.not_found(response)
