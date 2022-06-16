import app

from flask import request
from resources.base_resource import BaseResource
from validator import job_schema


class Job(BaseResource):
    @app.require_oauth("create-job")
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json("Job", content, job_schema)
        job = self.storage.save_item_to_collection("jobs", content)
        return job, 201

    @app.require_oauth("read-job")
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        ids = request.args.get("ids")
        job_type = request.args.get("type")
        if ids:
            ids = ids.split(",")
            return self.storage.get_items_from_collection_by_ids("jobs", ids)
        if job_type:
            fields = {"job_type": job_type, "parent_job_id": ""}
        else:
            fields = {"parent_job_id": ""}
        jobs = self.storage.get_items_from_collection_by_fields(
            "jobs", fields, skip, limit
        )
        count = jobs["count"]
        jobs["limit"] = limit
        if skip + limit < count:
            jobs["next"] = f"/jobs?skip={skip + limit}&limit={limit}"
        if skip > 0:
            jobs["previous"] = f"/jobs?skip={max(0, skip - limit)}&limit={limit}"
        return jobs


class JobDetail(BaseResource):
    @app.require_oauth("read-job")
    def get(self, id):
        job = self.abort_if_item_doesnt_exist("jobs", id)
        if "parent_job_id" in job and job["parent_job_id"] == "":
            sub_jobs = self.storage.get_items_from_collection_by_fields(
                "jobs", {"parent_job_id": job["_key"]}, limit=job["amount_of_jobs"]
            )
            job["sub_jobs"] = sub_jobs["results"]
        return job

    @app.require_oauth("patch-job")
    def patch(self, id):
        self.abort_if_item_doesnt_exist("jobs", id)
        content = self.get_request_body()
        job = self.storage.patch_item_from_collection("jobs", id, content)
        return job, 201

    @app.require_oauth("update-job")
    def put(self, id):
        self.abort_if_item_doesnt_exist("jobs", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("Job", content, job_schema)
        job = self.storage.update_item_from_collection("jobs", id, content)
        return job, 201

    @app.require_oauth("delete-job")
    def delete(self, id):
        self.abort_if_item_doesnt_exist("jobs", id)
        self.storage.delete_item_from_collection("jobs", id)
        return "", 204
