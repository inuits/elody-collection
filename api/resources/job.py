import app

from flask import request
from resources.base_resource import BaseResource


class Job(BaseResource):
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
                "jobs",
                {"parent_job_id": job["identifiers"][0]},
                limit=job["amount_of_jobs"],
            )
            job["sub_jobs"] = sub_jobs["results"]
        return job
