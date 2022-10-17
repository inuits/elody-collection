import app

from flask import request
from resources.base_resource import BaseResource


class Job(BaseResource):
    @app.require_oauth("read-job")
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        fields = {}
        filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
        elif job_type := request.args.get("type"):
            fields = {"job_type": job_type, "parent_job_id": None}
        else:
            fields = {"parent_job_id": None}
        jobs = self.storage.get_items_from_collection(
            "jobs", skip, limit, fields, filters, "start_time", False
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
        job = self._abort_if_item_doesnt_exist("jobs", id)
        if not job.get("parent_job_id", True):
            job["sub_jobs"] = self.storage.get_items_from_collection(
                "jobs",
                limit=job["amount_of_jobs"],
                fields={"parent_job_id": job["identifiers"][0]},
            )
        return job
