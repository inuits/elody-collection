from app import policy_factory
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
)


class Job(GenericObject):
    @policy_factory.authenticate(RequestContext(request))
    def get(self):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        job_type = request.args.get("type")
        status = request.args.get("status")
        fields = {"parent_job_id": None}
        if job_type:
            fields["job_type"] = job_type
        if status:
            fields["status"] = status
        filters = {}
        if ids := request.args.get("ids"):
            filters["ids"] = ids.split(",")
            fields = {}
        jobs = self.storage.get_items_from_collection(
            "jobs", skip, limit, fields, filters, "start_time", False
        )
        jobs["limit"] = limit
        job_filter = f"&type={job_type}" if job_type else ""
        status_filter = f"&status={status}" if status else ""
        if skip + limit < jobs["count"]:
            jobs["next"] = (
                f"/jobs?skip={skip + limit}&limit={limit}{job_filter}{status_filter}"
            )
        if skip > 0:
            jobs["previous"] = (
                f"/jobs?skip={max(0, skip - limit)}&limit={limit}{job_filter}{status_filter}"
            )
        return jobs


class JobDetail(GenericObjectDetail):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        job = super().get("jobs", id)
        if job.get("parent_job_id"):
            return job
        fields = {"parent_job_id": job["identifiers"][0]}
        if status := request.args.get("status"):
            fields["status"] = status
        job["sub_jobs"] = self.storage.get_items_from_collection(
            "jobs", limit=job["amount_of_jobs"], fields=fields
        )
        return job
