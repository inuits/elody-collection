from app import policy_factory
from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext
from resources.job import Job, JobDetail

api_bp = Blueprint("job", __name__)
api = Api(api_bp)


class CoghentJob(CoghentBaseResource, Job):
    @policy_factory.apply_policies(RequestContext(request, ["read-job"]))
    def get(self):
        return super().get()


class CoghentJobDetail(CoghentBaseResource, JobDetail):
    @policy_factory.apply_policies(RequestContext(request, ["read-job"]))
    def get(self, id):
        return super().get(id)


api.add_resource(CoghentJob, "/jobs")
api.add_resource(CoghentJobDetail, "/jobs/<string:id>")
