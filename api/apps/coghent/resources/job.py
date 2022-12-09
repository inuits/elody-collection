import app

from apps.coghent.resources.base_resource import CoghentBaseResource
from flask import Blueprint
from flask_restful import Api
from resources.job import Job, JobDetail

api_bp = Blueprint("job", __name__)
api = Api(api_bp)


class CoghentJob(CoghentBaseResource, Job):
    @app.require_oauth("read-job")
    def get(self):
        return super().get()


class CoghentJobDetail(CoghentBaseResource, JobDetail):
    @app.require_oauth("read-job")
    def get(self, id):
        return super().get(id)


api.add_resource(CoghentJob, "/jobs")
api.add_resource(CoghentJobDetail, "/jobs/<string:id>")
