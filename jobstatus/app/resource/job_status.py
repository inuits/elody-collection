import werkzeug
from flask import g, json
from flask_apispec import doc
from flask_jwt_extended import jwt_required, current_user
from flask_restful import Resource, reqparse, abort

# from app.config import oidc
from flask_restful_swagger import swagger
from werkzeug.exceptions import BadRequest

from app.config import api, oidc
from app.model import Job
from app.resource.upload_image import FileUpload


@jwt_required()
# @oidc.accept_token(require_token=True, scopes_required=['openid'])
@doc(description="Get All Jobs")
class Jobs(Resource):
    def get(self):
        try:

            jobs = Job.query.filter(Job.user == current_user.name).first()
            response = {
                "message": f"fetching all jobs for {current_user.name}",
                "jobs": jobs,
            }, 201
        except BadRequest:

            return response


 @oidc.accept_token(require_token=True, scopes_required=['openid'])
class GetJobById(Resource):
    def get(self, job_id):
        try:
            response = {
                "job": Job.query.filter(Job.job_id == job_id).first(),
                "message": f"Job Status for job ID  : {job_id} retrieved",
            }, 201
        except BadRequest:
            response = {"message": "Invalid Job ID", "status": 404}, 404
        return response



@oidc.accept_token(require_token=True, scopes_required=['openid'])
class PostJobs(Resource):
    def put(self, job):
        try:
            response = {}
        except BadRequest:
            response = {}
        return response


class JobStatusByUser(Resource):
    def get(self):
        # Set the response code to 201 and return custom headers
        user = g.oidc_token_info['sub']
        return Job.query.filter(Job.job.user==user.name)


def abort_if_doesnt_exist(job_id):
    job = Job.query.filter(Job.jon_id == job_id).first()
    if job is None:
        abort(404, message=f"Job not Found ")


parser = reqparse.RequestParser()




# TodoList
# shows a list of all todos, and lets you POST to add new tasks
class JobsByUser(Resource):
    def get(self, user):
        return Job.query.filter(Job.asset == user)

    def post(self):

        parser.add_argument(
            "file", type=werkzeug.FileStorage, location="files", action="append"
        )
        parser.add_argument(
            "description", required=True, help="File description is required"
        )
        args = parser.parse_args()
        message = FileUpload.post(args)
        return message


##
## Actually setup the Api resource routing here
##
api.add_resource(JobsByUser, "/jobs")
api.add_resource(GetJobById, "/jobs/<job_id>")
