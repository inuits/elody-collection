import app

from flask import request
from flask_restful import Resource
from resources.base_resource import BaseResource
from storage.storagemanager import MongoStorageManager as storage
from werkzeug.exceptions import abort
from workers.jobs import CreateJobs


def abort_if_not_exist(option, target):
    """Checks if requested item exists - items can either be job ID, user, or asset"""
    job = storage().get_jobs_from_collection("jobs", option, target)
    if job:
        return job  # returns a pymongo cursor
    else:
        abort(404, f"job for this ID : {target} Not Found")


class JobStatusById(Resource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, job_id):
        job = abort_if_not_exist("job_id", job_id)
        for item in job:
            return {"fetched-job": item}


class JobStatusByAsset(Resource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, asset):
        abort_if_not_exist(option="asset", target=asset)


class JobStatusByUser(Resource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        user = app.oidc.user_getfield("name")
        abort_if_not_exist(option="user", target=user)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        post_data = request.get_json()
        if post_data:
            return CreateJobs(
                job_type="multiple" if isinstance(post_data, list) else "single",
                job=post_data,
            ).create_job()
        else:
            abort(405, "Request failed")
