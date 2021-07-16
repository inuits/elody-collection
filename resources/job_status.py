import json
import os

import werkzeug.datastructures

import app

from flask import request, g
from flask_restful import reqparse
from resources.base_resource import BaseResource
from storage.storagemanager import MongoStorageManager as storage
from werkzeug.exceptions import abort
from workers.jobs import CreateJobs


def abort_if_not_exist(option, target):
    """Checks if requested item exists - items can either be job ID, user, or asset"""
    job = storage().get_jobs_from_collection("jobs", target)
    if job:
        return job  # returns a pymongo cursor
    else:
        abort(404, f"job for {option} : {target} Not Found in jobs Collection")


class JobStatusById(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, job_id):
        job = abort_if_not_exist("job_id", job_id)
        for item in job:
            return item


class JobsByAsset(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, asset):
        jobs = list()
        data = abort_if_not_exist(option="user", target=asset)
        for job in data:
            jobs.append(job)
        return jobs


class JobStatusByUser(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        user = g.oidc_token_info["email"]
        jobs = list()
        data = abort_if_not_exist(option="user", target=user)
        for job in data:
            jobs.append(job)
        return jobs

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        post_data = request.get_json()
        if post_data:
            return CreateJobs(
                job_type="multiple" if isinstance(post_data, list) else "single",
                job=post_data,
            ).create_single_job()
        else:
            abort(405, "Request failed")


# Upload single file
class JobUploadSingleItem(BaseResource, CreateJobs):
    """Upload single file endpoint"""

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument(
            "asset",
            type=werkzeug.datastructures.FileStorage,
            location="files",
            help="Image is required",
            required=True,
        )
        parse.add_argument("info", required=True, help="File Information")
        self.job = parse.parse_args()
        self.job_type = "single"
        return self.create_single_job()

# Upload multiple files
class JobUploadMultipleItem(BaseResource, CreateJobs):
    """ Upload Multiple files"""

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument(
            "asset",
            type=werkzeug.datastructures.FileStorage,
            location="files",
            required=True,
            help="Files required",
            action="append",
        )
        parse.add_argument("info", required=True)
        self.job = parse.parse_args()
        self.job_type = "multiple"
        self.user = g.oidc_token_info["email"]
        return self.create_multiple_job()
