import app

from flask import g
from flask_restful import reqparse
from resources.base_resource import BaseResource
from resources.jobs import generate_file_signature

import werkzeug.datastructures


class JobStatusById(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, job_id):
        return self.abort_if_item_doesnt_exist("jobs", job_id)


class JobsByAsset(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, asset):
        return self.abort_if_item_doesnt_exist("jobs", asset)


class JobStatusByUser(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        user = "user"
        return self.abort_if_item_doesnt_exist("jobs", user)


# Upload single file
class JobUploadSingleItem(BaseResource):
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
        self.req = parse.parse_args()
        signature = generate_file_signature(self.req.get("asset"))
        return self.create_single_job(signature)


# Upload multiple files
class JobUploadMultipleItem(BaseResource):
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
            action="append",  # grabs multiple files
        )
        parse.add_argument("info", required=True)
        self.req = parse.parse_args()
        return self.create_multiple_jobs()
