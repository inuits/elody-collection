import os

import app

from resources.base_resource import BaseResource

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
        self.req.add_argument(
            "asset",
            type=werkzeug.datastructures.FileStorage,
            location="files",
            help="Image is required",
            required=True,
        )
        self.req.add_argument("info", required=True, help="File Information")
        message = self.create_single_job()
        app.ramq.send(
            message,
            exchange_name=os.getenv("EXCHANGE_NAME", "dams"),
            routing_key=os.getenv("ROUTING_KEY", "dams.job_status"),
        )
        return message


# Upload multiple files
class JobUploadMultipleItem(BaseResource):
    """ Upload Multiple files"""

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        self.req.add_argument(
            "asset",
            type=werkzeug.datastructures.FileStorage,
            location="files",
            required=True,
            help="Files required",
            action="append",  # grabs multiple files
        )
        self.req.add_argument("info", required=True)
        message = self.create_multiple_jobs()
        app.ramq.send(
            message,
            exchange_name=os.getenv("EXCHANGE_NAME", "dams"),
            routing_key=os.getenv("ROUTING_KEY", "dams.job_status"),
        )
        return message
