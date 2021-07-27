from json import JSONDecodeError

import app
import json
import os

import requests

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
        )
        self.req.add_argument("info", required=True, help="File Information")
        message = self.create_single_job(self.req.parse_args().get("asset"))
        if message.status_code == 201:
            app.ramq.send(
                message,
                exchange_name=os.getenv("EXCHANGE_NAME", "dams"),
                routing_key=os.getenv("ROUTING_KEY", "dams.job_status"),
            )
        return message


# Upload multiple files
class JobUploadMultipleItem(BaseResource):
    """Upload Multiple files"""

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


@app.ramq.queue(
    exchange_name=os.getenv("EXCHANGE_NAME", "dams"),
    routing_key=os.getenv("ROUTING_KEY", "dams.job_status"),
)
class StartJobs(BaseResource):
    storage_api = os.getenv("STORAGE_API_URL", "http://localhost:8001")

    def job_status(self, body):
        try:
            data = json.loads(body)
            print(data)

            # fetch job from collection
            job = self.storage.get_jobs_from_collection("jobs", data["job_id"])
            job["status"] = "In-Progress"
            # Update Job Status
            self.storage.patch_item_from_collection("jobs", job["job_id"], job)
            # process multiple jobs
            if data["job_type"] == "multiple":
                for data in data["data"]:
                    save = self.process_data(data["job_folder"])
                    job["status"] = "Finished" if save.status_code == 201 else "Failed"
            else:
                # process single jobs
                save_file = self.process_data(+data["asset"])
                job["status"] = "Finished" if save_file.status_code == 201 else "Failed"
            # Update Job Status
            self.storage.patch_item_from_collection("jobs", job["job_id"], job)
        except JSONDecodeError as e:
            pass

        return True

    def process_data(self, path):
        file_name = os.path.basename(StartJobs.mount_point + path)
        return requests.post(
            f"{StartJobs.storage_api}/upload/{os.path.basename(file_name)}"
        )
