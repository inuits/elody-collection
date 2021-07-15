import app
import os
import uuid

from flask import jsonify, g
# from jobstatus.app.model import Job
from resources.base_resource import BaseResource
from workers.importer import Importer


importer = None


@app.ramq.queue(exchange_name="dams", routing_key="dams.import_start")
def csv_import(body):
    upload_folder = body["data"]["upload_folder"]
    importer.import_from_csv(upload_folder)
    return True


class ImporterStart(BaseResource):
    def __init__(self):
        super().__init__()
        self.upload_folder = os.getenv("UPLOAD_FOLDER", "/mnt/media-import")
        global importer
        importer = Importer(self.storage)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        message_id = str(uuid.uuid4())
        message = {
            "message_id": message_id,
            "data": {
                "upload_folder": os.path.join(
                    self.upload_folder, request_body["upload_folder"]
                )
            },
        }

        """
        user = json.dumps(g.oidc_token_info["sub"])
        job = Job(
            user=user.name,
            job_info="",
            status="queued",
            mediafile_id=message_id,
            mediafile=os.path.join(self.upload_folder, request_body["upload_folder"]),
        )

        job.save()
        """

        app.ramq.send(message, routing_key="dams.import_start", exchange_name="dams")
        return message


class ImporterDirectories(BaseResource):
    def __init__(self):
        super().__init__()
        self.upload_folder = os.getenv("UPLOAD_FOLDER", "/mnt/media-import")

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        directories = [
            str(x[0]).removeprefix(self.upload_folder)
            for x in os.walk(self.upload_folder)
        ]
        return jsonify(directories)
