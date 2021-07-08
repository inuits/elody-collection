from flask import jsonify
from resources.base_resource import BaseResource
import json
from workers.importer import Importer
from storage.storagemanager import StorageManager
import os
import uuid

import app


storage = StorageManager().get_db_engine()
importer = Importer(storage)


@app.ramq.queue(exchange_name="dams", routing_key="dams.import_start")
def csv_import(body):
    body_dict = json.loads(body)
    upload_folder = body_dict["data"]["upload_folder"]
    importer.import_from_csv(upload_folder)
    return True


class ImporterStart(BaseResource):
    def __init__(self):
        super().__init__()
        self.upload_folder = os.getenv("UPLOAD_FOLDER", "/mnt/media-import")

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        message_id = str(uuid.uuid4())
        message = {
            "message_id": message_id,
            "data": {"upload_folder": self.upload_folder},
        }
        app.ramq.send(message, routing_key="dams.import_start", exchange_name="dams")
        return message

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        directories = [
            str(x[0]).removeprefix(self.upload_folder)
            for x in os.walk(self.upload_folder)
        ]
        return jsonify(directories)
