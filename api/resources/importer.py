import app
import os
import uuid

from flask import jsonify
from resources.base_resource import BaseResource


class ImporterStart(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        message_id = str(uuid.uuid4())
        upload_folder = os.path.join(
            self.upload_source, str(request_body["upload_folder"]).removeprefix("/")
        )
        message = {
            "message_id": message_id,
            "data": {
                "upload_source": self.upload_source,
                "upload_folder": upload_folder,
                "collection_api_url": self.collection_api_url,
                "storage_api_url": self.storage_api_url,
            },
        }
        app.ramq.send(message, routing_key="dams.import_start", exchange_name="dams")
        return message, 201


class ImporterDirectories(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        directories = [
            str(x[0]).removeprefix(self.upload_source)
            for x in os.walk(self.upload_source)
        ]
        return jsonify(directories)
