import app
import os
import uuid

from flask import jsonify
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
        global importer
        importer = Importer(self.storage, self.storage_api_url)

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        upload_location = self.storage.get_item_from_collection_by_id("config", "0")[
            "upload_location"
        ]
        message_id = str(uuid.uuid4())
        message = {
            "message_id": message_id,
            "data": {
                "upload_folder": os.path.join(
                    upload_location, request_body["upload_folder"]
                )
            },
        }

        app.ramq.send(message, routing_key="dams.import_start", exchange_name="dams")
        return message, 201


class ImporterDirectories(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        upload_location = self.storage.get_item_from_collection_by_id("config", "0")[
            "upload_location"
        ]
        directories = [
            str(x[0]).removeprefix(upload_location) for x in os.walk(upload_location)
        ]
        return jsonify(directories)


class ImporterSources(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        request_body["identifiers"] = ["0"]
        request_body["upload_location"] = ""
        self.storage.delete_item_from_collection("config", "0")
        config = self.storage.save_item_to_collection("config", request_body)
        return config["upload_sources"], 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        if config := self.storage.get_item_from_collection_by_id("config", "0"):
            return config["upload_sources"]
        else:
            return []


class ImporterLocation(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        request_body["identifiers"] = ["0"]
        config = self.storage.patch_item_from_collection("config", "0", request_body)
        return config["upload_location"], 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        if config := self.storage.get_item_from_collection_by_id("config", "0"):
            return config["upload_location"]
        else:
            return ""
