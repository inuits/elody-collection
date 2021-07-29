import app
import os
import uuid

from flask import jsonify
from resources.base_resource import BaseResource


class ImporterStart(BaseResource):
    def __init__(self):
        super().__init__()

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        config = self.abort_if_item_doesnt_exist("config", "0")
        upload_location = self.abort_if_location_not_set(config)
        message_id = str(uuid.uuid4())
        message = {
            "message_id": message_id,
            "data": {
                "upload_location": upload_location,
                "upload_folder": os.path.join(
                    upload_location, request_body["upload_folder"]
                ),
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
        config = self.abort_if_item_doesnt_exist("config", "0")
        upload_location = self.abort_if_location_not_set(config)
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
        self.storage.delete_item_from_collection("config", "0")
        config = self.storage.save_item_to_collection("config", request_body)
        return config["upload_sources"], 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        return self.abort_if_item_doesnt_exist("config", "0")["upload_sources"]


class ImporterLocation(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        config = self.abort_if_item_doesnt_exist("config", "0")
        request_body = self.get_request_body()
        config["upload_location"] = request_body["upload_location"]
        config = self.storage.patch_item_from_collection("config", "0", config)
        return config["upload_location"], 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        config = self.abort_if_item_doesnt_exist("config", "0")
        return self.abort_if_location_not_set(config)


class ImporterDrop(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        self.storage.drop_all_collections()
        return "", 201
