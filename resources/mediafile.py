from flask_restful import Resource
from resources.base_resource import BaseResource
from models.mediafile import MediafileModel

import app


class Mediafile(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request = self.get_request_body()
        mediafile = self.storage.save_item_to_collection("mediafiles", request)
        return mediafile, 201


class MediafileDetail(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        mediafile = self.abort_if_item_doesnt_exist("mediafiles", id)
        return mediafile

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        request = self.get_request_body()
        asset = self.storage.patch_item_from_collection("mediafiles", id, request)
        return asset, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        request = self.get_request_body()
        mediafile = self.storage.update_item_from_collection("mediafiles", id, request)
        return mediafile, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        self.storage.delete_item_from_collection("mediafiles", id)
        return "", 204
