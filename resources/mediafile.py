import app

from flask import request
from resources.base_resource import BaseResource
from validator import MediafileValidator

validator = MediafileValidator()


class Mediafile(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json(validator, "Mediafile", content)
        mediafile = self.storage.save_item_to_collection("mediafiles", content)
        return mediafile, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        mediafiles = self.storage.get_items_from_collection("mediafiles", skip, limit)
        count = mediafiles["count"]
        mediafiles["limit"] = limit
        if skip + limit < count:
            mediafiles["next"] = "/{}?skip={}&limit={}".format(
                "entities", skip + limit, limit
            )
        if skip > 0:
            mediafiles["previous"] = "/{}?skip={}&limit={}".format(
                "mediafiles", max(0, skip - limit), limit
            )
        return mediafiles


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
        content = self.get_request_body()
        mediafile = self.storage.patch_item_from_collection("mediafiles", id, content)
        return mediafile, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json(validator, "Mediafile", content)
        mediafile = self.storage.update_item_from_collection("mediafiles", id, content)
        return mediafile, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        self.storage.delete_item_from_collection("mediafiles", id)
        return "", 204
