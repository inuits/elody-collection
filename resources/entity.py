from flask_restful import reqparse

import app

from flask import request
from resources.base_resource import BaseResource
import werkzeug.datastructures


class Entity(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        entity = self.storage.save_item_to_collection("entities", request_body)
        return entity, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))
        entities = self.storage.get_items_from_collection("entities", skip, limit)
        count = entities["count"]
        entities["limit"] = limit
        if skip + limit < count:
            entities["next"] = "/{}?skip={}&limit={}".format(
                "entities", skip + limit, limit
            )
        if skip > 0:
            entities["previous"] = "/{}?skip={}&limit={}".format(
                "entities", max(0, skip - limit), limit
            )
        return entities


class EntityDetail(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        entity = self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        return entity

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        request = self.get_request_body()
        entity = self.storage.update_item_from_collection("entities", id, request)
        return entity, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        request = self.get_request_body()
        entity = self.storage.patch_item_from_collection("entities", id, request)
        return entity, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_item_from_collection("entities", id)
        return "", 204


class EntityMetadata(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        metadata = self.storage.get_collection_item_metadata("entities", id)
        return metadata

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        request = self.get_request_body()
        metadata = self.storage.add_collection_item_metadata("entities", id, request)
        return metadata, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        request = self.get_request_body()
        metadata = self.storage.update_collection_item_metadata("entities", id, request)
        return metadata, 201


class EntityMetadataKey(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id, key):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        metadata = self.storage.get_collection_item_metadata_key("entities", id, key)
        return metadata

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id, key):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        self.storage.delete_collection_item_metadata_key("entities", id, key)
        return "", 204


class EntityMediafiles(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        mediafiles = self.storage.get_collection_item_mediafiles("entities", id)
        return mediafiles

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        # grab request data ---

        request_body = self.get_request_body()
        mediafile_id = request_body["_id"]

        mediafile = self.storage.add_mediafile_to_entity("entities", id, mediafile_id)
        return mediafile, 201


class EntityMediafilesCreate(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        self.req.add_argument(
            "filename",
            location="files",
            required=False,
            type=werkzeug.datastructures.FileStorage,
            help="filename to be uploaded",
        )

        parse_args = self.req.parse_args()
        file_name = parse_args.get('filename').filename
        media_file = {"filename": file_name, "file_extension": file_name}
        mediafile = self.storage.save_item_to_collection("mediafiles", media_file)
        mediafile_id = mediafile["_id"]
        upload_location = "{}/upload/{}".format(self.storage_api_url, mediafile_id)
        location = {
            "location": "{}/download/{}".format(self.storage_api_url, mediafile_id)
        }

        self.storage.patch_item_from_collection("mediafiles", mediafile_id, location)
        self.storage.add_mediafile_to_entity("entities", id, mediafile_id)
        return location, 201


class EntityRelationships(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openId"]
    )
    def get(self, entity_id):
        self.abort_if_item_doesnt_exist("entities", entity_id)
        return self.storage.get_entity_relationships("entities", entity_id)

    def post(self, entity_id):
        self.abort_if_item_doesnt_exist('entities', entity_id)

    def put(self, relations):
        pass

    def patch(self, relations):
        pass
