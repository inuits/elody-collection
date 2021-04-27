from flask_restful import Resource
from resources.base_resource import BaseResource
from flask_restful_swagger import swagger
from models.entity import EntityModel
from flask import request

import app


class Entity(BaseResource):
    @swagger.operation(
        notes="Creates a new entity",
        responseClass=EntityModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "An entity item",
                "required": True,
                "allowMultiple": False,
                "dataType": EntityModel.__name__,
                "paramType": "body",
            }
        ],
        responseMessages=[
            {"code": 201, "message": "Created."},
            {"code": 405, "message": "Invalid input"},
        ],
    )
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request_body = self.get_request_body()
        entity = self.storage.save_item_to_collection("entities", request_body)
        return entity, 201

    @swagger.operation(
        notes="Get tenants",
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
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
    @swagger.operation(
        notes="get a entity item by ID",
        responseClass=EntityModel.__name__,
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        entity = self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
        return entity

    @swagger.operation(
        notes="Updates an existing entity",
        responseClass=EntityModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "An entity item",
                "required": True,
                "allowMultiple": False,
                "dataType": EntityModel.__name__,
                "paramType": "body",
            }
        ],
        responseMessages=[
            {"code": 201, "message": "Created."},
            {"code": 404, "message": "Entity not found"},
            {"code": 405, "message": "Invalid input"},
        ],
    )
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

    @swagger.operation(
        notes="Patch an existing entity",
        responseMessages=[
            {"code": 201, "message": "Created."},
            {"code": 404, "message": "Entity not found"},
            {"code": 405, "message": "Invalid input"},
        ],
    )
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

    @swagger.operation(
        notes="delete a entity item by ID",
        responseMessages=[
            {"code": 204, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("entities", id)
        self.storage.delete_item_from_collection("entities", id)
        return "", 204


class EntityMetadata(BaseResource):
    @swagger.operation(
        notes="Get specific entity metadata for a specific ID",
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
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

    @swagger.operation(
        notes="Add entity metadata for a specific ID",
        responseMessages=[
            {"code": 201, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
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

    @swagger.operation(
        notes="Update entity metadata for a specific ID",
        responseMessages=[
            {"code": 201, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
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
    @swagger.operation(
        notes="Get specific entity metadata for a specific entity ID",
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
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

    @swagger.operation(
        notes="Delete a specific entity metadata field for a specific ID",
        responseMessages=[
            {"code": 204, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
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
    @swagger.operation(
        notes="Get specific entity metadata for a specific entity ID",
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
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

    @swagger.operation(
        notes="Add entity mediafiles for a specific ID",
        responseMessages=[
            {"code": 204, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self, id):
        self.abort_if_item_doesnt_exist(
            "entities",
            id,
        )
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
        mediafile = self.storage.save_item_to_collection("mediafiles", dict())
        mediafile_id = mediafile["_id"]
        upload_location = "{}/upload/{}".format(self.storage_api_url, mediafile_id)
        location = {
            "location": "{}/download/{}".format(self.storage_api_url, mediafile_id)
        }
        self.storage.patch_item_from_collection("mediafiles", mediafile_id, location)
        self.storage.add_mediafile_to_entity("entities", id, mediafile_id)
        return upload_location, 201
