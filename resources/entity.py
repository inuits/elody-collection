from flask_restful import Resource
from resources.base_resource import BaseResource
from flask_restful_swagger import swagger
from models.entity import EntityModel

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
    def post(self):
        self.authorize_request()
        request_body = self.get_request_body()
        entity = self.storage.save_item_to_collection('entities', request_body)
        return entity, 201

    @app.oidc.accept_token(require_token=True, scopes_required=['openid'])
    def get(self):
        entities = self.storage.get_items_from_collection('entities')
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
    @app.oidc.accept_token(require_token=True, scopes_required=['openid'])
    def get(self, id):
        entity = self.abort_if_item_doesnt_exist('entities', id)
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
    def put(self, id):
        self.authorize_request()
        request = self.get_request_body()
        entity = self.storage.update_item_from_collection('entities', id, request)
        return entity, 201

    def patch(self, id):
        self.authorize_request()
        request = self.get_request_body()
        entity = self.storage.patch_item_from_collection('entities', id, request)
        return entity, 201

    @swagger.operation(
        notes="delete a entity item by ID",
        responseMessages=[
            {"code": 204, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Entity not found"},
        ],
    )
    @app.oidc.accept_token(require_token=True, scopes_required=['openid'])
    def delete(self, id):
        entity = self.abort_if_item_doesnt_exist('entities', id)
        self.storage.delete_item_from_collection('entities', id)
        return 204

class EntityMetadata(BaseResource):
    @app.oidc.accept_token(require_token=True, scopes_required=['openid'])
    def get(self, id):
        metadata = self.storage.get_collection_item_metadata('entities', id)
        return metadata

class EntityMetadataKey(BaseResource):
    @app.oidc.accept_token(require_token=True, scopes_required=['openid'])
    def get(self, id, key):
        metadata = self.storage.get_collection_item_metadata('entities', id)
        return metadata
