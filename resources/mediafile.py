from flask_restful import Resource
from resources.base_resource import BaseResource
from flask_restful_swagger import swagger
from models.mediafile import MediafileModel

import app


class Mediafile(BaseResource):
    @swagger.operation(
        notes="Creates a new mediafile",
        responseClass=MediafileModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "A mediafile item",
                "required": True,
                "allowMultiple": False,
                "dataType": MediafileModel.__name__,
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
        request = self.get_request_body()
        mediafile = self.storage.save_item_to_collection("mediafiles", request)
        return mediafile, 201


class MediafileDetail(BaseResource):
    @swagger.operation(
        notes="get a mediafile item by ID",
        responseClass=MediafileModel.__name__,
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Mediafile not found"},
        ],
    )
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

    @swagger.operation(
        notes="Updates an existing mediafile",
        responseClass=MediafileModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "A mediafile item",
                "required": True,
                "allowMultiple": False,
                "dataType": MediafileModel.__name__,
                "paramType": "body",
            }
        ],
        responseMessages=[
            {"code": 201, "message": "Created."},
            {"code": 404, "message": "Mediafile not found"},
            {"code": 405, "message": "Invalid input"},
        ],
    )
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        request = self.get_request_body()
        mediafile = self.storage.update_item_from_collection("mediafiles", id, request)
        return mediafile, 201

    @swagger.operation(
        notes="delete a mediafile item by ID",
        responseMessages=[
            {"code": 204, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Mediafile not found"},
        ],
    )
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("mediafiles", id)
        self.storage.delete_item_from_collection("mediafiles", id)
        return "", 204
