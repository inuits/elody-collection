from flask_restful import Resource
from resources.base_resource import BaseResource
from flask_restful_swagger import swagger
from models.asset import AssetModel

import app

class Asset(BaseResource):
    @swagger.operation(
        notes="Creates a new asset",
        responseClass=AssetModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "An asset item",
                "required": True,
                "allowMultiple": False,
                "dataType": AssetModel.__name__,
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
        asset = self.storage.save_item_to_collection('assets', request_body)
        return asset, 201


class AssetDetail(BaseResource):
    @swagger.operation(
        notes="get a asset item by ID",
        responseClass=AssetModel.__name__,
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Asset not found"},
        ],
    )
    @app.oidc.accept_token(require_token=True, scopes_required=['openid'])
    def get(self, id):
        asset = self.abort_if_item_doesnt_exist('assets', id)
        return asset

    @swagger.operation(
        notes="Updates an existing asset",
        responseClass=AssetModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "An asset item",
                "required": True,
                "allowMultiple": False,
                "dataType": AssetModel.__name__,
                "paramType": "body",
            }
        ],
        responseMessages=[
            {"code": 201, "message": "Created."},
            {"code": 404, "message": "Asset not found"},
            {"code": 405, "message": "Invalid input"},
        ],
    )
    def put(self, id):
        self.authorize_request()
        request = self.get_request_body()
        asset = self.storage.update_item_from_collection('assets', id, request)
        return asset, 201

    def patch(self, id):
        self.authorize_request()
        request = self.get_request_body()
        asset = self.storage.patch_item_from_collection('assets', id, request)
        return asset, 201

    @swagger.operation(
        notes="delete a asset item by ID",
        responseMessages=[
            {"code": 204, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Asset not found"},
        ],
    )
    @app.oidc.accept_token(require_token=True, scopes_required=['openid'])
    def delete(self, id):
        asset = self.abort_if_item_doesnt_exist('assets', id)
        self.storage.delete_item_from_collection('assets', id)
        return 204
