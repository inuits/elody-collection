from flask_restful import Resource
from resources.base_resource import BaseResource
from flask_restful_swagger import swagger
from models.asset import AssetModel
from flask import request

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
        token = None
        if 'Authorization' in request.headers and request.headers['Authorization'].startswith('Bearer '):
            token = request.headers['Authorization'].split(None,1)[1].strip()
        validity = app.oidc.validate_token(token)
        if not validity:
            response_body = {'error': 'invalid_token',
                             'error_description': validity}
            return response_body, 401, {'WWW-Authenticate': 'Bearer'}
        request_body = self.get_request_body()
        asset = self.storage.save_item_to_collection('assets', request_body)
        return asset, 201

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
    def put(self):
        request = self.get_request_body()
        asset = self.storage.update_item_from_collection('assets', request)
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
