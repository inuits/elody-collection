from flask_restful import Resource, abort
from flask import request
from flask_restful_swagger import swagger
from models.tenant import TenantModel
from validator import TenantValidator
from resources.base_resource import BaseResource

validator = TenantValidator()

def abort_if_not_valid_tenant(tenant_json):
    if not validator.validate(tenant_json):
        abort(405, message="Tenant doesn't have a valid format".format(id))

class Tenant(BaseResource):
    @swagger.operation(
        notes="Creates a new tenant",
        responseClass=TenantModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "A tenant item",
                "required": True,
                "allowMultiple": False,
                "dataType": TenantModel.__name__,
                "paramType": "body",
            }
        ],
        responseMessages=[
            {"code": 201, "message": "Created."},
            {"code": 405, "message": "Invalid input"},
        ],
    )
    def post(self):
        response = self.get_response_body()
        abort_if_not_valid_tenant(response)
        tenant = self.storage.save_tenant(response)
        return tenant, 201

    @swagger.operation(
        notes="Updates an existing tenant",
        responseClass=TenantModel.__name__,
        parameters=[
            {
                "name": "body",
                "description": "A tenant item",
                "required": True,
                "allowMultiple": False,
                "dataType": TenantModel.__name__,
                "paramType": "body",
            }
        ],
        responseMessages=[
            {"code": 201, "message": "Created."},
            {"code": 404, "message": "Tenant not found"},
            {"code": 405, "message": "Invalid input"},
        ],
    )
    def put(self):
        response = self.get_response_body()
        abort_if_not_valid_tenant(response)
        tenant = self.storage.update_tenant(response)
        return tenant, 201

class TenantDetail(BaseResource):
    @swagger.operation(
        notes="get a tenant item by ID",
        responseClass=TenantModel.__name__,
        responseMessages=[
            {"code": 200, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Tenant not found"},
        ],
    )
    def get(self, id):
        tenant = self.abort_if_item_doesnt_exist('tenants', id)
        return tenant

    @swagger.operation(
        notes="delete a tenant item by ID",
        responseMessages=[
            {"code": 204, "message": "successful operation"},
            {"code": 400, "message": "Invalid ID supplied"},
            {"code": 404, "message": "Tenant not found"},
        ],
    )
    def delete(self, id):
        tenant = self.abort_if_item_doesnt_exist('tenants', id)
        self.storage.delete_tenant(id)
        return 204

