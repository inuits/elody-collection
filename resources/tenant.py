from flask_restful import Resource, abort
from flask import request
from flask_restful_swagger import swagger
from storage.arangostore import ArangoStorageManager
from storage.mongostore import MongoStorageManager
from models.tenant import TenantModel
from validator import TenantValidator
from resources.base_resource import BaseResource

storage = ArangoStorageManager()
#storage = MongoStorageManager()
validator = TenantValidator()

def abort_if_tenant_doesnt_exist(id):
    tenant = storage.get_tenant_by_id(id)
    if tenant is None:
        abort(404, message="Tenant {} doesn't exist".format(id))
    return tenant

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
        response = super().get_response_body()
        abort_if_not_valid_tenant(response)
        tenant = storage.save_tenant(response)
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
        response = super().get_response_body()
        abort_if_not_valid_tenant(response)
        tenant = storage.update_tenant(response)
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
        tenant = abort_if_tenant_doesnt_exist(id)
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
        tenant = abort_if_tenant_doesnt_exist(id)
        storage.delete_tenant(id)
        return 204

