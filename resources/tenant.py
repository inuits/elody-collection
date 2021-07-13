from flask_restful import Resource, abort
from flask import request
from models.tenant import TenantModel
from validator import TenantValidator
from resources.base_resource import BaseResource

import app

validator = TenantValidator()


def abort_if_not_valid_tenant(tenant_json):
    if not validator.validate(tenant_json):
        abort(405, message="Tenant doesn't have a valid format".format(id))


class Tenant(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def post(self):
        request = self.get_request_body()
        abort_if_not_valid_tenant(request)
        tenant = self.storage.save_item_to_collection("tenants", request)
        return tenant, 201


class TenantDetail(BaseResource):
    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def get(self, id):
        tenant = self.abort_if_item_doesnt_exist("tenants", id)
        return tenant

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def patch(self, id):
        self.abort_if_item_doesnt_exist("tenants", id)
        request = self.get_request_body()
        asset = self.storage.patch_item_from_collection("tenants", id, request)
        return asset, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def put(self, id):
        self.abort_if_item_doesnt_exist("tenants", id)
        request = self.get_request_body()
        abort_if_not_valid_tenant(request)
        tenant = self.storage.update_item_from_collection("tenants", id, request)
        return tenant, 201

    @app.oidc.accept_token(
        require_token=BaseResource.token_required, scopes_required=["openid"]
    )
    def delete(self, id):
        self.abort_if_item_doesnt_exist("tenants", id)
        self.storage.delete_item_from_collection("tenants", id)
        return "", 204
