import app

from resources.base_resource import BaseResource
from validator import tenant_schema


class Tenant(BaseResource):
    @app.require_oauth("create-tenant")
    def post(self):
        content = self.get_request_body()
        self.abort_if_not_valid_json("Tenant", content, tenant_schema)
        tenant = self.storage.save_item_to_collection("tenants", content)
        return tenant, 201


class TenantDetail(BaseResource):
    @app.require_oauth("read-tenant")
    def get(self, id):
        return self.abort_if_item_doesnt_exist("tenants", id)

    @app.require_oauth("patch-tenant")
    def patch(self, id):
        self.abort_if_item_doesnt_exist("tenants", id)
        content = self.get_request_body()
        tenant = self.storage.patch_item_from_collection("tenants", id, content)
        return tenant, 201

    @app.require_oauth("update-tenant")
    def put(self, id):
        self.abort_if_item_doesnt_exist("tenants", id)
        content = self.get_request_body()
        self.abort_if_not_valid_json("Tenant", content, tenant_schema)
        tenant = self.storage.update_item_from_collection("tenants", id, content)
        return tenant, 201

    @app.require_oauth("delete-tenant")
    def delete(self, id):
        self.abort_if_item_doesnt_exist("tenants", id)
        self.storage.delete_item_from_collection("tenants", id)
        return "", 204
