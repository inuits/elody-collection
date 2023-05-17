from app import policy_factory
from resources.base_resource import BaseResource


class UserPermissions(BaseResource):
    @policy_factory.authenticate()
    def get(self):
        return policy_factory.get_user_context().scopes
