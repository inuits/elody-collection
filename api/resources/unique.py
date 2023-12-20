from app import policy_factory
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource

class Unique(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, collection, unique_id):
        if self.storage.get_item_from_collection_by_id(collection, unique_id):
            return f"Item in {collection} with identifier {unique_id} already exists", 409
        return "", 200
    