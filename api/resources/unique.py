from app import policy_factory
from elody.exceptions import DuplicateFileException
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource

class Unique(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, collection, unique_id):
        if self.storage.check_if_file_already_exists(collection, unique_id):
            return f"Item in {collection} with identifier {unique_id} already exists", 409
        return "", 200