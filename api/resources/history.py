from app import policy_factory
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource


class History(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, collection, id):
        timestamp = request.args.get("timestamp")
        all_entries = request.args.get("all", 0, int)
        if timestamp and all_entries:
            abort(400, message="Can't specify both 'timestamp' and 'all'")
        history_object = self.storage.get_history_for_item(
            collection, id, timestamp, all_entries
        )
        if not history_object:
            abort(404, message="Could not find history object")
        return history_object
