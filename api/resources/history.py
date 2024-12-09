from elody.error_codes import ErrorCode, get_error_code, get_read
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies
from resources.base_resource import BaseResource


class History(BaseResource):
    @apply_policies(RequestContext(request))
    def get(self, collection, id):
        timestamp = request.args.get("timestamp")
        all_entries = request.args.get("all", 0, int)
        if timestamp and all_entries:
            abort(
                400,
                message=f"{get_error_code(ErrorCode.CANNOT_SPECIFY_BOTH, get_read())} - Can't specify both 'timestamp' and 'all'",
            )
        history_object = self.storage.get_history_for_item(
            collection, id, timestamp, all_entries
        )
        if not history_object:
            abort(
                404,
                message=f"{get_error_code(ErrorCode.HISTORY_ITEM_NOT_FOUND, get_read())} - Could not find history object",
            )
        return history_object
