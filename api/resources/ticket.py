from elody.error_codes import ErrorCode, get_error_code, get_read, get_write
from datetime import datetime, timezone
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
)


class Ticket(GenericObject):
    @apply_policies(RequestContext(request))
    def post(self):
        content = request.get_json()
        if "filename" not in content:
            abort(
                400,
                message=f"{get_error_code(ErrorCode.NO_FILENAME_SPECIFIED, get_write())} No filename was specified",
            )
        ticket_id = self._create_ticket(content["filename"])
        return ticket_id, 201


class TicketDetail(GenericObjectDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        ticket = super().get("abstracts", id) or {}
        is_expired = datetime.now(tz=timezone.utc).timestamp() >= float(ticket["exp"])
        ticket["is_expired"] = is_expired
        return ticket
