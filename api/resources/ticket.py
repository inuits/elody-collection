from datetime import UTC, datetime

from elody.error_codes import ErrorCode, get_error_code, get_write
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from policy_factory import authenticate
from resources.generic_object import (
    GenericObjectDetailV2,
    GenericObjectV2,
)


class Ticket(GenericObjectV2):
    @authenticate(RequestContext(request))
    def post(self):
        content = request.get_json()
        content["type"] = "ticket"
        if "filename" not in content:
            abort(
                400,
                message=f"{get_error_code(ErrorCode.NO_FILENAME_SPECIFIED, get_write())} - No filename was specified",
            )
        ticket_id = self._create_ticket(content["filename"])
        return ticket_id, 201


class TicketDetail(GenericObjectDetailV2):
    @authenticate(RequestContext(request))
    def get(self, id):
        ticket = super().get("abstracts", id) or ({},)
        is_expired = datetime.now(tz=UTC).timestamp() >= float(ticket[0]["exp"])
        ticket[0]["is_expired"] = is_expired
        return ticket
