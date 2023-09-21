from app import policy_factory
from datetime import datetime, timezone
from flask import request
from flask_restful import abort
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource


class Ticket(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def post(self):
        content = request.get_json()
        if "filename" not in content:
            abort(400, message="No filename was specified")
        ticket_id = self._create_ticket(content["filename"])
        return ticket_id, 201


class TicketDetail(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self, id):
        ticket = self._abort_if_item_doesnt_exist("abstracts", id) or {}
        self._abort_if_no_access(ticket, collection="abstracts")
        is_expired = datetime.now(tz=timezone.utc).timestamp() >= float(ticket["exp"])
        ticket["is_expired"] = is_expired
        return ticket
