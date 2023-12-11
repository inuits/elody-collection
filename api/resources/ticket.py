from app import policy_factory
from datetime import datetime, timezone
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.generic_object import (
    GenericObject,
    GenericObjectDetail,
)


class Ticket(GenericObject):
    @policy_factory.apply_policies(RequestContext(request))
    def post(self):
        ticket_id = self._create_ticket(content=request.get_json())
        return ticket_id, 201


class TicketDetail(GenericObjectDetail):
    @policy_factory.apply_policies(RequestContext(request))
    def get(self, id):
        ticket = super().get("abstracts", id) or {}
        is_expired = datetime.now(tz=timezone.utc).timestamp() >= float(ticket["exp"])
        ticket["is_expired"] = is_expired
        return ticket
