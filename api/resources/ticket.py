from app import policy_factory
from datetime import datetime, timezone
from resources.base_resource import BaseResource


class TicketDetail(BaseResource):
    @policy_factory.authenticate()
    def get(self, id):
        user = self._get_user()
        ticket = self._abort_if_item_doesnt_exist("abstracts", id) or {}
        self._abort_if_no_access(ticket, user, collection="abstracts")

        is_expired = datetime.now(tz=timezone.utc).timestamp() >= float(ticket["exp"])
        ticket["is_expired"] = is_expired

        return ticket
