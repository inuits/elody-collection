import mappers

from app import policy_factory
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.base_resource import BaseResource


class Tenant(BaseResource):
    @policy_factory.authenticate(RequestContext(request))
    def get(self):
        accept_header = request.headers.get("Accept")
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)
        skip_relations = request.args.get("skip_relations", 0, int)
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)

        filters = {"type": "tenant"}
        access_restricting_filters = (
            policy_factory.get_user_context().access_restrictions.filters
        )
        if isinstance(access_restricting_filters, dict):
            filters = {**filters, **access_restricting_filters}

        entities = self.storage.get_entities(
            skip,
            limit,
            skip_relations,
            filters,
            order_by,
            ascending,
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                entities,
                accept_header,
                "entities",
                [],
            ),
            accept_header,
        )
