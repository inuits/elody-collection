import app
import mappers

from app import policy_factory
from flask import request
from inuits_policy_based_auth import RequestContext
from resources.base_filter_resource import BaseFilterResource
from resources.base_resource import BaseResource


class Tenant(BaseFilterResource, BaseResource):
    @policy_factory.apply_policies(RequestContext(request))
    def get(self, spec="elody"):
        accept_header = request.headers.get("Accept")
        filters = self.get_filters_from_query_parameters(request)
        filters.append({"type": "type", "value": "tenant"})
        collection = app.object_configuration_mapper.get("tenant").crud()["collection"]
        items = self._execute_advanced_search_with_query_v2(filters, collection)
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                policy_factory.get_user_context().access_restrictions.post_request_hook(
                    items
                ),
                accept_header,
                "entities",
                [],
                spec,
                request.args,
            ),
            accept_header,
            spec=spec,
        )
