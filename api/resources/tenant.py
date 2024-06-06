import mappers

from configuration import get_object_configuration_mapper
from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies, get_user_context
from resources.base_filter_resource import BaseFilterResource
from resources.base_resource import BaseResource


class Tenant(BaseFilterResource, BaseResource):
    @apply_policies(RequestContext(request))
    def get(self, spec="elody"):
        accept_header = request.headers.get("Accept")
        filters = self.get_filters_from_query_parameters(request)
        filters.append({"type": "type", "value": "tenant"})
        collection = (
            get_object_configuration_mapper().get("tenant").crud()["collection"]
        )
        items = self._execute_advanced_search_with_query_v2(filters, collection)
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(items),
                accept_header,
                "entities",
                [],
                spec,
                request.args,
            ),
            accept_header,
            spec=spec,
        )
