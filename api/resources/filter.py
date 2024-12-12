import mappers

from configuration import get_object_configuration_mapper
from filters_v2.filter_matcher_mapping import FilterMatcherMapping
from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies, get_user_context
from resources.base_filter_resource import BaseFilterResource
from configuration import get_storage_mapper


class FilterMatchers(BaseFilterResource):
    def get(self, spec="elody"):
        return {
            key: [matcher.__name__ for _, matcher in value.items()]
            for key, value in FilterMatcherMapping.mapping.items()
        }, 200


class FilterEntities(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        accept_header = request.headers.get("Accept")
        query: list = request.get_json()
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        entities = self._execute_advanced_search_with_query(
            query, "entities", order_by, ascending
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(entities),
                accept_header,
                "entities",
                fields,
                spec,
                request.args,
            ),
            accept_header,
        )


# currently only suitable when using generic policies
# end goal is to replace this with FilterGenericObjectsV2
class FilterEntitiesV2(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, spec="elody", request_json=None):
        if request.args.get("soft", 0, int):
            return "good", 200
        accept_header = request.headers.get("Accept")
        query: list = request.get_json() if request_json is None else request_json
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        type = ""
        for dict in query:
            if dict.get("type") == "type":
                type = dict.get("value")
        config = get_object_configuration_mapper().get(type)
        storage_type = config.crud()["storage_type"]
        collection = get_object_configuration_mapper().get(type).crud()["collection"]
        if storage_type == "http":
            http_storage = get_storage_mapper().get("http")
            filter = config.serialization(
                f"{spec}_filter", f"{config.SCHEMA_TYPE}_filter"
            )
            filters = filter(query)
            entities = http_storage.get_items_from_collection(
                self, collection, filters=filters
            )
        else:
            entities = self._execute_advanced_search_with_query_v2(query, "entities")
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(entities),
                accept_header,
                "entities",
                [],
                spec,
                request.args,
            ),
            accept_header,
        )


class FilterMediafilesV2(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        accept_header = request.headers.get("Accept")
        query: list = request.get_json()
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        entities = self._execute_advanced_search_with_query_v2(query, "mediafiles")
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(entities),
                accept_header,
                "mediafiles",
                [],
                spec,
                request.args,
            ),
            accept_header,
        )


class FilterEntitiesBySavedSearchId(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        entities = self._execute_advanced_search_with_saved_search(
            id, "entities", order_by, ascending
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                entities,
                accept_header,
                "entities",
                fields,
            ),
            accept_header,
        )


class FilterGenericObjects(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, collection, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        self._check_if_collection_name_exists(collection)
        accept_header = request.headers.get("Accept")
        query: list = request.get_json()
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        items = self._execute_advanced_search_with_query(
            query, collection, order_by, ascending
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                get_user_context().access_restrictions.post_request_hook(items),
                accept_header,
                "entities",  # specific collection name not relevant for this method
                fields,
                spec,
                request.args,
            ),
            accept_header,
        )


# currently only suitable when using generic policies
class FilterGenericObjectsV2(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, collection, spec="elody"):
        if request.args.get("soft", 0, int):
            return "good", 200
        config = get_object_configuration_mapper().get(collection)
        storage_type = config.crud()["storage_type"]
        if storage_type != "http":
            self._check_if_collection_name_exists(collection)
        accept_header = request.headers.get("Accept")
        query: list = request.get_json()
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        if storage_type == "http":
            http_storage = get_storage_mapper().get("http")
            filter = config.serialization(
                f"{spec}_filter", f"{config.SCHEMA_TYPE}_filter"
            )
            filters = filter(query)
            items = http_storage.get_items_from_collection(
                self, collection, filters=filters
            )
        else:
            items = self._execute_advanced_search_with_query_v2(query, collection)
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
        )


class FilterGenericObjectsBySavedSearchId(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, collection, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        self._check_if_collection_name_exists(collection)
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        items = self._execute_advanced_search_with_saved_search(
            id, collection, order_by, ascending
        )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                items,
                accept_header,
                collection,
                fields,
            ),
            accept_header,
        )


class FilterMediafiles(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self):
        if request.args.get("soft", 0, int):
            return "good", 200
        query = request.get_json()
        access_restricting_filters = get_user_context().access_restrictions.filters
        order_by = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        return self._execute_advanced_search_with_query(
            query, "mediafiles", order_by, ascending
        )


class FilterMediafilesBySavedSearchId(BaseFilterResource):
    @apply_policies(RequestContext(request))
    def post(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        return self._execute_advanced_search_with_saved_search(id, "mediafiles")
