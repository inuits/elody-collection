import mappers
import re

from configuration import get_object_configuration_mapper, get_storage_mapper
from copy import deepcopy
from filters_v2.filter_matcher_mapping import FilterMatcherMapping
from filters_v2.helpers.base_helper import (
    get_options_requesting_filter,
    get_selection_type_filter_value,
    get_type_filter_value,
)
from flask import request
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies, get_user_context
from resources.base_filter_resource import BaseFilterResource
from werkzeug.exceptions import BadRequest

from tracing import get_tracer

tracer = get_tracer()


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
                (
                    get_user_context().access_restrictions.post_request_hook(entities)
                    if not get_options_requesting_filter(query)
                    else entities
                ),
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
        collection = (
            get_object_configuration_mapper()
            .get(type)
            .crud()
            .get("collection", "entities")
        )
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
            sort_by_order = request.args.get("order_by", None)
            ascending = request.args.get("asc", 1, int)
            if sort_by_order == "order":
                relationType = get_relation_type_from_body(query)
                if relationType:
                    entities["results"] = sort_items_by_relations(
                        entities["results"], relationType, ascending=ascending
                    )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                (
                    get_user_context().access_restrictions.post_request_hook(entities)
                    if not get_options_requesting_filter(query)
                    else entities
                ),
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
        all_mediafiles = request.args.get("all_mediafiles", 0, int)
        query: list = request.get_json()
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        if not all_mediafiles:
            filter_out_empty_mediafiles = [
                {
                    "type": "text",
                    "key": ["elody:1|original_filename"],
                    "value": "*",
                }
            ]
            query = query + filter_out_empty_mediafiles
        entities = self._execute_advanced_search_with_query_v2(query, "mediafiles")
        sort_by_order = request.args.get("order_by", None)
        ascending = request.args.get("asc", 1, int)
        if sort_by_order == "order":
            relationType = get_relation_type_from_body(query)
            if relationType:
                entities["results"] = sort_items_by_relations(
                    entities["results"], relationType, ascending=ascending
                )
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                (
                    get_user_context().access_restrictions.post_request_hook(entities)
                    if not get_options_requesting_filter(query)
                    else entities
                ),
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
                (
                    get_user_context().access_restrictions.post_request_hook(items)
                    if not get_options_requesting_filter(query)
                    else items
                ),
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
    @tracer.start_as_current_span("base.FilterGenericObjectsV2")
    def post(self, collection, spec="elody", is_type_required=False, content=[]):

        if request.args.get("soft", 0, int):
            return "good", 200
        query: list = content or request.get_json()
        document_type = get_type_filter_value(query)
        if not document_type:
            document_type = get_selection_type_filter_value(query)
            if len(document_type) > 0:
                document_type = document_type[0]
            elif is_type_required:
                raise BadRequest(
                    "Filter with type 'type', or a filter with type 'selection' and 'key' equal to 'type' is required"
                )
        config = get_object_configuration_mapper().get(document_type or collection)
        collection_key = (
            "collection" if not request.args.get("history") else "collection_history"
        )
        collection = config.crud().get(collection_key)
        # TODO: storage_type is determined from the first type in the list,
        # multi-collection filtering does not work across different storage types (e.g. http + mongo mix)
        storage_type = config.crud()["storage_type"]
        if storage_type != "http":
            self._check_if_collection_name_exists(collection)
        accept_header = request.headers.get("Accept")
        access_restricting_filters = get_user_context().access_restrictions.filters
        if access_restricting_filters:
            for filter in access_restricting_filters:
                query.insert(0, filter)
        if storage_type == "http":
            http_storage = get_storage_mapper().get(
                "http"
            )()  # pyright: ignore[reportOptionalCall]
            filter = config.serialization(
                f"{spec}_filter", f"{config.SCHEMA_TYPE}_filter"
            )
            filters = filter(query)
            skip = request.args.get("skip", 0, int)
            limit = request.args.get("limit", 20, int)
            items = http_storage.get_items_from_collection(
                collection,
                filters=filters,
                skip=skip,
                limit=limit,
            )
        else:
            type_values = _get_type_filter_list_value(query)
            if type_values and len(type_values) > 1:
                items = self._execute_multi_collection_filter(
                    query, type_values, collection_key
                )
            else:
                items = self._execute_advanced_search_with_query_v2(query, collection)
        return self._create_response_according_accept_header(
            mappers.map_data_according_to_accept_header(
                (
                    get_user_context().access_restrictions.post_request_hook(items)
                    if not get_options_requesting_filter(query)
                    else items
                ),
                accept_header,
                "entities",
                [],
                spec,
                request.args,
            ),
            accept_header,
        )

    def _execute_multi_collection_filter(
        self, query, type_values, collection_key="collection"
    ):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)

        collections_to_types = {}
        for type_value in type_values:
            config = get_object_configuration_mapper().get(type_value)
            collection = (
                config.crud().get(collection_key, "entities")
                if config
                else "entities"
            )
            collections_to_types.setdefault(collection, []).append(type_value)

        merged_results = []
        merged_count = 0
        merged = None

        for collection, types in collections_to_types.items():
            collection_query = deepcopy(query)
            for filter_criteria in collection_query:
                if filter_criteria.get("type") == "type":
                    filter_criteria["value"] = types
                    break
            result = self._execute_advanced_search_with_query_v2(
                collection_query, collection, skip=0, limit=skip + limit
            )
            merged_results.extend(result.get("results", []))
            merged_count += result.get("count", 0)
            if merged is None:
                merged = result

        if merged is not None:
            merged["results"] = merged_results[skip : skip + limit]
            merged["count"] = merged_count
            merged.pop("next", None)
            merged.pop("previous", None)
            return merged
        return {"results": [], "count": 0}


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


def _get_type_filter_list_value(query):
    for filter_criteria in query:
        if filter_criteria.get("type") == "type":
            value = filter_criteria.get("value")
            if isinstance(value, list):
                return value
    return None


def sort_items_by_relations(data, relation_type=None, ascending=True):
    def get_order_value(item):
        order_values = []
        for relation in item.get("relations", []):
            if relation_type is None or relation.get("type") == relation_type:
                for metadata in relation.get("metadata", []):
                    if metadata.get("key") == "order":
                        order_values.append(metadata.get("value", float("inf")))
        if not order_values:
            return float("inf")
        return min(order_values)

    return sorted(data, key=get_order_value, reverse=not ascending)


def get_relation_type_from_body(query):
    for query_item in query:
        if query_item.get("type") == "selection":
            key = query_item.get("key", [])[0]
            if key:
                return extract_value_from_key(key)

    return None


def extract_value_from_key(input_string, key="relations"):
    pattern = re.compile(rf"{key}\.([^.|]+)")
    match = pattern.search(input_string)
    if match:
        return match.group(1)

    return None
