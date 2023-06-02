import mappers

from app import policy_factory
from flask import request
from filters.filter_matcher_mapping import FilterMatcherMapping
from resources.base_filter_resource import BaseFilterResource


class FilterMatchers(BaseFilterResource):
    @policy_factory.authenticate()
    def get(self):
        return {
            key: [matcher.__name__ for _, matcher in value.items()]
            for key, value in FilterMatcherMapping.mapping.items()
        }, 200


class FilterEntities(BaseFilterResource):
    @policy_factory.authenticate()
    def post(self):
        accept_header = request.headers.get("Accept")
        query = request.get_json()
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
                entities,
                accept_header,
                "entities",
                fields,
            ),
            accept_header,
        )


class FilterEntitiesBySavedSearchId(BaseFilterResource):
    @policy_factory.authenticate()
    def post(self, id):
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


class FilterMediafiles(BaseFilterResource):
    @policy_factory.authenticate()
    def post(self):
        query = request.get_json()
        return self._execute_advanced_search_with_query(query, "mediafiles")


class FilterMediafilesBySavedSearchId(BaseFilterResource):
    @policy_factory.authenticate()
    def post(self, id):
        return self._execute_advanced_search_with_saved_search(id, "mediafiles")
