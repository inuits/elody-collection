import app
import mappers

from flask import request
from resources.base_filter_resource import BaseFilterResource


class FilterEntities(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self):
        accept_header = request.headers.get("Accept")
        query = request.get_json()
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        entities = self._execute_advanced_search_with_query(query, "entities")
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
    @app.require_oauth("search-advanced")
    def post(self, id):
        accept_header = request.headers.get("Accept")
        fields = [
            *request.args.getlist("field"),
            *request.args.getlist("field[]"),
        ]
        entities = self._execute_advanced_search_with_saved_search(id, "entities")
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
    @app.require_oauth("search-advanced")
    def post(self):
        query = request.get_json()
        return self._execute_advanced_search_with_query(query, "mediafiles")


class FilterMediafilesBySavedSearchId(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self, id):
        return self._execute_advanced_search_with_saved_search(id, "mediafiles")
