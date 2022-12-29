import app

from flask import request
from resources.base_filter_resource import BaseFilterResource


class FilterEntities(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self):
        query = request.get_json()
        return self._execute_advanced_search_with_query(query, "entities")


class FilterEntitiesBySavedSearchId(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self, id):
        return self._execute_advanced_search_with_saved_search(id, "entities")


class FilterMediafiles(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self):
        query = request.get_json()
        return self._execute_advanced_search_with_query(query, "mediafiles")


class FilterMediafilesBySavedSearchId(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self, id):
        return self._execute_advanced_search_with_saved_search(id, "mediafiles")
