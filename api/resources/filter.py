import app

from resources.base_filter_resource import BaseFilterResource


class FilterEntities(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self):
        query = self._get_request_body()
        return self._execute_advanced_search_with_query(query, "entities")


class FilterEntitiesBySavedSearchId(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self, id):
        return self._execute_advanced_search_with_saved_search(id, "entities")


class FilterMediafiles(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self):
        query = self._get_request_body()
        return self._execute_advanced_search_with_query(query, "mediafiles")


class FilterMediafilesBySavedSearchId(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self, id):
        return self._execute_advanced_search_with_saved_search(id, "mediafiles")
