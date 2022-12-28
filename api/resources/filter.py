import app

from resources.base_filter_resource import BaseFilterResource


class FilterEntities(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self):
        return self._execute_advanced_search("entities")


class FilterMediafiles(BaseFilterResource):
    @app.require_oauth("search-advanced")
    def post(self):
        return self._execute_advanced_search("mediafiles")
