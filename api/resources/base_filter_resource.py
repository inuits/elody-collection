import app

from filters.filter_manager import FilterManager
from flask import request, after_this_request
from resources.base_resource import BaseResource


class BaseFilterResource(BaseResource):
    def __init__(self):
        super().__init__()
        self.filter_engine = FilterManager().get_filter_engine()

    def _execute_advanced_search(self, collection="entities"):
        body = request.get_json()
        app.logger.info(body)
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 20))

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        if not self.filter_engine:
            abort(500, message="Failed to init search engine")
        self.validate_advanced_query_syntax(body)

        filters = dict()
        ids = self.filter_engine.filter("", body, skip, limit, collection)
        if ids:
            filters["ids"] = list(ids)
        count = ids.extra["stats"]["fullCount"]
        items = self.storage.get_items_from_collection(collection, 0, limit, None, filters)
        items["count"] = count
        items["limit"] = limit
        if skip + limit < count:
            items[
                "next"
            ] = f"/{collection}/filter?skip={skip + limit}&limit={limit}"
        if skip > 0:
            items[
                "previous"
            ] = f"/{collection}/filter?skip={max(0, skip - limit)}&limit={limit}"
        items["results"] = self._inject_api_urls_into_entities(items["results"])
        return items

    def validate_advanced_query_syntax(self, queries):
        for query in queries:
            if query["type"] == "MinMaxInput":
                if "min" not in query["value"] and "max" not in query["value"]:
                    abort(
                        400,
                        message="MinMaxfilter must specify min and/or max value, none are specified",
                    )
                if (
                    "min" in query["value"]
                    and "max" in query["value"]
                    and query["value"]["min"] > query["value"]["max"]
                ):
                    abort(
                        400,
                        message="Min-value can not be bigger than max-value in MinMaxfilter",
                    )
