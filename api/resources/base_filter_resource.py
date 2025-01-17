from filters.filter_manager import FilterManager
from filters_v2.filter_manager import FilterManager as FilterManagerV2
from flask import after_this_request, request
from flask_restful import abort
from resources.base_resource import BaseResource


class BaseFilterResource(BaseResource):
    def __init__(self):
        super().__init__()
        self.filter_engine = FilterManager().get_filter_engine()
        self.filter_engine_v2 = FilterManagerV2().get_filter_engine()

    def _execute_advanced_search_with_query(
        self, query, collection="entities", order_by=None, asc=True
    ):
        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        if not self.filter_engine:
            abort(500, message="Failed to init search engine")
        self.validate_advanced_query_syntax(query)

        items = self.filter_engine.filter(query, skip, limit, collection, order_by, asc)
        if skip + limit < items["count"]:
            items["next"] = f"/{collection}/filter?skip={skip + limit}&limit={limit}"
        if skip > 0:
            items["previous"] = (
                f"/{collection}/filter?skip={max(0, skip - limit)}&limit={limit}"
            )
        items["results"] = self._inject_api_urls_into_entities(items["results"])
        return items

    def _execute_advanced_search_with_query_v2(
        self, query, collection="entities", *, skip=None, limit=None
    ):
        order_by = request.args.get("order_by", None)
        asc = bool(request.args.get("asc", 1, int))
        skip = skip if skip is not None else request.args.get("skip", 0, int)
        limit = limit if limit is not None else request.args.get("limit", 20, int)

        @after_this_request
        def add_header(response):
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

        if not self.filter_engine:
            abort(500, message="Failed to init search engine")

        items = self.filter_engine_v2.filter(
            query, skip, limit, collection, order_by, asc
        )
        if skip + limit < items["count"]:
            items["next"] = f"/{collection}/filter?skip={skip + limit}&limit={limit}"
        if skip > 0:
            items["previous"] = (
                f"/{collection}/filter?skip={max(0, skip - limit)}&limit={limit}"
            )
        # items["results"] = self._inject_api_urls_into_entities(items["results"])
        # this should be done when serializing ^
        return items

    def _execute_advanced_search_with_saved_search(
        self, id, collection="entities", order_by=None, asc=True
    ):
        saved_search = self._abort_if_item_doesnt_exist("abstracts", id)
        self._abort_if_not_valid_type(saved_search, "saved_search")
        return self._execute_advanced_search_with_query(
            saved_search["definition"], collection, order_by, asc
        )

    def validate_advanced_query_syntax(self, queries):
        if not isinstance(queries, list):
            abort(
                400,
                message="Filter not passed as an array",
            )
        for query in queries:
            if query.get("type") == "MinMaxInput":
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
