from configuration import get_object_configuration_mapper
from filters_v2.filter_manager import FilterManager as FilterManagerV2
from flask import after_this_request, request
from flask_restful import abort
from logging_elody.log import log
from resources.base_resource import BaseResource
from search.typesense_client import build_type_filter, ensure_collection as typesense_ensure_collection, search as typesense_search, search_all_ids as typesense_search_all_ids
from storage.storagemanager import StorageManager
from tracing import get_tracer

tracer = get_tracer()


class BaseFilterResource(BaseResource):
    def __init__(self):
        super().__init__()
        self.filter_engine_v2 = FilterManagerV2().get_filter_engine()

    def _classify_filters_for_typesense(self, query):
        """Split query filters into text, type, and remaining categories."""
        text_filters = []
        type_filter_values = []
        remaining_filters = []
        for f in query:
            if (
                f.get("type") == "text"
                and not f.get("match_exact")
                and f.get("value")
                and f.get("value") != "*"
            ):
                text_filters.append(f)
            elif f.get("type") == "type":
                val = f.get("value")
                if isinstance(val, list):
                    type_filter_values.extend(val)
                elif val:
                    type_filter_values.append(val)
            elif f.get("type") == "selection" and f.get("key") == "type":
                val = f.get("value")
                if isinstance(val, list):
                    type_filter_values.extend(val)
                elif val:
                    type_filter_values.append(val)
            else:
                remaining_filters.append(f)
        return text_filters, type_filter_values, remaining_filters

    def _resolve_mongo_collections(self, type_filter_values, default_collection):
        """Map entity types to their MongoDB collections."""
        collections = set()
        if type_filter_values:
            mapper = get_object_configuration_mapper()
            for t in type_filter_values:
                try:
                    collections.add(mapper.get(t).crud().get("collection"))
                except Exception:
                    collections.add(default_collection)
        return collections or {default_collection}

    def _build_typesense_query(self, text_filters, type_filter_values, typesense_config):
        """Build Typesense search parameters from classified filters."""
        ts_collection = typesense_config.get("collection", "entities")
        typesense_ensure_collection(ts_collection)
        search_fields = typesense_config.get("search_fields", [])
        query_by = ",".join(field.replace(".", "_") for field in search_fields)
        search_terms = " ".join(
            f.get("value", "") for f in text_filters if f.get("value")
        )
        filter_by = build_type_filter(type_filter_values)
        return ts_collection, query_by, search_terms, filter_by

    def _execute_typesense_search(
        self, ts_collection, search_terms, query_by, filter_by, has_remaining, skip, limit
    ):
        """Execute Typesense search with fallback. Returns None if unavailable."""
        if has_remaining:
            return typesense_search_all_ids(
                ts_collection, search_terms, query_by, filter_by=filter_by
            )
        return typesense_search(
            ts_collection, search_terms, query_by,
            filter_by=filter_by, per_page=limit, offset=skip,
        )

    def _fetch_documents_from_mongo(self, matching_ids, collections):
        """Fetch documents from multiple MongoDB collections, ordered by Typesense relevance."""
        storage = StorageManager().get_db_engine()
        id_query = {
            "$or": [
                {"_id": {"$in": matching_ids}},
                {"identifiers": {"$in": matching_ids}},
            ]
        }
        documents = []
        for col in collections:
            documents.extend(storage.db[col].find(id_query))
        id_order = {doc_id: i for i, doc_id in enumerate(matching_ids)}
        documents.sort(key=lambda doc: id_order.get(doc["_id"], float("inf")))
        return [storage._prepare_mongo_document(doc, True) for doc in documents]

    def _add_pagination_links(self, items, skip, limit, collection):
        """Add next/previous pagination links to response."""
        if skip + limit < items.get("count", 0):
            items["next"] = f"/{collection}/filter?skip={skip + limit}&limit={limit}"
        if skip > 0:
            items["previous"] = (
                f"/{collection}/filter?skip={max(0, skip - limit)}&limit={limit}"
            )
        if collection in [
            "entities",
            "mediafiles",
            "entities_actual",
            "bibliographic_entities_actual",
        ]:
            items["results"] = self._inject_api_urls_into_entities(items["results"])
        return items

    def _add_cors_headers(self):
        if request:
            @after_this_request
            def add_header(response):
                response.headers["Access-Control-Allow-Origin"] = "*"
                return response

    def _execute_advanced_search_with_query(
        self, query, collection="entities", order_by=None, asc=True
    ):
        from filters.filter_manager import FilterManager

        self.filter_engine = FilterManager().get_filter_engine()

        skip = request.args.get("skip", 0, int)
        limit = request.args.get("limit", 20, int)

        self._add_cors_headers()

        if not self.filter_engine:
            abort(500, message="Failed to init search engine")
        self.validate_advanced_query_syntax(query)

        items = self.filter_engine.filter(query, skip, limit, collection, order_by, asc)
        return self._add_pagination_links(items, skip, limit, collection)

    @tracer.start_as_current_span(
        "base.BaseFilterResource._execute_advanced_search_with_query_v2"
    )
    def _execute_advanced_search_with_query_v2(
        self, query, collection="entities", *, skip=None, limit=None
    ):
        order_by = request.args.get("order_by", None) if request else None
        asc = bool(request.args.get("asc", 1, int)) if request else 1
        if request:
            skip = skip if skip is not None else request.args.get("skip", 0, int)
            limit = limit if limit is not None else request.args.get("limit", 20, int)
        else:
            skip = skip if skip else 0
            limit = limit if limit else 20

        self._add_cors_headers()

        if not self.filter_engine_v2:
            abort(500, message="Failed to init search engine")

        items = self.filter_engine_v2.filter(
            query, skip, limit, collection, order_by, asc
        )
        return self._add_pagination_links(items, skip, limit, collection)

    @tracer.start_as_current_span(
        "base.BaseFilterResource._execute_typesense_accelerated_search"
    )
    def _execute_typesense_accelerated_search(
        self, query, collection, typesense_config
    ):
        if not typesense_config.get("enabled"):
            return self._execute_advanced_search_with_query_v2(query, collection)

        skip = request.args.get("skip", 0, int) if request else 0
        limit = request.args.get("limit", 20, int) if request else 20
        order_by = request.args.get("order_by", None) if request else None
        asc = bool(request.args.get("asc", 1, int)) if request else 1

        text_filters, type_filter_values, remaining_filters = (
            self._classify_filters_for_typesense(query)
        )

        if not text_filters:
            return self._execute_advanced_search_with_query_v2(query, collection)
        ts_collection, query_by, search_terms, filter_by = (
            self._build_typesense_query(text_filters, type_filter_values, typesense_config)
        )

        ts_result = self._execute_typesense_search(
            ts_collection, search_terms, query_by, filter_by,
            bool(remaining_filters), skip, limit,
        )
        if ts_result is None:
            log.info("Typesense unavailable, falling back to MongoDB")
            return self._execute_advanced_search_with_query_v2(query, collection)

        matching_ids, total_count = ts_result["ids"], ts_result["count"]

        if not matching_ids:
            return {"results": [], "count": total_count, "skip": skip, "limit": limit}

        if remaining_filters:
            remaining_filters.append(
                {"type": "selection", "key": "_id", "value": matching_ids, "match_exact": True}
            )
            items = self.filter_engine_v2.filter(
                remaining_filters, skip, limit, collection, order_by, asc
            )
        else:
            mongo_collections = self._resolve_mongo_collections(type_filter_values, collection)
            results = self._fetch_documents_from_mongo(matching_ids, mongo_collections)
            items = {"results": results, "count": total_count, "skip": skip, "limit": limit}

        self._add_cors_headers()
        return self._add_pagination_links(items, skip, limit, collection)

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
