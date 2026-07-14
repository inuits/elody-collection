from os import getenv
from time import monotonic

from filters_v2.helpers.base_helper import (
    get_distinct_by,
    get_facets,
    get_options_requesting_filter,
    get_selection_type_filter_value,
    get_type_filter_value,
    has_non_exact_match_filter,
    has_selection_filter_with_multiple_values,
)
from filters_v2.helpers.mongo_helper import (
    get_bucket_stages,
    get_filter_option_label,
    has_bucket_filter,
)
from filters_v2.matchers.base_matchers import BaseMatchers
from filters_v2.stages import (
    facet_stage,
    group_stage,
    limit_stage,
    match_stage,
    project_stage,
    skip_stage,
    sort_stage,
)
from logging_elody.log import log
from storage.storagemanager import StorageManager
from tracing import get_tracer

tracer = get_tracer()

# How long a collection's distinct type set is cached before refetching. The set
# only changes when a brand new entity type is introduced (deploy time), so a
# few minutes is safe and keeps the count short-circuit cheap.
DISTINCT_TYPES_TTL_SECONDS = 300

# A filtered listing count is an index scan over every matching key. A broad
# filter matches millions of keys, so counting them all dominates the request
# (6-27s on prod). Stop at this cap ($limit short-circuits the scan) and let the
# UI render "<cap>+"; the unfiltered total stays exact via estimated_document_count.
# The cap also bounds the navigable page count (frontend derives pages from the
# count), so 1000 keeps ~50 pages of 20 reachable while staying ~1ms; users after
# something deeper are expected to narrow the filter. Set 0 to never cap.
LISTING_COUNT_CAP = int(getenv("LISTING_COUNT_CAP") or 0)


def get_type_only_filter_values(match: list, group: list):
    """Return the ``type`` values when a count reduces to a single type-only $match.

    The count pipeline is ``[*match, *group, {"$count": "count"}]``. When the only
    predicate is on ``type`` (and no ``$group`` narrows the set), the count equals
    the number of documents carrying one of those types — which, if the types
    cover the whole collection, is just the collection size. Returns the list of
    type values in that case, otherwise ``None`` (caller falls back to ``$count``).
    """
    if group:
        return None
    if len(match) != 1 or "$match" not in match[0]:
        return None
    predicate = match[0]["$match"]
    if set(predicate.keys()) != {"type"}:
        return None
    value = predicate["type"]
    if isinstance(value, dict):
        if set(value.keys()) != {"$in"} or not isinstance(value["$in"], list):
            return None
        return list(value["$in"])
    if isinstance(value, str):
        return [value]
    return None


class MongoFilters:
    def __init__(self):
        self.storage = StorageManager().get_db_engine()
        # collection -> (set_of_types, monotonic_timestamp)
        self._distinct_types_cache: dict = {}

    @tracer.start_as_current_span("base.MongoFilters.filter")
    def filter(
        self,
        filter_request_body,
        skip,
        limit,
        collection="entities",
        order_by="",
        asc=True,
        return_query_without_executing=False,
        tidy_up_match=True,
        return_cursor=False,
    ):
        entity_type = get_type_filter_value(filter_request_body)
        if not entity_type:
            entity_type = get_selection_type_filter_value(filter_request_body)
            if len(entity_type) > 0:
                entity_type = entity_type[0]
        options_requesting_filter = get_options_requesting_filter(filter_request_body)
        force_base_nested_matcher_builder = bool(
            options_requesting_filter
            or has_non_exact_match_filter(filter_request_body)
            or has_selection_filter_with_multiple_values(filter_request_body)
        )

        with BaseMatchers.context(
            collection=collection,
            type_name=entity_type,
            force_base=force_base_nested_matcher_builder,
        ):
            facets_request = get_facets(filter_request_body)

            pipeline, match, group = self.__build_aggregation_query(
                filter_request_body,
                skip,
                limit,
                order_by,
                asc,
                options_requesting_filter,
                facets_request,
                tidy_up_match,
            )
            if return_query_without_executing:
                return pipeline

            return self.__execute_aggregation_query(
                pipeline,
                match,
                group,
                skip,
                limit,
                options_requesting_filter,
                facets_request,
                return_cursor,
            )

    def __build_aggregation_query(
        self,
        filter_request_body: list[dict],
        skip,
        limit,
        order_by: str,
        asc: bool,
        options_requesting_filter: dict,
        facets_request: list[dict],
        tidy_up_match: bool,
    ):
        match = match_stage.build(filter_request_body, tidy_up_match)
        group = group_stage.build(get_distinct_by(filter_request_body))
        if options_requesting_filter:
            project = project_stage.build(
                options_requesting_filter=options_requesting_filter, match=match
            )
            pipeline = [*match, *project]
        else:
            sort = sort_stage.build(order_by, asc, filter_request_body, self.storage)
            skip = skip_stage.build(skip)
            limit = limit_stage.build(limit) if limit != -1 else []
            if facets_request:
                facet = facet_stage.build(facets_request, sort, skip, limit)
                project = project_stage.build(facet=facet[-1]["$facet"])
                pipeline = [*match, *facet, *project]
            else:
                pipeline = [*match, *group, *sort, *skip, *limit]
        if geo_bucket_filter := has_bucket_filter(filter_request_body):
            bucket_group, replace_root = get_bucket_stages(geo_bucket_filter)
            pipeline = [*match, *bucket_group, *replace_root]

        return pipeline, match, group

    @tracer.start_as_current_span("base.MongoFilters.__execute_aggregation_query")
    def __execute_aggregation_query(
        self,
        pipeline,
        match,
        group,
        skip,
        limit,
        options_requesting_filter,
        facets_request,
        return_cursor=False,
    ):
        try:
            with tracer.start_as_current_span(
                "base.MongoFilters.__execute_aggregation_query.aggregate"
            ) as aggregation_span:
                cursor = self.storage.db[BaseMatchers.collection].aggregate(
                    pipeline, allowDiskUse=self.storage.allow_disk_use
                )
                if return_cursor:
                    return cursor
        except Exception as exception:
            log.exception(
                f"{exception.__class__.__name__}: {exception}",
                {},
                exc_info=exception,
                info_labels={"pipeline": pipeline},
            )
            raise exception

        if facets_request:
            output = next(cursor)
            output = {"results": output["results"], "facets": output["facets"]}
        else:
            output = {"results": list(cursor)}

        return self.__get_items(
            output,
            match,
            group,
            skip,
            limit,
            options_requesting_filter,
        )

    @tracer.start_as_current_span("base.MongoFilters.__get_items")
    def __get_items(
        self,
        output,
        match,
        group,
        skip,
        limit,
        options_requesting_filter=None,
    ):
        items = {"results": [], "count": 0, "facets": output.get("facets", [])}

        if options_requesting_filter:
            if len(output["results"]) > 0:
                for option in output["results"][0]["options"]:
                    if isinstance(option, list):
                        items["results"].extend(option)
                    else:
                        if option.get("value") is not None:
                            items["results"].append(option)
                seen = set()
                items["results"] = [
                    option
                    for option in items["results"]
                    if option["value"] not in seen and not seen.add(option["value"])
                ]
                extra_options = []
                for option in items["results"]:
                    if key := options_requesting_filter.get("metadata_key_as_label"):
                        labels = get_filter_option_label(
                            self.storage.db, option["value"], key
                        )
                        if isinstance(labels, list):
                            option["label"] = labels.pop()
                            for label in labels:
                                extra_options.append({**option, "label": label})
                        else:
                            option["label"] = labels
                items["results"].extend(extra_options)
            items["count"] = len(items["results"])
        else:
            items["skip"] = skip
            items["limit"] = limit
            items["count"] = self.__count(BaseMatchers.collection, match, group, output)
            for document in output["results"]:
                items["results"].append(
                    self.storage._prepare_mongo_document(document, True)
                )

        return items

    def __count(self, collection, match, group, output):
        """Count matching documents, avoiding a full scan for large result sets.

        Two short-circuits keep this off the hot path:
        - Whole-collection filter (covers every type): the answer is the
          collection size, read from metadata in O(1) via estimated_document_count.
        - Any other (filtered) listing: the count is an index scan over every
          matching key, which dominates the request on large filters. Cap it with
          a ``$limit`` before ``$count`` so the scan stops early; a returned count
          above the cap means "<cap>+". Disable with LISTING_COUNT_CAP=0.
        """
        type_values = get_type_only_filter_values(match, group)
        if type_values is not None:
            collection_types = self.__get_collection_types(collection)
            if collection_types and collection_types <= set(type_values):
                return self.storage.db[collection].estimated_document_count()

        cap_stage = [{"$limit": LISTING_COUNT_CAP + 1}] if LISTING_COUNT_CAP > 0 else []
        count = self.storage.db[collection].aggregate(
            [*match, *group, *cap_stage, {"$count": "count"}],
            allowDiskUse=self.storage.allow_disk_use,
        )
        return next(count, {"count": len(output["results"])})["count"]

    def __get_collection_types(self, collection):
        """Return the collection's distinct ``type`` values, cached with a TTL.

        ``distinct`` is served by the ``type``-prefixed index (DISTINCT_SCAN), and
        the result is cached so the count short-circuit stays cheap on every call.
        """
        cached = self._distinct_types_cache.get(collection)
        now = monotonic()
        if cached and now - cached[1] < DISTINCT_TYPES_TTL_SECONDS:
            return cached[0]
        types = set(self.storage.db[collection].distinct("type"))
        self._distinct_types_cache[collection] = (types, now)
        return types
