from filters_v2.helpers.base_helper import (
    get_distinct_by,
    get_facets,
    get_options_requesting_filter,
    get_type_filter_value,
    has_non_exact_match_filter,
    has_selection_filter_with_multiple_values,
)
from filters_v2.helpers.mongo_helper import get_filter_option_label
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


class MongoFilters:
    def __init__(self):
        self.storage = StorageManager().get_db_engine()

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
    ):
        with tracer.start_as_current_span("MongoFilters.filter") as mongo_filter_span:
            BaseMatchers.collection = collection
            BaseMatchers.type = get_type_filter_value(filter_request_body)
            options_requesting_filter = get_options_requesting_filter(filter_request_body)
            BaseMatchers.force_base_nested_matcher_builder = bool(
                options_requesting_filter
                or has_non_exact_match_filter(filter_request_body)
                or has_selection_filter_with_multiple_values(filter_request_body)
            )
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

        return pipeline, match, group

    def __execute_aggregation_query(
        self,
        pipeline,
        match,
        group,
        skip,
        limit,
        options_requesting_filter,
        facets_request,
    ):
        with tracer.start_as_current_span("MongoFilters.__execute_aggregation_query") as mongo_execute_span:
            try:
                cursor = self.storage.db[BaseMatchers.collection].aggregate(
                    pipeline, allowDiskUse=self.storage.allow_disk_use
                )
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
                output, match, group, skip, limit, options_requesting_filter
            )

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
            count = self.storage.db[BaseMatchers.collection].aggregate(
                [*match, *group, {"$count": "count"}],
                allowDiskUse=self.storage.allow_disk_use,
            )
            items["count"] = next(count, {"count": len(output["results"])})["count"]
            for document in output["results"]:
                items["results"].append(
                    self.storage._prepare_mongo_document(document, True)
                )

        return items
