from configuration import get_object_configuration_mapper
from copy import deepcopy
from filters_v2.helpers.base_helper import (
    get_options_requesting_filter,
    get_type_filter_value,
    has_non_exact_match_filter,
    has_selection_filter_with_multiple_values,
)
from filters_v2.helpers.mongo_helper import (
    append_matcher,
    get_filter_option_label,
    get_options_mapper,
    unify_matchers_per_schema_into_one_match,
)
from filters_v2.matchers.base_matchers import BaseMatchers
from filters_v2.types.filter_types import get_filter
from logging_elody.log import log
from pymongo import ASCENDING, DESCENDING
from storage.mongostore import MongoStorageManager


class MongoFilters(MongoStorageManager):
    def filter(
        self,
        filter_request_body,
        skip,
        limit,
        collection="entities",
        order_by=None,
        asc=True,
        return_query_without_executing=False,
        tidy_up_match=True,
    ):
        BaseMatchers.collection = collection
        BaseMatchers.type = get_type_filter_value(filter_request_body)
        options_requesting_filter = get_options_requesting_filter(filter_request_body)
        BaseMatchers.force_base_nested_matcher_builder = bool(
            options_requesting_filter
            or has_non_exact_match_filter(filter_request_body)
            or has_selection_filter_with_multiple_values(filter_request_body)
        )

        pipeline, match_stage = self.__generate_aggregation_query(
            filter_request_body,
            skip,
            limit,
            order_by,
            asc,
            options_requesting_filter,
            tidy_up_match,
        )
        if return_query_without_executing:
            return pipeline
        return self.__execute_aggregation_query(
            pipeline, match_stage, skip, limit, options_requesting_filter
        )

    def __generate_aggregation_query(
        self,
        filter_request_body,
        skip,
        limit,
        order_by,
        asc,
        options_requesting_filter,
        tidy_up_match,
    ):
        pipeline = []
        pipeline.extend(self.__lookup_stage(filter_request_body))
        match_stage = self.__match_stage(filter_request_body, tidy_up_match)
        pipeline.append(match_stage)

        if options_requesting_filter_keys := options_requesting_filter.get("key"):
            pipeline.extend(self.__project_stage(options_requesting_filter_keys))
        else:
            if order_by:
                pipeline.extend(self.__sort_stage(order_by, asc))
            pipeline.extend([{"$skip": skip}, {"$limit": limit}])
        return pipeline, match_stage

    def __execute_aggregation_query(
        self, pipeline, match_stage, skip, limit, options_requesting_filter
    ):
        try:
            documents = self.db[BaseMatchers.collection].aggregate(
                pipeline, allowDiskUse=self.allow_disk_use
            )
        except Exception as exception:
            log.exception(
                f"{exception.__class__.__name__}: {exception}",
                {},
                exc_info=exception,
                info_labels={"pipeline": pipeline},
            )
            raise exception

        document = {"results": list(documents)}
        return self.__get_items(
            document, match_stage, skip, limit, options_requesting_filter
        )

    def __lookup_stage(self, filter_request_body):
        lookups = []
        for filter_criteria in filter_request_body:
            lookup = filter_criteria.get("lookup")
            if lookup:
                lookups.append(
                    {
                        "$lookup": {
                            "from": lookup["from"],
                            "localField": lookup["local_field"],
                            "foreignField": lookup["foreign_field"],
                            "as": lookup["as"],
                        }
                    }
                )
        return lookups

    def __match_stage(self, filter_request_body: list[dict], tidy_up_match):
        matchers_per_schema = {"general": []}

        for filter_criteria in filter_request_body:
            filter = get_filter(filter_criteria["type"])

            if isinstance(filter_criteria.get("key"), list):
                for key in filter_criteria["key"]:
                    schema, key = key.split("|")
                    filter_criteria_for_schema = deepcopy(filter_criteria)
                    filter_criteria_for_schema["key"] = key

                    matcher = filter.generate_query(filter_criteria_for_schema)

                    if matchers := matchers_per_schema.get(schema):
                        append_matcher(
                            matcher,
                            matchers,
                            filter_criteria_for_schema.get("operator", "and"),
                        )
                    else:
                        schema_type, schema_version = schema.split(":")
                        if schema == "elody:1":
                            matchers = [
                                {
                                    "$or": [
                                        {"schema": {"$exists": False}},
                                        {
                                            "$and": [
                                                {"schema.type": schema_type},
                                                {"schema.version": int(schema_version)},
                                            ]
                                        },
                                    ]
                                }
                            ]
                        else:
                            matchers = [
                                {"schema.type": schema_type},
                                {"schema.version": int(schema_version)},
                            ]

                        append_matcher(
                            matcher,
                            matchers,
                            filter_criteria_for_schema.get("operator", "and"),
                        )
                        matchers_per_schema.update({schema: matchers})

            else:
                matcher = filter.generate_query(filter_criteria)
                append_matcher(
                    matcher,
                    matchers_per_schema["general"],
                    filter_criteria.get("operator", "and"),
                )

            item_types = filter_criteria.get("item_types", [])
            if len(item_types) > 0:
                matchers_per_schema["general"].append({"type": {"$in": item_types}})

        return {
            "$match": unify_matchers_per_schema_into_one_match(
                matchers_per_schema, tidy_up_match
            )
        }

    def __sort_stage(self, order_by, asc):
        key_order_map = {order_by: ASCENDING if asc else DESCENDING}
        sorting = (
            get_object_configuration_mapper()
            .get(BaseMatchers.collection)
            .crud()
            .get("sorting", lambda *_: [])(key_order_map)
        )

        if len(sorting) > 0:
            return sorting
        else:
            return [
                {
                    "$sort": {
                        self.get_sort_field(order_by): (
                            ASCENDING if asc else DESCENDING
                        )
                    }
                }
            ]

    def __project_stage(self, options_requesting_filter_keys):
        mappers = []

        if isinstance(options_requesting_filter_keys, list):
            for key in options_requesting_filter_keys:
                _, key = key.split("|")
                if options_mapper := get_options_mapper(key):
                    mappers.append(options_mapper)
        else:
            if options_mapper := get_options_mapper(options_requesting_filter_keys):
                mappers.append(options_mapper)

        return [
            {"$project": {"_id": 0, "options": {"$concatArrays": mappers}}},
            {"$unwind": "$options"},
            {"$group": {"_id": "options", "options": {"$addToSet": "$options"}}},
        ]

    def __get_items(self, document, match, skip, limit, options_requesting_filter=None):
        items = {"results": [], "count": 0}

        if options_requesting_filter:
            if len(document["results"]) > 0:
                for option in document["results"][0]["options"]:
                    if isinstance(option, list):
                        items["results"].extend(option)
                    else:
                        if option.get("value") is not None:
                            items["results"].append(option)
                for option in items["results"]:
                    if key := options_requesting_filter.get("metadata_key_as_label"):
                        option["label"] = get_filter_option_label(
                            self.db, option["value"], key
                        )
            items["count"] = len(items["results"])
        else:
            items["skip"] = skip
            items["limit"] = limit
            items["count"] = self.db[BaseMatchers.collection].count_documents(
                match["$match"]
            )
            for document in document["results"]:
                items["results"].append(
                    self._prepare_mongo_document(
                        document, True, BaseMatchers.collection
                    )
                )

        return items
