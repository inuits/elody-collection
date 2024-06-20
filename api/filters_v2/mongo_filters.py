from configuration import get_object_configuration_mapper
from copy import deepcopy
from filters_v2.helpers.mongo_helper import (
    append_matcher,
    get_filter_option_label,
    get_options_mapper,
    get_options_requesting_filter,
    has_non_exact_match_filter,
    has_selection_filter_with_multiple_values,
    unify_matchers_per_schema_into_one_match,
)
from filters_v2.matchers.base_matchers import BaseMatchers
from filters_v2.types.filter_types import get_filter
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
    ):
        BaseMatchers.collection = collection
        options_requesting_filter = get_options_requesting_filter(filter_request_body)
        BaseMatchers.force_base_nested_matcher_builder = bool(
            options_requesting_filter
            or has_non_exact_match_filter(filter_request_body)
            or has_selection_filter_with_multiple_values(filter_request_body)
        )

        lookup = self.__lookup_stage(filter_request_body)
        match = self.__match_stage(filter_request_body)
        facet = self.__facet_stage(
            skip,
            limit,
            order_by,
            asc,
            options_requesting_filter.get("key"),
        )

        pipeline = [*lookup, match, facet]
        documents = self.db[collection].aggregate(
            pipeline, allowDiskUse=self.allow_disk_use
        )
        document = list(documents)[0]
        return self.__get_items(document, limit, skip, options_requesting_filter)

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

    def __match_stage(self, filter_request_body: list[dict]):
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

        return {"$match": unify_matchers_per_schema_into_one_match(matchers_per_schema)}

    def __facet_stage(
        self, skip, limit, order_by, asc, options_requesting_filter_keys=None
    ):
        results = []
        if options_requesting_filter_keys:
            results.extend(self.__project_stage(options_requesting_filter_keys))
        else:
            if order_by:
                results.extend(self.__sort_stage(order_by, asc))
            results.append({"$skip": skip})
            results.append({"$limit": limit})

        return {
            "$facet": {
                "results": results,
                "count": [{"$count": "count"}],
            }
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

    def __get_items(self, document, limit, skip, options_requesting_filter):
        items = {"results": [], "count": 0}

        if options_requesting_filter:
            if len(document["results"]) > 0:
                for option in document["results"][0]["options"]:
                    if isinstance(option, list):
                        items["results"].extend(option)
                    else:
                        items["results"].append(option)
                for option in items["results"]:
                    if key := options_requesting_filter.get("metadata_key_as_label"):
                        option["label"] = get_filter_option_label(
                            self.db, option["value"], key
                        )
            items["count"] = len(items["results"])
        else:
            items["limit"] = limit
            items["skip"] = skip
            items["count"] = (
                document["count"][0]["count"] if len(document["count"]) > 0 else 0
            )
            for document in document["results"]:
                items["results"].append(
                    self._prepare_mongo_document(
                        document, True, BaseMatchers.collection
                    )
                )

        return items
