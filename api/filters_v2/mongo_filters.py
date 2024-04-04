import app
import pymongo

from copy import deepcopy
from elody.util import interpret_flat_key
from filters_v2.matchers.base_matchers import BaseMatchers
from filters_v2.types.filter_types import get_filter
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
        options_requesting_filter = self.__get_options_requesting_filter(
            filter_request_body
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
                        option["label"] = self.__get_filter_option_label(
                            option["value"], key, BaseMatchers.get_object_lists_config()
                        )
            items["count"] = len(items["results"])
        else:
            items["limit"] = limit
            items["skip"] = skip
            items["count"] = (
                document["count"][0]["count"] if len(document["count"]) > 0 else 0
            )
            for document in document["results"]:
                items["results"].append(self._prepare_mongo_document(document, True))
        return items

    def __get_options_requesting_filter(self, filter_request_body):
        options_requesting_filter = [
            filter_criteria
            for filter_criteria in filter_request_body
            if filter_criteria.get("provide_value_options_for_key")
        ]
        return (
            options_requesting_filter[0] if len(options_requesting_filter) > 0 else {}
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
                        len_matchers, index, early_exit_loop = len(matchers), 0, False
                        matcher_key = list(matcher.keys())[0]

                        while index < len_matchers and not early_exit_loop:
                            if matchers[index].get(matcher_key):
                                if matchers[index][matcher_key].get("$all"):
                                    matchers[index][matcher_key]["$all"].extend(
                                        matcher[matcher_key]["$all"]
                                    )
                                else:
                                    matchers[index][matcher_key] = matcher[matcher_key]
                                early_exit_loop = True
                            index += 1

                        if not early_exit_loop:
                            matchers.append(matcher)
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

                        matchers.append(matcher)
                        matchers_per_schema.update({schema: matchers})

            else:
                matcher = filter.generate_query(filter_criteria)
                matchers_per_schema["general"].append(matcher)

            item_types = filter_criteria.get("item_types", [])
            if len(item_types) > 0:
                matchers_per_schema["general"].append({"type": {"$in": item_types}})

        match = {}
        general_matchers = matchers_per_schema.pop("general")
        for matcher in general_matchers:
            match.update(matcher)
        if matchers_per_schema:
            match.update(
                {
                    "$or": [
                        {"$and": matchers} for matchers in matchers_per_schema.values()
                    ]
                }
            )
        return {"$match": match}

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
        key_order_map = {order_by: pymongo.ASCENDING if asc else pymongo.DESCENDING}
        sorting = (
            app.object_configuration_mapper.get(BaseMatchers.collection)
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
                            pymongo.ASCENDING if asc else pymongo.DESCENDING
                        )
                    }
                }
            ]

    def __project_stage(self, options_requesting_filter_keys):
        object_lists_config = BaseMatchers.get_object_lists_config()
        mappers = []

        if isinstance(options_requesting_filter_keys, list):
            for key in options_requesting_filter_keys:
                _, key = key.split("|")
                if options_mapper := self.__get_options_mapper(
                    key, object_lists_config
                ):
                    mappers.append(options_mapper)
        else:
            if options_mapper := self.__get_options_mapper(
                options_requesting_filter_keys, object_lists_config
            ):
                mappers.append(options_mapper)

        return [
            {"$project": {"_id": 0, "options": {"$concatArrays": mappers}}},
            {"$unwind": "$options"},
            {"$group": {"_id": "options", "options": {"$addToSet": "$options"}}},
        ]

    def __get_options_mapper(self, key, object_lists_config):
        keys_info = interpret_flat_key(key, object_lists_config)
        if len(keys_info) != 2:
            return {}

        return {
            "$map": {
                "input": {
                    "$filter": {
                        "input": f"${keys_info[0]['key']}",
                        "as": "object",
                        "cond": {
                            "$eq": [
                                f"$$object.{object_lists_config[keys_info[0]['key']]}",
                                keys_info[0]["object_key"],
                            ]
                        },
                    }
                },
                "as": "object",
                "in": {
                    "$cond": {
                        "if": {"$isArray": f"$$object.{keys_info[1]['key']}"},
                        "then": {
                            "$map": {
                                "input": f"$$object.{keys_info[1]['key']}",
                                "as": "item",
                                "in": {
                                    "label": "$$item",
                                    "value": "$$item",
                                },
                            }
                        },
                        "else": {
                            "label": f"$$object.{keys_info[1]['key']}",
                            "value": f"$$object.{keys_info[1]['key']}",
                        },
                    }
                },
            }
        }

    def __get_filter_option_label(self, identifier, key, object_lists_config):
        return next(
            self.db[BaseMatchers.collection].aggregate(
                [
                    {"$match": {"identifiers": {"$in": [identifier]}}},
                    {
                        "$project": {
                            "label": self.__get_options_mapper(key, object_lists_config)
                        }
                    },
                ]
            )
        )["label"][0]["label"]
