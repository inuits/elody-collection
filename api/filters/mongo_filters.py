import pymongo

from filters.filter_option import FilterOption
from filters.matchers.base_matchers import BaseMatchers
from filters.types.filter_types import get_filter
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
        items = {"count": 0, "results": list()}
        pipeline, last_filter = self.__generate_aggregation_pipeline(
            filter_request_body, collection
        )
        pipeline_count = pipeline + [{"$count": "count"}]
        count = list(
            self.db[collection].aggregate(
                pipeline_count, allowDiskUse=self.allow_disk_use
            )
        )

        if len(count) == 0:
            return items
        items["count"] = count[0]["count"]
        if order_by:
            pipeline += [
                {
                    "$sort": {
                        self.get_sort_field(order_by): pymongo.ASCENDING
                        if asc
                        else pymongo.DESCENDING
                    }
                },
            ]
        pipeline += [
            {"$skip": skip},
            {"$limit": limit},
        ]
        documents = self.db[collection].aggregate(
            pipeline, allowDiskUse=self.allow_disk_use
        )
        for document in documents:
            items["results"].append(self._prepare_mongo_document(document, True))
        items["limit"] = limit

        self.__provide_value_options_for_key_if_necessary(
            collection, last_filter, items
        )
        return items

    def __generate_aggregation_pipeline(
        self, filter_request_body: list[dict], collection="entities"
    ):
        pipeline = []
        pipeline.append(
            {
                "$lookup": {
                    "from": collection,
                    "localField": "relations.key",
                    "foreignField": "_id",
                    "as": "relationDocuments",
                }
            }
        )

        filter_criteria = {}
        matchers = []
        operator = "$and"
        for filter_criteria in filter_request_body:
            filter = get_filter(filter_criteria["type"])
            if "operator" in filter_criteria:
                operator = f"${filter_criteria['operator']}"
            item_types = filter_criteria.get("item_types", [])
            if len(item_types) > 0:
                pipeline.append({"$match": {"type": {"$in": item_types}}})

            for matcher in filter.generate_query(filter_criteria):
                if "$match" in matcher:
                    matchers.append(matcher.get("$match"))
                else:
                    matchers.append(matcher)

            if filter_criteria.get("provide_value_options_for_key"):
                key = filter_criteria["key"]
                parent_key = filter_criteria["parent_key"]
                document_key, _ = BaseMatchers.get_document_key_value(parent_key)

                pipeline.extend(
                    [
                        {
                            "$project": {
                                "_id": 0,
                                key: {
                                    "$map": {
                                        "input": {
                                            "$filter": {
                                                "input": f"${parent_key}",
                                                "as": "item",
                                                "cond": {
                                                    "$eq": [
                                                        f"$$item.{document_key}",
                                                        key,
                                                    ]
                                                },
                                            }
                                        },
                                        "as": "filteredItem",
                                        "in": "$$filteredItem",
                                    }
                                },
                            }
                        },
                        {"$group": {"_id": None, "options": {"$addToSet": f"${key}"}}},
                        {"$project": {"_id": 0, "options": 1}},
                    ]
                )
                break

        if matchers and not filter_criteria.get(
            "provide_value_options_for_key", False
        ):
            pipeline.append({"$match": {operator: matchers}})
        pipeline.append({"$project": {"relationDocuments": 0, "numberOfRelations": 0}})
        return pipeline, filter_criteria

    def __provide_value_options_for_key_if_necessary(self, collection, filter, items):
        if not filter.get("provide_value_options_for_key", False):
            return
        parent_key = filter["parent_key"]
        _, document_value = BaseMatchers.get_document_key_value(parent_key)
        options = set()
        queried_items = [
            option for options in items["results"][0]["options"] for option in options
        ]
        for item in queried_items:
            if isinstance(item.get("value"), list):
                for value in item.get("value", list()):
                    options.add(FilterOption(value, value))
            elif "value" in item:
                label = item.get("value")
                if parent_key == "relations":
                    label = self.__get_filter_option_label(
                        collection,
                        item[document_value],
                        filter.get("metadata_key_as_label"),
                    )
                options.add(FilterOption(label, item[document_value]))
        items["results"] = [option.to_dict() for option in options]

    def __get_filter_option_label(self, collection, identifier, metadata_key_as_label):
        if not metadata_key_as_label:
            raise Exception(
                "Please provide 'metadata_key_as_label,' a metadata key whose value will be used as label for filter options."
            )
        return list(
            self.db[collection].aggregate(
                [
                    {"$match": {"identifiers": {"$in": [identifier]}}},
                    {
                        "$project": {
                            "_id": 0,
                            "label": {
                                "$arrayElemAt": [
                                    {
                                        "$map": {
                                            "input": {
                                                "$filter": {
                                                    "input": "$metadata",
                                                    "as": "item",
                                                    "cond": {
                                                        "$eq": [
                                                            "$$item.key",
                                                            metadata_key_as_label,
                                                        ]
                                                    },
                                                }
                                            },
                                            "as": "filteredItem",
                                            "in": "$$filteredItem.value",
                                        }
                                    },
                                    0,
                                ]
                            },
                        }
                    },
                ]
            )
        )[0]["label"]
