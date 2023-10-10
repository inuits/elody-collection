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
        pipeline = self.__generate_aggregation_pipeline(filter_request_body, collection)
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

        if any(
            "provide_value_options_for_key" in value
            and value["provide_value_options_for_key"] == True
            for value in filter_request_body
        ):
            parent_key = filter_request_body[-1]["parent_key"]
            _, document_value = BaseMatchers.get_document_key_value(parent_key)
            options = set()

            queried_items = [
                option
                for options in items["results"][0]["options"]
                for option in options
            ]
            for item in queried_items:
                if isinstance(item.get("value"), list):
                    for value in item.get("value", list()):
                        options.add(FilterOption(value, value))
                elif "value" in item:
                    options.add(FilterOption(item.get("value"), item[document_value]))
            items["results"] = [option.to_dict() for option in options]

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

        for filter_criteria in filter_request_body:
            filter = get_filter(filter_criteria["type"])
            item_types = filter_criteria.get("item_types", [])
            if len(item_types) > 0:
                pipeline.append({"$match": {"type": {"$in": item_types}}})

            pipeline.extend(filter.generate_query(filter_criteria))  # type: ignore

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

        pipeline.append({"$project": {"relationDocuments": 0, "numberOfRelations": 0}})
        return pipeline
