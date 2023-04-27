from filters.types.filter_types import get_filter
from storage.mongostore import MongoStorageManager


class MongoFilters(MongoStorageManager):
    def filter(self, filter_request_body, skip, limit, collection="entities"):
        items = {"count": 0, "results": list()}
        pipeline = self.__generate_aggregation_pipeline(filter_request_body, collection)
        pipeline_count = pipeline + [{"$count": "count"}]
        count = list(self.db[collection].aggregate(pipeline_count))

        if len(count) == 0:
            return items
        items["count"] = count[0]["count"]
        pipeline += [
            {"$skip": skip},
            {"$limit": limit},
        ]

        documents = self.db[collection].aggregate(pipeline)
        for document in documents:
            items["results"].append(self._prepare_mongo_document(document, True))
        items["limit"] = limit

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
            if filter_criteria.get("parent"):
                pipeline.append(
                    {
                        "$match": {
                            "relations": {
                                "$elemMatch": {
                                    "key": filter_criteria["parent"],
                                    "type": "parent",
                                }
                            }
                        }
                    }
                )

            pipeline.extend(filter.generate_query(filter_criteria))

        pipeline.append({"$project": {"relationDocuments": 0, "numberOfRelations": 0}})
        return pipeline
