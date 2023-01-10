import app
import sys

from storage.mongostore import MongoStorageManager


class MongoFilters(MongoStorageManager):
    def filter(self, output_type, body, skip, limit, collection="entities"):
        items = {"results": list()}
        pipeline = self.__generate_aggregation_pipeline(body)
        pipeline_count = pipeline + [{"$count": "count"}]
        count = self.db[collection].aggregate(pipeline_count)
        items["count"] = list(count)[0]["count"]
        pipeline += [
            {"$skip": skip},
            {"$limit": limit},
        ]
        documents = self.db[collection].aggregate(pipeline)
        for document in documents:
            items["results"].append(self._prepare_mongo_document(document, True))
        items["limit"] = limit
        return items

    def __generate_aggregation_pipeline(self, queries):
        pipeline = list()
        for query in queries:
            if query["type"] == "TextInput" and "value" in query:
                pipeline += self.__generate_text_input_query(query)
        return pipeline

    def __get_text_input_metadata_filter(self, query):
        metadata_match = {"$match": {"metadata": {"$elemMatch": {}}}}
        if "label" in query:
            metadata_match["$match"]["metadata"]["$elemMatch"]["label"] = {
                "$regex": query["label"],
                "$options": "i",
            }
        if "key" in query:
            metadata_match["$match"]["metadata"]["$elemMatch"]["key"] = query["key"]
        if "value" in query:
            metadata_match["$match"]["metadata"]["$elemMatch"]["value"] = {
                "$regex": query["value"],
                "$options": "i",
            }
        return metadata_match

    def __get_text_input_root_field_filter(self, query):
        root_field_match = {
            "$match": {
                query["key"]: {
                    "$regex": query["value"],
                    "$options": "i",
                }
            }
        }
        return root_field_match

    def __generate_text_input_query(self, query):
        root_fields = ["filename", "mimetype"]
        sub_pipeline = list()
        if "item_types" in query and len(query["item_types"]):
            sub_pipeline.append(
                {
                    "$match": {
                        "type": {
                            "$in": query["item_types"],
                        }
                    }
                }
            )
        if "key" in query and query["key"] in root_fields:
            sub_pipeline.append(self.__get_text_input_root_field_filter(query))
        else:
            sub_pipeline.append(self.__get_text_input_metadata_filter(query))
        return sub_pipeline

    def __get_multi_select_metadata_filter(
        self, query, prev_collection, type, ignore_previous_results
    ):
        pass

    def __generate_multi_select_input_query(
        self,
        query,
        counter,
        prev_collection,
        collection="entities",
        type=None,
        ignore_previous_results=False,
    ):
        pass

    def __text_input_filter_exception(self, query, counter, prev_collection):
        pass

    def __generate_min_max_relations_filter(
        self, query, counter, prev_collection, relation_types
    ):
        pass

    def __get_min_max_filter_query(self, relation_types, prev_collection, min, max):
        pass

    def __generate_min_max_metadata_filter(
        self, query, counter, prev_collection, metadata_field, item_types=None
    ):
        pass

    def __map_relation_types(self, relation_types):
        pass

    def __get_prev_collection_loop(self, prev_collection):
        pass
