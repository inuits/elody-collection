import sys

from storage.mongostore import MongoStorageManager


class MongoFilters(MongoStorageManager):
    operator_map = {
        "==": "$eq",
        "!=": "$ne",
        ">": "$gt",
        "<": "$lt",
        ">=": "$gte",
        "<=": "$lte",
    }

    def filter(self, body, skip, limit, collection="entities"):
        items = {"count": 0, "results": list()}
        pipeline = self.__generate_aggregation_pipeline(body, collection)
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

    def __add_helper_queries(self, query):
        sub_pipeline = list()
        if len(query.get("item_types", [])):
            sub_pipeline.append(self.__get_item_types_query(query["item_types"]))
        if query.get("parent"):
            sub_pipeline.append(self.__get_parent_filter(query["parent"]))
        return sub_pipeline

    def __generate_aggregation_pipeline(self, queries, collection="entities"):
        pipeline = [
            {
                "$lookup": {
                    "from": collection,
                    "localField": "relations.key",
                    "foreignField": "_id",
                    "as": "relationDocuments",
                }
            }
        ]
        for query in queries:
            if query.get("type") == "TextInput" and "value" in query:
                pipeline += self.__generate_text_input_query(query)
            elif query.get("type") == "MultiSelectInput":
                pipeline += self.__generate_multi_select_input_query(query)
            elif query.get("type") == "MinMaxInput":
                if "metadata_field" in query:
                    pipeline += self.__generate_min_max_metadata_filter(query)
                elif "relation_types" in query:
                    pipeline += self.__generate_min_max_relations_filter(query)
        pipeline.append(
            {
                "$project": {
                    "relationDocuments": 0,
                    "numberOfRelations": 0,
                }
            }
        )
        return pipeline

    def __generate_min_max_metadata_filter(self, query):
        min = query.get("value", {}).get("min", -1)
        max = query.get("value", {}).get("max", sys.maxsize)
        metadata_min_max_match = {
            "$match": {
                "metadata": {
                    "$elemMatch": {
                        "key": f"{query['metadata_field']}_float",
                        "value": {
                            "$gte": min,
                            "$lte": max,
                        },
                    }
                }
            }
        }
        sub_pipeline = self.__add_helper_queries(query)
        sub_pipeline.append(metadata_min_max_match)
        return sub_pipeline

    def __generate_min_max_relations_filter(self, query):
        relation_types = self.__map_relation_types(query["relation_types"])
        min = query.get("value", {}).get("min", -1)
        max = query.get("value", {}).get("max", sys.maxsize)
        relation_match = {"$match": {"relations.type": {"$in": relation_types}}}
        number_of_relations_calculator = {
            "$addFields": {
                "numberOfRelations": {
                    "$size": {
                        "$filter": {
                            "input": "$relations",
                            "as": "el",
                            "cond": {"$in": ["$$el.type", relation_types]},
                        }
                    }
                }
            }
        }
        min_max_match = {"$match": {"numberOfRelations": {"$gte": min, "$lte": max}}}
        sub_pipeline = self.__add_helper_queries(query)
        sub_pipeline.extend(
            [relation_match, number_of_relations_calculator, min_max_match]
        )
        return sub_pipeline

    def __generate_multi_select_input_query(self, query):
        sub_pipeline = self.__add_helper_queries(query)
        sub_pipeline.append(self.__get_multi_select_filter(query))
        return sub_pipeline

    def __generate_text_input_query(self, query):
        root_fields = ["filename", "mimetype"]
        sub_pipeline = self.__add_helper_queries(query)
        if query.get("key") in root_fields:
            sub_pipeline.append(self.__get_text_input_root_field_filter(query))
        else:
            sub_pipeline.append(self.__get_text_input_metadata_filter(query))
        return sub_pipeline

    def __get_item_types_query(self, item_types):
        return {
            "$match": {
                "type": {
                    "$in": item_types,
                }
            }
        }

    def __get_multi_select_filter(self, query):
        match_field = (
            "metadata"
            if query["key"] in ["rights", "source", "publication_status"]
            else "relationDocuments.metadata"
        )
        multi_select_match = {
            "$match": {
                match_field: {
                    "$elemMatch": {
                        "value": {},
                    }
                }
            }
        }
        if query.get("key"):
            multi_select_match["$match"][match_field]["$elemMatch"]["key"] = query[
                "key"
            ]
        if len(query.get("value", [])):
            multi_select_match["$match"][match_field]["$elemMatch"]["value"][
                "$in"
            ] = query["value"]
        return multi_select_match

    def __get_parent_filter(self, parent_id):
        return {
            "$match": {
                "relations": {"$elemMatch": {"key": parent_id, "type": "parent"}}
            }
        }

    def __get_text_input_metadata_filter(self, query):
        metadata_match = {"$match": {"metadata": {"$elemMatch": {}}}}
        if "label" in query:
            metadata_match["$match"]["metadata"]["$elemMatch"]["label"] = {
                "$regex": query["label"],
                "$options": "i",
            }
        if "key" in query:
            metadata_match["$match"]["metadata"]["$elemMatch"]["key"] = query["key"]
        if "value" in query and query.get("match_exact"):
            metadata_match["$match"]["metadata"]["$elemMatch"]["value"] = query["value"]
        elif (
            "value" in query
            and "operator" in query
            and query["operator"] in self.operator_map
        ):
            metadata_match["$match"]["metadata"]["$elemMatch"]["value"] = {
                self.operator_map[query["operator"]]: query["value"]
            }
        elif "value" in query:
            metadata_match["$match"]["metadata"]["$elemMatch"]["value"] = {
                "$regex": query["value"],
                "$options": "i",
            }
        return metadata_match

    def __get_text_input_root_field_filter(self, query):
        return {
            "$match": {
                query["key"]: {
                    "$regex": query["value"],
                    "$options": "i",
                }
            }
        }

    def __map_relation_types(self, relation_types):
        relation_types_map = {
            "mediafiles": "hasMediafile",
            "testimonies": "hasTestimony",
        }
        return [
            relation_types_map.get(relation_type, relation_type)
            for relation_type in relation_types
        ]
