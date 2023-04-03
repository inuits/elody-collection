from filters.matchers.base_matchers import BaseMatchers
from filters.types.base_filter_type_query_generator import BaseFilterTypeQueryGenerator


class MongoFilterTypeQueryGenerator(BaseFilterTypeQueryGenerator):
    def __init__(self):
        self.operator_map = {
            "==": "$eq",
            "!=": "$ne",
            ">": "$gt",
            "<": "$lt",
            ">=": "$gte",
            "<=": "$lte",
        }

    def generate_query_for_id_filter_type(self, matchers, filter_criteria):
        if filter_criteria.get("key") != "identifiers":
            return list()

        sub_pipeline = self.__add_helper_queries(filter_criteria)
        if isinstance(filter_criteria["value"], list):
            sub_pipeline.append(
                matchers["id"]().match(
                    filter_criteria["key"],
                    str.join(BaseMatchers.separator, filter_criteria["value"]),
                )
            )
        elif filter_criteria.get("match_exact"):
            sub_pipeline.append(
                matchers["exact"]().match(
                    filter_criteria["key"], filter_criteria["value"]
                )
            )
        else:
            sub_pipeline.append(
                matchers["contains"]().match(
                    filter_criteria["key"], filter_criteria["value"]
                )
            )

        return sub_pipeline

    def generate_query_for_text_filter_type(self, matchers, filter_criteria):
        root_fields = ["filename", "mimetype"]
        sub_pipeline = self.__add_helper_queries(filter_criteria)

        if filter_criteria["key"] in root_fields:
            sub_pipeline.append(
                matchers["contains"]().match(
                    filter_criteria["key"], filter_criteria["value"]
                )
            )
        else:
            key_value_matcher_map = {
                "label": {
                    "value": filter_criteria["label"],
                    "matcher": "contains",
                },
                "key": {"value": filter_criteria["key"], "matcher": "exact"},
                "value": {
                    "value": filter_criteria["value"],
                    "matcher": "exact"
                    if filter_criteria.get("match_exact")
                    else "any"
                    if filter_criteria["value"] == "*"
                    else "none"
                    if filter_criteria["value"] == ""
                    else "contains",
                },
            }

            for key, mapping in key_value_matcher_map.items():
                if key in filter_criteria:
                    sub_pipeline.append(
                        matchers[mapping["matcher"]]().match(
                            key, mapping["value"], "metadata"
                        )
                    )

        return sub_pipeline

    def __add_helper_queries(self, filter_criteria):
        sub_pipeline = list()

        if len(filter_criteria.get("item_types", [])):
            sub_pipeline.append(
                {"$match": {"type": {"$in": filter_criteria["item_types"]}}}
            )

        if filter_criteria.get("parent"):
            sub_pipeline.append(
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

        return sub_pipeline
