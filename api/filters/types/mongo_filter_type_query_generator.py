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

    def generate_query_for_text_filter_type(self, matchers, filter_criteria):
        root_fields = ["filename", "mimetype"]
        sub_pipeline = self.__add_helper_queries(filter_criteria)

        if filter_criteria["key"] in root_fields:
            sub_pipeline.append(
                matchers["case_insensitive"]().match(
                    filter_criteria["key"], filter_criteria["value"]
                )
            )
        else:
            match_exact = filter_criteria.get("match_exact")
            operator = (
                filter_criteria.get("operator")
                and filter_criteria["operator"] in self.operator_map
            )
            key_value_matcher_map = {
                "label": {
                    "value": filter_criteria["label"],
                    "matcher": "case_insensitive",
                },
                "key": {"value": filter_criteria["key"], "matcher": "exact"},
                "value": {
                    "value": filter_criteria["value"],
                    "matcher": "exact"
                    if match_exact
                    else "operator"
                    if operator
                    else "case_insensitive",
                },
            }

            for key, mapping in key_value_matcher_map.items():
                sub_pipeline.append(
                    matchers[mapping["matcher"]]().match(
                        "metadata",
                        mapping["value"],
                        key,
                        operator=self.operator_map[filter_criteria["operator"]]
                        if operator
                        else None,
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
