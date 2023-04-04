from filters.matchers.matchers import BaseMatcher
from filters.types.base_filter_type_query_generator import BaseFilterTypeQueryGenerator
from typing import Type


class MongoFilterTypeQueryGenerator(BaseFilterTypeQueryGenerator):
    def generate_query_for_id_filter_type(self, matchers, filter_criteria):
        return self.__apply_matchers(
            self.__add_helper_queries(filter_criteria),
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            ids=filter_criteria["value"],
            match_exact=filter_criteria.get("match_exact"),
        )

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
            sub_pipeline.append(
                matchers["contains"]().match(
                    "label", filter_criteria["label"], "metadata"
                )
            )

            sub_pipeline = self.__apply_matchers(
                sub_pipeline,
                matchers,
                filter_criteria["key"],
                filter_criteria["value"],
                "metadata",
                match_exact=filter_criteria.get("match_exact"),
            )

        return sub_pipeline

    def generate_query_for_date_filter_type(self, matchers, filter_criteria):
        value = filter_criteria.get("value", {})
        return self.__apply_matchers(
            self.__add_helper_queries(filter_criteria),
            matchers,
            filter_criteria["key"],
            filter_criteria["value"],
            "metadata",
            match_exact=filter_criteria.get("match_exact"),
            after=value.get("after"),
            before=value.get("before"),
            or_equal=value.get("or_equal", False),
        )

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

    def __apply_matchers(
        self,
        sub_pipeline: list,
        matchers: dict[str, Type[BaseMatcher]],
        key: str,
        value,
        parent_key: str = "",
        **kwargs
    ) -> list:
        for matcher in matchers.values():
            result = matcher().match(key, value, parent_key, **kwargs)
            if result:
                sub_pipeline.append(result)

        return sub_pipeline
