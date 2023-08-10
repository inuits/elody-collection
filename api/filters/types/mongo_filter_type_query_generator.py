from filters.types.base_filter_type_query_generator import BaseFilterTypeQueryGenerator


class MongoFilterTypeQueryGenerator(BaseFilterTypeQueryGenerator):
    def generate_query_for_id_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_id_filter_type(matchers, filter_criteria)
        return self.__parse_query(filter)

    def generate_query_for_text_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_text_filter_type(matchers, filter_criteria)
        if filter != None:
            return [filter]

        pipeline = []
        if filter_criteria.get("label"):
            pipeline.append(
                matchers["contains"]().match(
                    "label", filter_criteria["label"], "metadata"
                )
            )

        pipeline.append(
            super()._apply_matchers(
                matchers,
                filter_criteria["key"],
                filter_criteria["value"],
                "metadata",
                match_exact=filter_criteria.get("match_exact"),
            )
        )

        return pipeline

    def generate_query_for_date_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_date_filter_type(matchers, filter_criteria)
        return self.__parse_query(filter)

    def generate_query_for_number_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_number_filter_type(
            matchers, filter_criteria
        )
        return self.__parse_query(filter)

    def generate_query_for_selection_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_selection_filter_type(
            matchers, filter_criteria
        )
        return self.__parse_query(filter)

    def generate_query_for_boolean_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_boolean_filter_type(
            matchers, filter_criteria
        )
        return self.__parse_query(filter)

    def generate_query_for_relation_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_relation_filter_type(
            matchers, filter_criteria
        )
        return self.__parse_query(filter)

    def generate_query_for_type_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_type_filter_type(
            matchers, filter_criteria
        )
        return self.__parse_query(filter)

    def __parse_query(self, filter) -> list:
        if filter:
            if isinstance(filter, list):
                return filter
            return [filter]

        return []
