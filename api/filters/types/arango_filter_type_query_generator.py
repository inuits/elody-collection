from filters.types.base_filter_type_query_generator import BaseFilterTypeQueryGenerator


class ArangoFilterTypeQueryGenerator(BaseFilterTypeQueryGenerator):
    def generate_query_for_id_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_id_filter_type(matchers, filter_criteria)
        return self.__parse_query(filter)

    def generate_query_for_text_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_text_filter_type(matchers, filter_criteria)
        if filter != None:
            return filter

        aql = ""
        if filter_criteria.get("label"):
            result = matchers["contains"]().match(
                "label", filter_criteria["label"], "metadata"
            )
            if result and isinstance(result, str):
                aql += result

        aql += str(
            super()._apply_matchers(
                matchers,
                filter_criteria["key"],
                filter_criteria["value"],
                filter_criteria.get("parent_key", ""),
                match_exact=filter_criteria.get("match_exact"),
            )
        )

        return aql

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

    def generate_query_for_type_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_type_filter_type(matchers, filter_criteria)
        return self.__parse_query(filter)

    def generate_query_for_metadata_on_relation_filter_type(
        self, matchers, filter_criteria
    ):
        filter = super().generate_query_for_metadata_on_relation_filter_type(
            matchers, filter_criteria
        )
        return self.__parse_query(filter)

    def __parse_query(self, filter) -> str:
        if filter and isinstance(filter, str):
            return filter

        return ""
