from filters_v2.types.base_filter_type_query_generator import (
    BaseFilterTypeQueryGenerator,
)


class MongoFilterTypeQueryGenerator(BaseFilterTypeQueryGenerator):
    def generate_query_for_text_filter_type(self, matchers, filter_criteria):
        filter = super().generate_query_for_text_filter_type(matchers, filter_criteria)
        return self.__parse_query(filter)

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

    def __parse_query(self, filter) -> dict:
        return filter if filter else {}
