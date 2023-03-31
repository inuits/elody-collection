from filters.types.base_filter_type_query_generator import BaseFilterTypeQueryGenerator


class MongoFilterTypeQueryGenerator(BaseFilterTypeQueryGenerator):
    def generate_query_for_text_filter_type(self, matchers, filter_request_body):
        raise NotImplementedError("This method is not yet implemented")
