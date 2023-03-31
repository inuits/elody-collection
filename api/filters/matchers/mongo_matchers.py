from filters.matchers.base_matchers import BaseMatchers


class MongoMatchers(BaseMatchers):
    def exact_match(self, filter_request_body):
        raise NotImplementedError("This method is not yet implemented")
