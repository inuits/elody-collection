import app
import sys

from storage.mongostore import MongoStorageManager


class MongoFilters(MongoStorageManager):
    def filter(self, output_type, body, skip, limit, collection="entities"):
        pass

    def __generate_mongo_query(self, queries, collection="entities"):
        pass

    def __get_text_input_metadata_filter(self, query):
        pass

    def __get_text_input_root_field_filter(self, query):
        pass

    def __generate_text_input_query(
        self, query, counter, prev_collection, item_types=None
    ):
        pass

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
