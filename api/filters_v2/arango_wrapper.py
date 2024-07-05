from filters_v2.mongo_filters import MongoFilters
from storage.arangostore import ArangoStorageManager


class ArangoWrapper(ArangoStorageManager):
    def filter(
        self,
        filter_request_body,
        skip,
        limit,
        collection="entities",
        order_by=None,
        asc=True,
    ):
        mongo_pipeline = MongoFilters().filter(
            filter_request_body, skip, limit, collection, order_by, asc, True
        )
        raise Exception(mongo_pipeline)
