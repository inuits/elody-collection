from configuration import get_object_configuration_mapper
from filters_v2.matchers.base_matchers import BaseMatchers
from pymongo import ASCENDING, DESCENDING


def build(
    order_by: str, asc: bool, filter_request_body: list[dict], storage
) -> list[dict]:
    if not order_by:
        return [{"$sort": {"_id": ASCENDING}}]

    key_order_map = {}
    keys = order_by.split(",")
    for key in keys:
        key_order_map.update({key: ASCENDING if asc else DESCENDING})
    key_order_map["_id"] = ASCENDING

    sorting = (
        get_object_configuration_mapper()
        .get(BaseMatchers.type or BaseMatchers.collection)
        .crud()
        .get("sorting")
    )
    if sorting:
        return sorting(key_order_map, filter_request_body=filter_request_body)
    else:
        return [
            {
                "$sort": {
                    storage.get_sort_field(order_by): (ASCENDING if asc else DESCENDING),
                    "_id": ASCENDING,
                }
            }
        ]
