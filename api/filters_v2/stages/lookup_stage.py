from configuration import get_object_configuration_mapper
from filters_v2.matchers.base_matchers import BaseMatchers


def build(filter_request_body: list[dict]) -> list[dict]:
    object_lists = (
        get_object_configuration_mapper()
        .get(BaseMatchers.type or BaseMatchers.collection)
        .document_info()
        .get("object_lists", {})
    )
    lookups = []

    for filter_criteria in filter_request_body:
        lookup = filter_criteria.get("lookup")
        if not lookup:
            continue

        lookup, lookups = __determine_lookup_fields(lookup, lookups, object_lists)
        if filter_criteria.get("aggregation"):
            lookups.append(__handle_aggregation_lookup(lookup))
        else:
            lookups.extend(__handle_match_lookup(lookup))

    return lookups


def __add_fields_stage(object_list, primary_key, data_key):
    return {
        "$addFields": {
            data_key: {
                "$filter": {
                    "input": f"${object_list}",
                    "as": object_list,
                    "cond": {"$eq": [f"$${object_list}.{primary_key}", data_key]},
                }
            },
        }
    }


def __determine_lookup_fields(
    lookup: dict, lookups: list[dict], object_lists: dict
) -> tuple[dict, list[dict]]:
    for object_list, primary_key in object_lists.items():
        if lookup["local_field"].startswith(object_list):
            data_key, data_value_key = (
                lookup["local_field"].removeprefix(f"{object_list}.").split(".", 1)
            )
            lookups.append(__add_fields_stage(object_list, primary_key, data_key))
            lookup["local_field"] = f"{data_key}.{data_value_key}"
        if (
            lookup["foreign_field"].startswith(object_list)
            and len(lookup["foreign_field"].split(".")) > 2
        ):
            raise Exception(
                "Mongo does not support foreignField referencing a virutal field."
            )

    return lookup, lookups


def __handle_aggregation_lookup(lookup: dict) -> dict:
    return {
        "$lookup": {
            "from": lookup["from"],
            "let": {"localField": f"${lookup['local_field']}"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {
                                    "$in": [
                                        f"${lookup['foreign_field']}",
                                        "$$localField",
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$$localField",
                                        f"${lookup['foreign_field']}",
                                    ]
                                },
                            ]
                        }
                    }
                },
                {"$project": {"_id": 1, "id": 1}},
            ],
            "as": lookup["as"],
        }
    }


def __handle_match_lookup(lookup: dict) -> list:
    return [
        {
            "$lookup": {
                "from": lookup["from"],
                "localField": lookup["local_field"],
                "foreignField": lookup["foreign_field"],
                "as": lookup["as"],
            }
        },
        {"$unwind": f"${lookup['as']}"},
    ]
