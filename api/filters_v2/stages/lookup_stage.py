from configuration import get_object_configuration_mapper
from copy import deepcopy
from filters_v2.helpers.mongo_helper import lookup_already_exists_in_pipeline
from filters_v2.matchers.base_matchers import BaseMatchers


def build(
    *, filter_criteria: dict = {}, facets: list[dict] = [], lookups: list[dict] = []
) -> list[dict]:
    lookups = deepcopy(lookups)
    if filter_criteria:
        lookups = __build_filter_lookups(filter_criteria, lookups)
    elif facets:
        lookups = __build_facet_lookups(facets, lookups)

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


def __build_facet_lookups(facets: list[dict], lookups: list[dict]):
    for facet in facets:
        facet_lookups = facet.get("lookups", [])
        range_stop = len(facet_lookups) + 1

        for i in range(1, range_stop):
            facet_lookup = facet_lookups[i - 1]
            lookup = [
                {
                    "$lookup": {
                        "from": facet_lookup["from"],
                        "localField": facet_lookup["local_field"],
                        "foreignField": facet_lookup["foreign_field"],
                        "as": facet_lookup["as"],
                    }
                },
                {"$unwind": f"${facet_lookup['as']}"},
            ]
            if lookup_already_exists_in_pipeline(lookup, lookups):
                lookup = []

            lookups.extend(lookup)

    return lookups


def __build_filter_lookups(filter_criteria: dict, lookups: list[dict]):
    object_lists = (
        get_object_configuration_mapper()
        .get(BaseMatchers.type or BaseMatchers.collection)
        .document_info()
        .get("object_lists", {})
    )
    lookup = filter_criteria.get("lookup")
    if not lookup:
        return lookups

    lookup, lookups = __determine_lookup_fields(lookup, lookups, object_lists)
    if filter_criteria.get("aggregation"):
        lookups.append(__handle_aggregation_lookup(lookup))
    else:
        lookups.extend(__handle_match_lookup(lookup))

    return lookups


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
