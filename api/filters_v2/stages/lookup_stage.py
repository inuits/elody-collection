from configuration import get_object_configuration_mapper
from copy import deepcopy
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
            lookup = facet_lookups[i - 1]
            project_field = facet["key"].removeprefix(f"{lookup['as']}.")
            if i < (range_stop - 1) and facet_lookups[i]["local_field"].startswith(
                "lookup.virtual_relations"
            ):
                project_field = facet_lookups[i]["local_field"].removeprefix(
                    f"{lookup['as']}."
                )

            lookups.extend(
                [
                    {
                        "$lookup": {
                            "from": lookup["from"],
                            "let": {"local_field": f"${lookup['local_field']}"},
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$eq": [
                                                f"${lookup['foreign_field']}",
                                                "$$local_field",
                                            ]
                                        }
                                    }
                                },
                                {"$project": {"_id": 0, project_field: 1}},
                            ],
                            "as": lookup["as"],
                        }
                    },
                    {"$unwind": f"${lookup['as']}"},
                ]
            )

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
        lookups.extend(__handle_match_lookup(filter_criteria, lookup))

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
            "let": {"local_field": f"${lookup['local_field']}"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {
                                    "$eq": [
                                        f"${lookup['foreign_field']}",
                                        "$$local_field",
                                    ]
                                },
                                {
                                    "$and": [
                                        {"$isArray": f"${lookup['foreign_field']}"},
                                        {"$not": {"$isArray": "$$local_field"}},
                                        {
                                            "$in": [
                                                "$$local_field",
                                                f"${lookup['foreign_field']}",
                                            ]
                                        },
                                    ]
                                },
                                {
                                    "$and": [
                                        {"$isArray": "$$local_field"},
                                        {
                                            "$not": {
                                                "$isArray": f"${lookup['foreign_field']}"
                                            }
                                        },
                                        {
                                            "$in": [
                                                f"${lookup['foreign_field']}",
                                                "$$local_field",
                                            ]
                                        },
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


def __handle_match_lookup(filter_criteria: dict, lookup: dict) -> list:
    key = filter_criteria["key"]
    if isinstance(filter_criteria["key"], list):
        key = filter_criteria["key"][0]
    project_field = key.split("|")[-1].removeprefix(f"{lookup['as']}.")

    return [
        {
            "$lookup": {
                "from": lookup["from"],
                "let": {"local_field": f"${lookup['local_field']}"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {
                                        "$eq": [
                                            f"${lookup['foreign_field']}",
                                            "$$local_field",
                                        ]
                                    },
                                    {
                                        "$and": [
                                            {"$isArray": f"${lookup['foreign_field']}"},
                                            {"$not": {"$isArray": "$$local_field"}},
                                            {
                                                "$in": [
                                                    "$$local_field",
                                                    f"${lookup['foreign_field']}",
                                                ]
                                            },
                                        ]
                                    },
                                    {
                                        "$and": [
                                            {"$isArray": "$$local_field"},
                                            {
                                                "$not": {
                                                    "$isArray": f"${lookup['foreign_field']}"
                                                }
                                            },
                                            {
                                                "$in": [
                                                    f"${lookup['foreign_field']}",
                                                    "$$local_field",
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 1,
                            "id": 1,
                            project_field: 1,
                            lookup["foreign_field"].rsplit(".", 1)[0]: 1,
                        }
                    },
                ],
                "as": lookup["as"],
            }
        }
    ]
