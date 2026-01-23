from copy import deepcopy
from filters_v2.helpers.mongo_helper import lookup_already_exists_in_pipeline
from filters_v2.stages import add_fields_stage


def build(
    *, filter_criteria: dict = {}, facets: list[dict] = [], lookups: list[dict] = []
) -> list[dict]:
    lookups = deepcopy(lookups)
    if filter_criteria:
        lookups = __build_filter_lookups(filter_criteria, lookups)
    elif facets:
        lookups = __build_facet_lookups(facets, lookups)

    return lookups


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
                {
                    "$unwind": {
                        "path": f"${facet_lookup['as']}",
                        "preserveNullAndEmptyArrays": facet_lookup.get(
                            "preserve_null_and_empty_arrays", False
                        )
                        is True,
                    }
                },
            ]
            if not lookup_already_exists_in_pipeline(lookup, lookups):
                lookups.extend(lookup)

    return lookups


def __build_filter_lookups(filter_criteria: dict, lookups: list[dict]):
    lookup = filter_criteria.get("lookup")
    if not lookup:
        return lookups

    lookup, lookups = __determine_lookup_fields(lookup, lookups)
    if filter_criteria.get("aggregation"):
        # XXX: This is a temporary fix for lookups in hairoad where we do not
        # need the failsafe behaviour, which has a *significant* performance
        # overhead
        if filter_criteria.get("simple"):
            lookups.append(__handle_match_lookup(lookup)[0])
        else:
            lookups.append(__handle_aggregation_lookup(lookup))
    else:
        lookups.extend(__handle_match_lookup(lookup))

    return lookups


def __determine_lookup_fields(
    lookup: dict, lookups: list[dict]
) -> tuple[dict, list[dict]]:
    if add_fields := add_fields_stage.build(lookup["local_field"]):
        lookups.extend(add_fields)
        lookup["local_field"] = add_fields_stage.compose_key_for_value(
            lookup["local_field"], add_fields
        )
    if (add_fields := add_fields_stage.build(lookup["foreign_field"])) and len(
        lookup["foreign_field"].split(".")
    ) > 2:
        raise Exception(
            "Mongo does not support foreignField referencing a virtual field."
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
        {
            "$unwind": {
                "path": f"${lookup['as']}",
                "preserveNullAndEmptyArrays": lookup.get(
                    "preserve_null_and_empty_arrays", False
                )
                is True,
            }
        },
    ]
    # starting from mongo v5, you can use the simple lookup syntax combined with `pipeline`
    # => this way `pipeline` will contain the following $match stage, preventing the need of $unwind
    # => no $unwind means no duplicate documents in the result
    # example:
    # {
    #     "$lookup": {
    #         "from": "mediafiles",
    #         "localField": "_id",
    #         "foreignField": "ref_assets",
    #         "as": "__lookup.virtual_relations.hasMediafile",
    #         "pipeline": [
    #             {
    #                 "$match": {
    #                     "$nor": [
    #                         { "relations": { "$elemMatch": { "type": "hasOrigin" } } },
    #                         { "relations": { "$elemMatch": { "type": "hasOrigin", "key": { "$exists": false } } } }
    #                     ]
    #                 }
    #             }
    #         ]
    #     }
    # }
