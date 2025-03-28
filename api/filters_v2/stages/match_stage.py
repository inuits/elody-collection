from copy import deepcopy
from filters_v2.helpers.base_helper import (
    has_or_filter,
    parse_optional_filters,
    split_document_and_virtual_field_filters,
)
from filters_v2.helpers.mongo_helper import (
    append_matcher,
    lookup_already_exists_in_pipeline,
    unify_matchers_per_schema_into_one_match,
)
from filters_v2.stages import lookup_stage
from filters_v2.types.filter_types import get_filter


def build(filter_request_body: list[dict], tidy_up_match: bool) -> list[dict]:
    match, restricted_keys = [], []
    if has_or_filter(filter_request_body):
        document_field_filters = filter_request_body
        virtual_field_filters = []
    else:
        document_field_filters, virtual_field_filters = (
            split_document_and_virtual_field_filters(filter_request_body)
        )

    if document_field_filters:
        matchers_per_schema, lookup = {"general": []}, []
        for matchers_per_schema, filter_criteria in __construct_matchers_per_schema(
            document_field_filters, restricted_keys, matchers_per_schema
        ):
            lookup = lookup_stage.build(filter_criteria=filter_criteria, lookups=lookup)
        match.extend(
            [
                *lookup,
                {
                    "$match": unify_matchers_per_schema_into_one_match(
                        matchers_per_schema, tidy_up_match
                    )
                },
            ]
        )
    if virtual_field_filters:
        for matchers_per_schema, filter_criteria in __construct_matchers_per_schema(
            virtual_field_filters, restricted_keys
        ):
            lookup = lookup_stage.build(filter_criteria=filter_criteria)
            if lookup_already_exists_in_pipeline(lookup, match):
                lookup = []

            match.extend(
                [
                    *lookup,
                    {
                        "$match": unify_matchers_per_schema_into_one_match(
                            matchers_per_schema, tidy_up_match
                        )
                    },
                ]
            )

    return match


def __construct_matchers_per_schema(
    filter_request_body: list[dict],
    restricted_keys: list,
    initial_matchers_per_schema: dict = {},
):
    matchers_per_schema = initial_matchers_per_schema
    for filter_criteria in filter_request_body:
        filter = get_filter(filter_criteria["type"])
        if not initial_matchers_per_schema:
            matchers_per_schema = {"general": []}

        if isinstance(filter_criteria.get("key"), list):
            matchers_per_schema = __handle_schema_specific_filter(
                filter, filter_criteria, restricted_keys, matchers_per_schema
            )
        else:
            matchers_per_schema = __handle_schema_agnostic_filter(
                filter, filter_criteria, restricted_keys, matchers_per_schema
            )

        item_types = filter_criteria.get("item_types", [])
        if len(item_types) > 0:
            matchers_per_schema["general"].append({"type": {"$in": item_types}})

        yield matchers_per_schema, filter_criteria


def __handle_schema_agnostic_filter(
    filter, filter_criteria, restricted_keys, matchers_per_schema
):
    filter_criterias = parse_optional_filters(filter_criteria)
    key = filter_criterias[0].get("key", "type")
    if key not in restricted_keys:
        restricted_keys.append(key)

        for filter_criteria in filter_criterias:
            matcher = filter.generate_query(filter_criteria)
            append_matcher(
                matcher,
                matchers_per_schema["general"],
                filter_criteria.get("operator", "and"),
            )

    return matchers_per_schema


def __handle_schema_specific_filter(
    filter, filter_criteria, restricted_keys, matchers_per_schema
):
    for key in filter_criteria["key"]:
        schema, key = key.split("|")
        filter_criteria_for_schema = deepcopy(filter_criteria)
        filter_criteria_for_schema["key"] = key
        filter_criterias_for_schema = parse_optional_filters(filter_criteria_for_schema)
        key = filter_criterias_for_schema[0].get("key", "type")
        if key in restricted_keys:
            break
        else:
            restricted_keys.append(key)

        for filter_criteria_for_schema in filter_criterias_for_schema:
            matcher = filter.generate_query(filter_criteria_for_schema)

            if matchers := matchers_per_schema.get(schema):
                append_matcher(
                    matcher,
                    matchers,
                    filter_criteria_for_schema.get("operator", "and"),
                )
            else:
                schema_type, schema_version = schema.split(":")
                if schema == "elody:1":
                    matchers = [
                        {
                            "$or": [
                                {"schema": {"$exists": False}},
                                {
                                    "$and": [
                                        {"schema.type": schema_type},
                                        {"schema.version": int(schema_version)},
                                    ]
                                },
                            ]
                        }
                    ]
                else:
                    matchers = [
                        {"schema.type": schema_type},
                        {"schema.version": int(schema_version)},
                    ]

                append_matcher(
                    matcher,
                    matchers,
                    filter_criteria_for_schema.get("operator", "and"),
                )
                matchers_per_schema.update({schema: matchers})

    return matchers_per_schema
