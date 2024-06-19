from copy import deepcopy
from elody.util import interpret_flat_key
from filters_v2.matchers.base_matchers import BaseMatchers
from logging_elody.log import log


def append_matcher(matcher, matchers, operator="and"):
    matcher_key = list(matcher.keys())[0]
    did_append_matcher = False

    if operator == "and":
        len_matchers, index = len(matchers), 0
        while index < len_matchers and not did_append_matcher:
            if matchers[index].get(matcher_key):
                if isinstance(matchers[index][matcher_key], dict) and matchers[index][
                    matcher_key
                ].get("$all"):
                    matchers[index][matcher_key]["$all"].extend(
                        matcher[matcher_key]["$all"]
                    )
                did_append_matcher = True
            index += 1
    elif operator == "or":
        matcher = {"OR_MATCHER": matcher}
    else:
        raise Exception(f"Operator '{operator}' not supported.")

    if not did_append_matcher:
        matchers.append(matcher)


def get_filter_option_label(db, identifier, key):
    try:
        return next(
            db[BaseMatchers.collection].aggregate(
                [
                    {"$match": {"identifiers": {"$in": [identifier]}}},
                    {"$project": {"label": get_options_mapper(key)}},
                ]
            )
        )["label"][0]["label"]
    except Exception as exception:
        log.exception(
            f"Failed fetching filter option label.",
            exc_info=exception,
            info_labels={
                "collection": BaseMatchers.collection,
                "identifier": identifier,
                "key_as_label": key,
            },
        )
        return identifier


def get_options_mapper(key):
    object_lists_config = BaseMatchers.get_object_lists()
    keys_info = interpret_flat_key(key, object_lists_config)
    if len(keys_info) != 2:
        return {}

    return {
        "$map": {
            "input": {
                "$filter": {
                    "input": f"${keys_info[0]['key']}",
                    "as": "object",
                    "cond": {
                        "$eq": [
                            f"$$object.{object_lists_config[keys_info[0]['key']]}",
                            keys_info[0]["object_key"],
                        ]
                    },
                }
            },
            "as": "object",
            "in": {
                "$cond": {
                    "if": {"$isArray": f"$$object.{keys_info[1]['key']}"},
                    "then": {
                        "$map": {
                            "input": f"$$object.{keys_info[1]['key']}",
                            "as": "item",
                            "in": {
                                "label": "$$item",
                                "value": "$$item",
                            },
                        }
                    },
                    "else": {
                        "label": f"$$object.{keys_info[1]['key']}",
                        "value": f"$$object.{keys_info[1]['key']}",
                    },
                }
            },
        }
    }


def get_options_requesting_filter(filter_request_body):
    options_requesting_filter = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria.get("provide_value_options_for_key")
    ]
    return options_requesting_filter[0] if len(options_requesting_filter) > 0 else {}


def has_selection_filter_with_multiple_values(filter_request_body):
    selection_filter_with_multiple_values = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria["type"] == "selection" and len(filter_criteria["value"]) > 1
    ]
    return len(selection_filter_with_multiple_values) > 0


def unify_matchers_per_schema_into_one_match(matchers_per_schema):
    match = {}
    general_matchers = matchers_per_schema.pop("general")
    __combine_or_matchers(general_matchers)

    if matchers_per_schema:
        for schema_matchers in matchers_per_schema.values():
            __combine_or_matchers(schema_matchers)
            for general_matcher in general_matchers:
                schema_matchers.append(general_matcher)

        if len(matchers_per_schema) > 1:
            for matchers in matchers_per_schema.values():
                for i in range(1, len(matchers)):
                    __unify_or_matchers(matchers[i])
            match.update(
                {
                    "$or": [
                        {"$and": matchers} for matchers in matchers_per_schema.values()
                    ]
                }
            )
        else:
            for matchers in matchers_per_schema.values():
                for matcher in matchers:
                    if list(matcher.keys())[0] in ["schema.type", "schema.version"]:
                        continue
                    match.update(matcher)
            __unify_or_matchers(match)
    else:
        for general_matcher in general_matchers:
            match.update(general_matcher)

    return match


def __combine_or_matchers(matchers):
    or_expression = []
    matchers_deepcopy = deepcopy(matchers)
    for matcher in matchers_deepcopy:
        if list(matcher.keys())[0] == "OR_MATCHER":
            or_expression.append(matcher["OR_MATCHER"])
            matchers.remove(matcher)
    if len(or_expression) > 0:
        matchers.append({"$or": or_expression})


def __unify_or_matchers(match):
    for or_matcher in match.get("$or", []):
        for key, value in or_matcher.items():
            if value.get("$all"):
                prefixed_key = f"AND_PREFIX_{key}"
                if match.get(prefixed_key):
                    match[prefixed_key]["$in"].extend(value["$all"])
                else:
                    match.update({prefixed_key: {"$in": value["$all"]}})
    if match.get("$or"):
        del match["$or"]
        match_deepcopy = deepcopy(match)
        and_matchers = []
        for key, value in match_deepcopy.items():
            if key.startswith("AND_PREFIX_"):
                and_matchers.append({key.removeprefix("AND_PREFIX_"): value})
                match.pop(key)
        match["$and"] = and_matchers

        and_matchers = deepcopy(match.get("$and", []))
        for and_matcher in and_matchers:
            for key, value in and_matcher.items():
                if not match.get(key):
                    match[key] = value
                    match.get("$and").remove(and_matcher)
        if len(match.get("$and", [])) == 0:
            del match["$and"]
