from configuration import get_object_configuration_mapper
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
            if matcher_key != "NOR_MATCHER" and matchers[index].get(matcher_key):
                if isinstance(matchers[index][matcher_key], dict) and matchers[index][
                    matcher_key
                ].get("$all"):
                    matchers[index][matcher_key]["$all"].extend(
                        matcher[matcher_key]["$all"]
                    )
                did_append_matcher = True
            index += 1
    elif operator == "or":
        object_lists = (
            get_object_configuration_mapper()
            .get(BaseMatchers.collection)
            .document_info()["object_lists"]
        )
        for appended_matcher in matchers:
            key = list(matcher.keys())[0]
            if key in appended_matcher.keys() and key not in object_lists.keys():
                return
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


def has_non_exact_match_filter(filter_request_body):
    non_exact_match_filter = [
        filter_criteria
        for filter_criteria in filter_request_body
        if not filter_criteria.get("match_exact") and filter_criteria["type"] != "type"
    ]
    return len(non_exact_match_filter) > 0


def has_selection_filter_with_multiple_values(filter_request_body):
    selection_filter_with_multiple_values = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria["type"] == "selection" and len(filter_criteria["value"]) > 1
    ]
    return len(selection_filter_with_multiple_values) > 0


def unify_matchers_per_schema_into_one_match(matchers_per_schema, tidy_up_match):
    match = {}
    general_matchers = matchers_per_schema.pop("general")
    __combine_matchers(general_matchers, "or")
    __combine_matchers(general_matchers, "nor")

    if matchers_per_schema:
        for schema_matchers in matchers_per_schema.values():
            __combine_matchers(schema_matchers, "or")
            __combine_matchers(schema_matchers, "nor")
            for general_matcher in general_matchers:
                schema_matchers.append(general_matcher)

        if len(matchers_per_schema) > 1:
            for matchers in matchers_per_schema.values():
                len_matchers = len(matchers)
                for i in range(1, len_matchers):
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
                len_matchers = len(matchers)
                for i in range(1, len_matchers):
                    if list(matchers[i].keys())[0] in ["schema.type", "schema.version"]:
                        continue
                    match.update(matchers[i])
            __unify_or_matchers(match)
    else:
        for general_matcher in general_matchers:
            match.update(general_matcher)
        __unify_or_matchers(match)

    if tidy_up_match:
        return __tidy_up_match(match)
    return match


def __combine_matchers(matchers, matcher_type):
    expressions = []
    matchers_deepcopy = deepcopy(matchers)
    for matcher in matchers_deepcopy:
        if list(matcher.keys())[0] == f"{matcher_type.upper()}_MATCHER":
            expressions.append(matcher[f"{matcher_type.upper()}_MATCHER"])
            matchers.remove(matcher)
    if len(expressions) > 0:
        matchers.append({f"${matcher_type}": expressions})


def __tidy_up_match(match):
    tidy_match = {}
    for matcher_key, matcher_value in match.items():
        if isinstance(matcher_value, dict):
            if len(matcher_value.items()) == 1:
                key = list(matcher_value.keys())[0]
                value = matcher_value[key]
                if key in ["$all", "$in"] and len(value) == 1:
                    matcher_value = value[0]
            elif matcher_value.get("$all") and len(matcher_value.get("$in", [])) == 1:
                matcher_value["$all"].extend(matcher_value["$in"])
                del matcher_value["$in"]
        elif matcher_key in ["$or", "$nor"]:
            values = []
            for value in matcher_value:
                values.append(__tidy_up_match(value))
            matcher_value = values
        tidy_match.update({matcher_key: matcher_value})
    return tidy_match


def __unify_or_matchers(match):
    match_deepcopy = deepcopy(match)
    for or_matcher in match_deepcopy.get("$or", []):
        for key, value in or_matcher.items():
            if (
                isinstance(value, dict)
                and value.get("$all")
                and list(value["$all"][0].keys())[0] != "$elemMatch"
            ):
                if match.get(key):
                    if match[key].get("$in"):
                        match[key]["$in"].extend(value["$all"])
                    else:
                        match[key].update({"$in": value["$all"]})
                else:
                    match.update({key: {"$in": value["$all"]}})
                match["$or"].remove({key: value})
            elif key == "NOR_MATCHER":
                match["$or"].append({"$nor": value})
                match["$or"].remove({key: value})

    if match.get("$or") is not None and len(match.get("$or")) == 0:
        del match["$or"]
