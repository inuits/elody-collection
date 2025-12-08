import math
from copy import deepcopy

from configuration import get_object_configuration_mapper
from elody.error_codes import ErrorCode, get_error_code, get_read
from elody.util import flatten_dict, interpret_flat_key
from filters_v2.matchers.base_matchers import BaseMatchers
from logging_elody.log import log


def append_matcher(matcher: dict, matchers: list[dict], operator="and"):
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
            .document_info()
            .get("object_lists", {})
        )
        for appended_matcher in matchers:
            key = list(matcher.keys())[0]
            if key in appended_matcher.keys() and key not in object_lists.keys():
                return
        matcher = {"OR_MATCHER": matcher}
    else:
        raise Exception(
            f"{get_error_code(ErrorCode.UNSUPPORTED_OPERATOR, get_read())} | operator:{operator} - Operator '{operator}' not supported."
        )

    if not did_append_matcher:
        matchers.append(matcher)


def get_filter_option_label(db, identifier, key):
    try:
        collection = BaseMatchers.collection
        if key.find("|") > -1:
            type, key = key.split("|")
            collection = (
                get_object_configuration_mapper().get(type).crud()["collection"]
            )

        item = db[collection].find_one({"identifiers": {"$in": [identifier]}})
        if not item:
            return identifier

        flat_item = flatten_dict(BaseMatchers.get_object_lists(), item)
        return flat_item[key]
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


def get_lookup_key(filter_key, match) -> str:
    for stage in match:
        if stage.get("$lookup"):
            lookup_key = stage["$lookup"]["as"]
            if filter_key.startswith(lookup_key):
                return lookup_key
    return ""


def get_options_mapper(
    filter_key: str, lookup_key: str, inner_exact_matches={}
) -> dict:
    object_lists_config = BaseMatchers.get_object_lists()
    keys_info = interpret_flat_key(filter_key, object_lists_config)
    if not keys_info[0]["object_list"]:
        return {
            "$map": {
                "input": [f"${filter_key}"],
                "as": "input",
                "in": {
                    "$cond": {
                        "if": {"$isArray": f"$$input"},
                        "then": {
                            "$map": {
                                "input": f"$$input",
                                "as": "item",
                                "in": {
                                    "label": "$$item",
                                    "value": "$$item",
                                },
                            }
                        },
                        "else": {
                            "label": f"$$input",
                            "value": f"${lookup_key}.id" if lookup_key else f"$$input",
                        },
                    }
                },
            }
        }

    return {
        "$map": {
            "input": {
                "$filter": {
                    "input": f"${keys_info[0]['key']}",
                    "as": "object",
                    "cond": {
                        "$and": [
                            {
                                "$eq": [
                                    f"$$object.{object_lists_config[keys_info[0]['object_list']]}",
                                    keys_info[0]["object_key"],
                                ]
                            },
                            *[
                                {"$eq": [f"$$object.{key}", value]}
                                for key, value in inner_exact_matches.items()
                            ],
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
                        "value": (
                            f"${lookup_key}.id"
                            if lookup_key
                            else f"$$object.{keys_info[1]['key']}"
                        ),
                    },
                }
            },
        }
    }


def lookup_already_exists_in_pipeline(lookup: list[dict], pipeline: list[dict]) -> bool:
    lookup_stage = None
    for stage in lookup:
        if stage.get("$lookup"):
            lookup_stage = stage
            break

    for stage in pipeline:
        if list(stage.keys())[0] == "$lookup":
            if stage["$lookup"] == lookup_stage:
                return True

    return False


def unify_matchers_per_schema_into_one_match(
    matchers_per_schema: dict, tidy_up_match: bool
) -> dict:
    match = {}
    general_matchers = matchers_per_schema.pop("general", [])
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
            expression = matcher[f"{matcher_type.upper()}_MATCHER"]
            if isinstance(expression, list):
                expressions.extend(expression)
            else:
                expressions.append(expression)
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


def has_bucket_filter(filter_request_body):
    geo_filter = None

    for filter in filter_request_body:
        if filter["type"] == "geo" and filter.get("bucket", False):
            geo_filter = filter
            break

    return geo_filter


def get_bucket_stages(geo_filter: dict):

    bucket = geo_filter["bucket"]
    value = geo_filter["value"]

    bucket = int(bucket)
    intial_polygon = value
    coordinates = intial_polygon["coordinates"][0]

    lngs = [p[0] for p in coordinates]
    min_lng = min(lngs)
    max_lng = max(lngs)

    lats = [p[1] for p in coordinates]
    min_lat = min(lats)
    max_lat = max(lats)

    lng_delta = max_lng - min_lng
    if lng_delta < 0 or (max_lng < min_lng):
        lng_delta = (180 - min_lng) + (max_lng + 180)
    step_size_x = lng_delta / bucket

    center_lat = (min_lat + max_lat) / 2
    lat_radians = math.radians(center_lat)
    correction_factor = math.cos(lat_radians)

    if correction_factor < 0.1:  # Avoid /0 at poles
        correction_factor = 0.1

    step_size_y = step_size_x * correction_factor

    group = {
        "$group": {
            "_id": {
                "grid_x": {
                    "$floor": {
                        "$divide": [
                            {"$arrayElemAt": ["$location.coordinates", 0]},
                            step_size_x,
                        ]
                    }
                },
                "grid_y": {
                    "$floor": {
                        "$divide": [
                            {"$arrayElemAt": ["$location.coordinates", 1]},
                            step_size_y,
                        ]
                    }
                },
            },
            "count": {"$sum": 1},
            # Visual center of the cluster
            "avg_lng": {"$avg": {"$arrayElemAt": ["$location.coordinates", 0]}},
            "avg_lat": {"$avg": {"$arrayElemAt": ["$location.coordinates", 1]}},
            # Keep the data of the first document found
            "first_doc": {"$first": "$$ROOT"},
        }
    }

    replaceRoot = {
        "$replaceRoot": {
            "newRoot": {
                "$mergeObjects": [
                    "$first_doc",
                    {
                        "bucket_count": "$count",
                        "location": {
                            "type": "Point",
                            "coordinates": ["$avg_lng", "$avg_lat"],
                        },
                    },
                ]
            }
        }
    }

    return [group], [replaceRoot]
