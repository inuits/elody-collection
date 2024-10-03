from elody.util import flatten_dict
from filters_v2.matchers.base_matchers import BaseMatchers
from logging_elody.log import log


AGGREGATOR_MAP = {"$size": "LENGTH"}
OPERATOR_MAP = {"$eq": "==", "$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}


def get_comparison(key, value, element_name):
    if isinstance(value, dict):
        value_key = list(value.keys())[0]
        if key == "$expr":
            comparison = ""
            for i in range(len(value[value_key])):
                operator = list(value[value_key][i].keys())[0]
                aggregator = list(value[value_key][i][operator][0].keys())[0]
                field_key = value[value_key][i][operator][0][aggregator]["$ifNull"][0][
                    1:
                ]
                field_value = value[value_key][i][operator][1]
                if field_key.startswith("relations."):
                    edge = field_key.split(".")[1]
                    comparison += (
                        f"{' AND ' if i > 0 else ''}{AGGREGATOR_MAP.get(aggregator)}("
                    )
                    comparison += f"\nFOR item IN {edge}"
                    comparison += f"\nFILTER item._from == document._id"
                    comparison += "\nRETURN item"
                    comparison += f"\n) {OPERATOR_MAP.get(operator)} {field_value}"
                else:
                    comparison += f"{' AND ' if i > 0 else ''}{AGGREGATOR_MAP.get(aggregator)}({element_name}.{field_key}) {OPERATOR_MAP.get(operator)} {field_value}"
            return f"({comparison})"
        elif value_key == "$in":
            comparison = ""
            for i in range(len(value[value_key])):
                comparison += f"{' OR ' if i > 0 else ''}'{value[value_key][i]}' IN {element_name}.{key}"
            return f"IS_ARRAY({element_name}.{key}) ? ({comparison}) : {element_name}.{key} IN {value[value_key]}"
        elif value_key == "$regex":
            return f"LOWER({element_name}.{key}) LIKE '%{value[value_key].lower()}%'"
        elif value_key == "$exists":
            key_parts = key.split(".")
            return f"{'' if value[value_key] else '!'}HAS({element_name}.{'.'.join(key_parts[:-1])}, '{key_parts[-1].replace('`', '')}')"
        else:
            comparison = ""
            operator = ""
            for value_key in value.keys():
                comparison += f"{operator}{element_name}.{key} {OPERATOR_MAP.get(value_key)} {value[value_key]}"
                operator = " AND "
            return f"({comparison})"
    elif isinstance(value, str):
        return f"LOWER({element_name}.{key}) == '{value.lower()}'"

    return f"{element_name}.{key} == {value}"


def get_filter_option_label(get_item_from_collection_by_id, identifier, key):
    try:
        item = get_item_from_collection_by_id(BaseMatchers.collection, identifier)
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


def handle_object_lists(
    key,
    value,
    element,
    aql,
    index,
    operator,
    get_filter_prefix,
    _handle_match_stage,
    is_none_matcher=False,
):
    for elem_match in value["$all"]:
        aql += f"{get_filter_prefix(operator, index)}{'' if operator.endswith('(') else ' '}LENGTH("
        if key == "relations":
            element = elem_match["$elemMatch"].pop("type")
        aql += f"\nFOR item IN {element if key == 'relations' else f'IS_ARRAY({element}) ? {element} : []'}"
        aql += _handle_match_stage(
            elem_match["$elemMatch"], "", element_name="item", operator="AND"
        )
        if key == "relations":
            aql += f"\nFILTER item._from == document._id"
        aql += "\nRETURN item"
        aql += f"\n) {'==' if is_none_matcher else '>'} 0"
        index += 1
    return aql, index


def parse_matcher_list(
    matchers,
    element_name,
    operator,
    aql,
    index,
    get_filter_prefix,
    _handle_match_stage,
    is_none_matcher=False,
):
    operator_index = 0
    close_bracket = False
    if is_none_matcher and not isinstance(matchers, list):
        matchers = [matchers]

    for matcher in matchers:
        for key, value in matcher.items():
            if isinstance(value, dict) and list(value.keys())[0] == "$all":
                aql, index = handle_object_lists(
                    key,
                    value,
                    f"{element_name}.{key}",
                    aql,
                    index,
                    (
                        operator
                        if operator_index > 0 or aql.find("\nOR ") >= 0
                        else "FILTER ("
                    ),
                    get_filter_prefix,
                    _handle_match_stage,
                    is_none_matcher=is_none_matcher,
                )
            else:
                close_bracket = True
                aql = _handle_match_stage(
                    {key: value},
                    aql,
                    operator=(
                        operator
                        if operator_index > 0 or aql.find("\nOR ") >= 0
                        else "FILTER ("
                    ),
                    index=operator_index if index == 0 else index,
                )
            operator_index += 1

    return (
        f"{aql})"
        if (close_bracket or aql.find("FILTER (") >= 0) and aql[-1] != ")"
        else aql
    ), index
