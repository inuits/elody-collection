OPERATOR_MAP = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}


def get_comparison(key, value, element_name):
    if isinstance(value, dict):
        value_key = list(value.keys())[0]
        if value_key == "$in":
            comparison = ""
            for i in range(len(value[value_key])):
                comparison += f"{' OR ' if i > 0 else ''}'{value[value_key][i]}' IN {element_name}.{key}"
            return f"({element_name}.{key} IN {value[value_key]} OR ({comparison}))"
        elif value_key == "$regex":
            return f"LOWER({element_name}.{key}) LIKE '%{value[value_key]}%'"
        elif value_key == "$exists":
            return f"{'' if value[value_key] else '!'}HAS({element_name}, '{key}')"
        else:
            comparison = ""
            operator = ""
            for value_key in value.keys():
                comparison += f"{operator}{element_name}.{key} {OPERATOR_MAP.get(value_key)} {value[value_key]}"
                operator = " AND "
            return f"({comparison})"
    elif isinstance(value, str):
        return f"LOWER({element_name}.{key}) == LOWER('{value}')"

    return f"{element_name}.{key} == {value}"


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
        aql += f"\nFOR item IN {element}"
        aql += _handle_match_stage(elem_match["$elemMatch"], "", element_name="item")
        if key == "relations":
            aql += f"\n{operator} item._from == document._id"
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
                        else "AND ("
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
                        else "AND ("
                    ),
                    index=index,
                )
            operator_index += 1

    return f"{aql})" if close_bracket and aql.find("AND (") >= 0 else aql, index
