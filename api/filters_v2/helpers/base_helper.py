from elody.policies.helpers import parse_optional_filter_key


def get_distinct_by(filter_request_body: list[dict]) -> str:
    distinct_by = [
        filter_criteria["distinct_by"]
        for filter_criteria in filter_request_body
        if filter_criteria.get("distinct_by")
    ]
    return distinct_by[0] if len(distinct_by) > 0 else ""


def get_facets(filter_request_body: list[dict]) -> list:
    facets_request = [
        filter_criteria["facets"]
        for filter_criteria in filter_request_body
        if filter_criteria.get("facets")
    ]
    return facets_request[0] if len(facets_request) > 0 else []


def get_options_requesting_filter(filter_request_body: list[dict]) -> dict:
    options_requesting_filter = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria.get("provide_value_options_for_key")
    ]
    return options_requesting_filter[0] if len(options_requesting_filter) > 0 else {}


def get_selection_type_filter_value(filter_request_body: list[dict]) -> list:
    selection_type_filter = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria["type"] == "selection" and filter_criteria["key"] == "type"
    ]
    return selection_type_filter[0]["value"] if len(selection_type_filter) > 0 else []


def get_type_filter_value(filter_request_body: list[dict]) -> str:
    type_filter = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria["type"] == "type"
    ]
    return type_filter[0]["value"] if len(type_filter) > 0 else ""


def has_non_exact_match_filter(filter_request_body: list[dict]) -> bool:
    non_exact_match_filters = [
        filter_criteria
        for filter_criteria in filter_request_body
        if not filter_criteria.get("match_exact") and filter_criteria["type"] != "type"
    ]
    return len(non_exact_match_filters) > 0


def has_or_filter(filter_request_body: list[dict]) -> bool:
    or_filters = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria.get("operator") == "or"
    ]
    return len(or_filters) > 0


def has_selection_filter_with_multiple_values(filter_request_body: list[dict]) -> bool:
    selection_filters_with_multiple_values = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria["type"] == "selection" and len(filter_criteria["value"]) > 1
    ]
    return len(selection_filters_with_multiple_values) > 0


def parse_optional_filters(filter_criteria: dict) -> list[dict]:
    if not filter_criteria.get("key"):
        return [filter_criteria]

    filter_criterias = []
    prefix, lookup_prefix = "", ""
    if lookup_key := filter_criteria.get("lookup", {}).get("as", ""):
        _, lookup_prefix = parse_optional_filter_key(filter_criteria["key"])
        lookup_key = f"{lookup_prefix}{lookup_key}."

    key, prefix = parse_optional_filter_key(
        filter_criteria["key"].removeprefix(lookup_key)
    )
    key = f"{lookup_key.removeprefix(lookup_prefix)}{key}"

    if filter_criteria["type"] == "boolean" and filter_criteria["value"] is False:
        prefix = "?"
    elif not prefix and not lookup_prefix:
        return [filter_criteria]

    filter_criterias.append(
        {**filter_criteria, "key": key, "operator": "or" if prefix == "?" else "and"}
    )
    if prefix == "?":
        filter_criterias.append(
            {**filter_criteria, "key": key, "value": "", "operator": "or"}
        )
    return filter_criterias


def split_document_and_virtual_field_filters(filter_request_body: list[dict]):
    document_field_filters, virtual_field_filters = [], []
    for filter_criteria in filter_request_body:
        if filter_criteria.get("lookup"):
            virtual_field_filters.append(filter_criteria)
        else:
            document_field_filters.append(filter_criteria)
    return document_field_filters, virtual_field_filters
