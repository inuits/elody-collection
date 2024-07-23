def get_options_requesting_filter(filter_request_body):
    options_requesting_filter = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria.get("provide_value_options_for_key")
    ]
    return options_requesting_filter[0] if len(options_requesting_filter) > 0 else {}


def get_type_filter_value(filter_request_body):
    type_filter = [
        filter_criteria
        for filter_criteria in filter_request_body
        if filter_criteria["type"] == "type"
    ]
    return type_filter[0]["value"] if len(type_filter) > 0 else ""


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
