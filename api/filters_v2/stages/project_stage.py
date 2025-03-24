from filters_v2.helpers.mongo_helper import get_lookup_key, get_options_mapper


def build(*, options_requesting_filter={}, facet={}, lookup_stage=[]) -> list[dict]:
    if options_requesting_filter:
        return __project_options(options_requesting_filter, lookup_stage)
    elif facet:
        return __project_facets(facet, lookup_stage)

    return []


def __project_options(
    options_requesting_filter: dict, lookup_stage: list[dict]
) -> list[dict]:
    project = []
    mappers = []
    lookup_key = None
    keys: list | str = options_requesting_filter.get("key", [])
    inner_exact_matches = options_requesting_filter.get("inner_exact_matches", {})

    if isinstance(keys, list):
        key = ""
        for key in keys:
            _, key = key.split("|")
            lookup_key = get_lookup_key(key, lookup_stage)
            mappers.append(get_options_mapper(key, lookup_key, inner_exact_matches))
    else:
        key = keys
        lookup_key = get_lookup_key(key, lookup_stage)
        mappers.append(get_options_mapper(key, lookup_key, inner_exact_matches))

    if lookup_key:
        project.append({"$unwind": f"${lookup_key}"})

    project.extend(
        [
            {"$project": {"_id": 0, "options": {"$concatArrays": mappers}}},
            {"$unwind": "$options"},
            {"$group": {"_id": "options", "options": {"$addToSet": "$options"}}},
        ]
    )
    return project


def __project_facets(facet: dict, _: list[dict]) -> list[dict]:
    facets = []

    for key in facet.keys():
        if key != "results":
            facets.append({key: f"${key}"})

    return [{"$project": {"results": 1, "facets": facets}}]
