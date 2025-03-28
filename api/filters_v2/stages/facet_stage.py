from filters_v2.stages import lookup_stage


def build(
    facets_request: list[dict], sort: list[dict], skip: list[dict], limit: list[dict]
) -> list[dict]:
    lookup = lookup_stage.build(facets=facets_request)
    facet = {"results": [*sort, *skip, *limit, {"$project": {"lookup": 0}}]}

    for facet_request in facets_request:
        facet.update(__handle_facet_key(facet_request["key"]))

    return [*lookup, {"$facet": facet}]


def __handle_facet_key(key: str):
    return {
        key.replace(".", "__"): [{"$group": {"_id": f"${key}", "count": {"$sum": 1}}}]
    }
