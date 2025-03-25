def build(
    facets_request: list[dict], sort: list[dict], skip: list[dict], limit: list[dict]
) -> dict:
    facet = {"results": [*sort, *skip, *limit, {"$project": {"lookup": 0}}]}

    for facet_request in facets_request:
        facet.update(__handle_facet_key(facet_request["key"]))

    return {"$facet": facet}


def __handle_facet_key(key: str):
    return {
        key.replace(".", "__"): [{"$group": {"_id": f"${key}", "count": {"$sum": 1}}}]
    }
