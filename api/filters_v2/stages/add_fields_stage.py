from configuration import get_object_configuration_mapper
from filters_v2.matchers.base_matchers import BaseMatchers


def build(flat_key: str) -> list[dict]:
    object_lists = (
        get_object_configuration_mapper()
        .get(BaseMatchers.type or BaseMatchers.collection)
        .document_info()
        .get("object_lists", {})
    )
    for object_list, primary_key in object_lists.items():
        if flat_key.startswith(object_list):
            key = flat_key.removeprefix(f"{object_list}.").split(".", 1)[0]
            return [
                {
                    "$addFields": {
                        f"__{key}": {
                            "$filter": {
                                "input": f"${object_list}",
                                "as": object_list,
                                "cond": {
                                    "$eq": [f"$${object_list}.{primary_key}", key]
                                },
                            }
                        },
                    }
                }
            ]
    return []


def compose_key_for_value(flat_key: str, add_fields: list[dict]) -> str:
    if not add_fields:
        return flat_key

    key = list(add_fields[0]["$addFields"].keys())[0]
    object_list = add_fields[0]["$addFields"][key]["$filter"]["as"]
    return f"{key}.{flat_key.removeprefix(f'{object_list}.{key.removeprefix('__')}.')}"
