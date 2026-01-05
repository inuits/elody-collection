from configuration import get_object_configuration_mapper
from filters_v2.matchers.base_matchers import BaseMatchers


def build(flat_key: str) -> list[dict]:
    object_lists = (
        get_object_configuration_mapper()
        .get(BaseMatchers.type or BaseMatchers.collection)
        .document_info()
        .get("object_lists", {})
    )
    object_list_lookup_prefix = __get_object_list_lookup_prefix(flat_key)
    for object_list, primary_key in object_lists.items():
        if flat_key.startswith(f"{object_list_lookup_prefix}{object_list}"):
            key = flat_key.removeprefix(
                f"{object_list_lookup_prefix}{object_list}."
            ).split(".", 1)[0]
            return [
                {
                    "$addFields": {
                        f"__{key}": {
                            "$filter": {
                                "input": f"${object_list_lookup_prefix}{object_list}",
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
    object_list_lookup_prefix = __get_object_list_lookup_prefix(flat_key)
    return f"{key}.{flat_key.removeprefix(f'{object_list_lookup_prefix}{object_list}.{key.removeprefix('__')}.')}"


def __get_object_list_lookup_prefix(flat_key: str):
    object_list_lookup_prefix = ""
    if flat_key.startswith("__lookup.virtual_relations"):
        lookup_as = flat_key.removeprefix("__lookup.virtual_relations.").split(".", 1)[
            0
        ]
        object_list_lookup_prefix = f"__lookup.virtual_relations.{lookup_as}."
    return object_list_lookup_prefix
