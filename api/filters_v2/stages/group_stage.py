from filters_v2.stages import add_fields_stage


def build(flat_key: str | list[str], unwind: bool = False) -> list[dict]:
    flat_keys = [flat_key] if isinstance(flat_key, str) else list(flat_key)
    flat_keys = [key for key in flat_keys if key]
    if not flat_keys:
        return []

    add_fields = []
    value_keys = []
    for key in flat_keys:
        key_add_fields = add_fields_stage.build(key)
        add_fields.extend(key_add_fields)
        value_keys.append(add_fields_stage.compose_key_for_value(key, key_add_fields))

    unwind_stage = (
        [{"$unwind": {"path": f"${value_keys[0]}", "preserveNullAndEmptyArrays": True}}]
        if unwind and len(value_keys) == 1
        else []
    )
    group_id = f"${value_keys[-1]}"
    for key in reversed(value_keys[:-1]):
        group_id = {"$ifNull": [f"${key}", group_id]}
    return [
        *add_fields,
        *unwind_stage,
        {"$group": {"_id": group_id, "document": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$document"}},
    ]
