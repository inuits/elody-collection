from filters_v2.stages import add_fields_stage


def build(flat_key: str, unwind: bool = False) -> list[dict]:
    if not flat_key:
        return []

    add_fields = add_fields_stage.build(flat_key)
    key = add_fields_stage.compose_key_for_value(flat_key, add_fields)
    unwind_stage = (
        [{"$unwind": {"path": f"${key}", "preserveNullAndEmptyArrays": True}}]
        if unwind
        else []
    )
    return [
        *add_fields,
        *unwind_stage,
        {"$group": {"_id": f"${key}", "document": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$document"}},
    ]
