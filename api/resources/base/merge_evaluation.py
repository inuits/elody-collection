"""How fit an entity is to be the survivor of a merge.

Requested on a regular document GET as `?merge_evaluation=<strategy>`.

A client declares strategies of its own in one `crud()` key, and which of its
properties can never change in another:

    "merge_survivor_evaluators": {"myStrategy": lambda document, **_: verdict}
    "immutable_properties": lambda document_type, document, **_: [
        {"key": "vlacc_number", "identity_value": "465088"}
    ]

A verdict is `{"status": ..., "score": int, "details": dict}`. The status is what
the user is shown; the score is what candidates are ranked on. Immutable
properties travel with it, each with the `identity_value` it contributes to the
document's identity. A property with no identity value bears no identity and may
be carried from one record to the other; one with an identity value may only be
carried when both records agree on it.
"""

from copy import deepcopy

from configuration import get_object_configuration_mapper
from werkzeug.exceptions import BadRequest

SURVIVOR_EVALUATORS = "merge_survivor_evaluators"
SEEDING_PROPERTIES = "seeding_properties"

STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_UNKNOWN = "unknown"

IDENTITY_KEYS = ("_id", "id", "identifiers")


def expected_generated_id(document):
    prepare = (
        get_object_configuration_mapper()
        .get(document["type"])
        .crud()
        .get("creation_preparer")
    )
    if not prepare:
        return None

    content = {
        key: value for key, value in document.items() if key not in IDENTITY_KEYS
    }
    generated = [prepare(deepcopy(content)).get("id") for _ in range(2)]

    if not generated[0] or generated[0] != generated[1]:
        return None
    return generated[0]


def evaluate_identifier_integrity(document, **_):
    expected_id = expected_generated_id(document)
    if expected_id is None:
        return {"status": STATUS_UNKNOWN, "score": 0, "details": {}}

    is_current = expected_id == document.get("id")
    return {
        "status": STATUS_VALID if is_current else STATUS_INVALID,
        "score": 1 if is_current else 0,
        "details": {"expected_id": expected_id},
    }


BUILTIN_EVALUATORS = {"identifierIntegrity": evaluate_identifier_integrity}


def survivor_evaluator(document_type, strategy):
    declared = (
        get_object_configuration_mapper()
        .get(document_type)
        .crud()
        .get(SURVIVOR_EVALUATORS)
        or {}
    )
    if evaluator := declared.get(strategy) or BUILTIN_EVALUATORS.get(strategy):
        return evaluator
    raise NotImplementedError(
        f"'{strategy}' is not a merge survivor strategy of '{document_type}'. "
        f"Declare it in the object configuration under '{SURVIVOR_EVALUATORS}', "
        f"or use one of: {', '.join(sorted(BUILTIN_EVALUATORS))}."
    )


def seeding_properties(document):
    document_type = document["type"]
    declared = (
        get_object_configuration_mapper()
        .get(document_type)
        .crud()
        .get(SEEDING_PROPERTIES)
    )
    if not declared:
        return []
    result = [
        _as_immutable_seeding_field(property)
        for property in declared(document_type=document_type, document=document)
    ]
    return result


def _as_immutable_seeding_field(property):
    if isinstance(property, str):
        return {"key": property, "identity_value": None}
    return {
        "key": property["key"],
        "identity_value": property.get("identity_value"),
    }


def evaluate_merge_candidate(document, strategy):
    document_type = document["type"]
    evaluator = survivor_evaluator(document_type, strategy)
    return {
        "strategy": strategy,
        **evaluator(document=document, strategy=strategy),
        "immutable_fields": seeding_properties(document),
    }


def attach_merge_evaluation(data, document, strategy):
    if not isinstance(data, dict):
        return data

    try:
        data["merge_evaluation"] = evaluate_merge_candidate(document, strategy)
    except NotImplementedError as error:
        raise BadRequest(str(error))
    return data
