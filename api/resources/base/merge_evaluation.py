"""How fit an entity is to be the survivor of a merge.

Requested on a regular document GET as `?merge_evaluation=<strategy>`.

A client declares strategies of its own in one `crud()` key:

    "merge_survivor_evaluators": {"myStrategy": lambda document, **_: verdict}

A verdict is `{"status": ..., "score": int, "details": dict}`. The status is what
the user is shown; the score is what candidates are ranked on.
"""

from copy import deepcopy

from configuration import get_object_configuration_mapper
from werkzeug.exceptions import BadRequest

SURVIVOR_EVALUATORS = "merge_survivor_evaluators"

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


def evaluate_merge_candidate(document, strategy):
    evaluator = survivor_evaluator(document["type"], strategy)
    return {
        "strategy": strategy,
        **evaluator(document=document, strategy=strategy),
    }


def attach_merge_evaluation(data, document, strategy):
    if not isinstance(data, dict):
        return data

    try:
        data["merge_evaluation"] = evaluate_merge_candidate(document, strategy)
    except NotImplementedError as error:
        raise BadRequest(str(error))
    return data
