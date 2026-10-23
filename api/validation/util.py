from functools import cache

from app_context import request
from jsonschema.validators import validator_for
from plugins import concatenated, first
from werkzeug.exceptions import BadRequest

__compiled_validators = {}


def requires_required_properties() -> bool:
    return (
        request.method != "PATCH" and request.endpoint != "elody.elodydocumentrelations"
    )


def get_schema(document_type, include_required: bool | None = None):
    """Return the schema for document_type, built once per variant.

    The same object is returned on every call, so callers must never mutate it.
    """
    if not document_type:
        raise BadRequest("No document type provided")
    if include_required is None:
        include_required = requires_required_properties()
    return __build_schema(document_type, include_required)


def get_compiled_validator(schema: dict):
    """Return schema's jsonschema validate function, built and checked once.

    Raises jsonschema.ValidationError on invalid content.
    """
    # Keyed on id(), which is stable because get_schema keeps its schemas alive
    key = id(schema)
    if key not in __compiled_validators:
        validator_class = validator_for(schema)
        validator_class.check_schema(schema)
        __compiled_validators[key] = validator_class(schema).validate
    return __compiled_validators[key]


def warm_up_validators():
    for document_type in concatenated(lambda plugin: plugin.validated_document_types()):
        for include_required in (True, False):
            get_compiled_validator(get_schema(document_type, include_required))


def reset_schemas():  # for tests
    __build_schema.cache_clear()
    __compiled_validators.clear()


@cache
def __build_schema(document_type, include_required: bool):
    schema = first(lambda plugin: plugin.build_schema(document_type, include_required))
    if schema is None:
        raise BadRequest(f"No schema found for document type '{document_type}'")
    return schema
