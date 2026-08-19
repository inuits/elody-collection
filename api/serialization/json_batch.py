from json import loads

from flask import has_request_context, request
from werkzeug.exceptions import BadRequest


def serialize_json_batch(data, *, document_type="", umbrella_types={}, **_):
    """
    Batch serializer for a json body: yields one elody-shaped document per item, the
    same shape a single-entity patch takes. Both the batch resource and the patch
    resource convert elody to the client schema per document, so nothing is converted
    here — only what the batch loop needs is validated.

    A client wires this up by delegating from its own serializer, whose method name has
    to carry the client's schema type:

        def from_applicationjson_to_<schema_type>(self, data, **kwargs):
            return serialize_json_batch(data, umbrella_types=UMBRELLA_TYPES, **kwargs)

    An item that fails validation is yielded as the exception, which the batch loop
    attributes to a line number instead of failing the whole body.
    """
    try:
        # Only the update path patches per document; the create batch posts already
        # serialized content, which a pass-through would not satisfy. The matched rule
        # is the discriminator, since the batch resource rewrites request.path.
        matched_rule = (
            str(getattr(request, "url_rule", "") or "") if has_request_context() else ""
        )
        if matched_rule.endswith("/batch"):
            raise BadRequest(
                "json is only supported to update documents, import with csv"
            )
        items = loads(data)
        if not isinstance(items, list):
            raise BadRequest("The json body must be an array of documents")
    except Exception as exception:
        yield exception
        return

    for item in items:
        yield _prepare_document(item, document_type, umbrella_types)


def _prepare_document(item, document_type, umbrella_types):
    try:
        if not isinstance(item, dict):
            raise BadRequest("Every item in the json body must be an object")

        type = item.get("type") or document_type
        if not type:
            raise BadRequest("Field 'type' is required for every item")
        if subtypes := umbrella_types.get(type):
            raise BadRequest(
                f"'{type}' is an umbrella type. Give the item one of: "
                f"{', '.join(subtypes)}."
            )

        identifiers = item.get("identifiers") or (
            [item["id"]] if item.get("id") else []
        )
        if not identifiers:
            raise BadRequest("Field 'id' or 'identifiers' is required for every item")

        return {**item, "type": type, "identifiers": identifiers}
    except Exception as exception:
        return exception
