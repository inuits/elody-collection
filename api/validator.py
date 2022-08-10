from jsonschema import validate, ValidationError

entity_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "default": {},
    "required": ["type"],
    "properties": {
        "_id": {"type": "string"},
        "identifiers": {
            "type": "array",
            "default": [],
            "items": {"$id": "#/properties/identifiers/items"},
        },
        "type": {
            "type": "string",
            "default": "",
        },
        "metadata": {
            "type": "array",
            "default": [],
            "additionalItems": True,
            "items": {"$id": "#/properties/metadata/items"},
        },
        "primary_mediafile_id": {"type": "string", "default": ""},
        "primary_thumbnail_file_location": {"type": "string", "default": ""},
        "data": {
            "type": "object",
            "title": "The data schema",
            "description": "An explanation about the purpose of this instance.",
            "default": {},
            "required": [],
            "properties": {
                "@context": {
                    "$id": "#/properties/data/properties/%40context",
                    "type": "array",
                    "default": [],
                    "additionalItems": True,
                    "items": {"$id": "#/properties/data/properties/%40context/items"},
                },
                "@id": {"type": "string", "default": ""},
                "@type": {"type": "string", "default": ""},
                "memberOf": {
                    "type": "string",
                },
            },
            "additionalProperties": True,
        },
        "user": {
            "type": "string",
            "default": "",
        },
    },
    "additionalProperties": True,
}


mediafile_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "default": {},
    "required": [
        "filename",
    ],
    "properties": {
        "filename": {
            "type": "string",
        },
        "original_file_location": {
            "type": "string",
        },
        "thumbnail_file_location": {
            "type": "string",
        },
        "entities": {
            "type": "array",
            "default": [],
            "items": {
                "anyOf": [
                    {
                        "type": "string",
                    }
                ]
            },
        },
    },
}

tenant_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "_id": {"type": "string"},
        "_key": {"type": "string"},
        "_from": {"type": "string"},
        "_to": {"type": "string"},
        "@context": {"type": ["object", "string", "array", "null"]},
        "@type": {"type": "string"},
        "security": {
            "type": "object",
            "properties": {
                "@context": {"type": ["object", "string", "array", "null"]},
                "@type": {"type": "string"},
                "list": {
                    "type": "array",
                },
            },
        },
        "data": {
            "type": "object",
            "properties": {
                "@context": {"type": ["object", "string", "array", "null"]},
                "@type": {"type": "string"},
            },
        },
    },
}
key_value_store_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "identifiers": {
            "type": "array",
            "default": [],
            "items": {"$id": "#/properties/identifiers/items"},
        },
        "items": {
            "type": "object",
            "default": {},
        },
    },
}

box_visit_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "default": {},
    "required": ["code", "start_time"],
    "properties": {
        "_id": {"type": "string"},
        "identifiers": {
            "type": "array",
            "default": [],
            "items": {"$id": "#/properties/identifiers/items"},
        },
        "type": {
            "type": "string",
            "default": "",
        },
        "metadata": {
            "type": "array",
            "default": [],
            "additionalItems": True,
            "items": {"$id": "#/properties/metadata/items"},
        },
        "code": {
            "type": "string",
        },
        "start_time": {
            "type": "string",
        },
        "touch_table_time": {
            "type": "string",
        },
        "frames_seen_last_visit": {
            "type": "string",
        },
    },
}


def validate_json(json, schema):
    try:
        validate(instance=json, schema=schema)
    except ValidationError as ve:
        return ve.message
    return ""
