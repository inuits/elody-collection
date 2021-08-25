from jsonschema import validate

tenant_schema = {
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

mediafile_schema = {
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

entity_schema = {
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

job_schema = {
    "type": "object",
    "default": {},
    "required": [
        "job_id",
        "job_type",
        "job_info",
        "status",
        "start_time"
    ],
    "properties": {
        "job_id": {
            "type": "string",
        },
        "job_type": {
            "type": "string",

        },
        "job_info": {
            "type": "string",
        },
        "end_time": {
            "type": "string",
        },
        "user": {
            "type": "string",
        },
        "asset": {
            "type": "string",
        },
        "mediafile_id": {
            "type": "string",
        },
        "parent_job_id": {
            "type": "string",
        },
        "status":{
            "enum": [
                "queued",
                "in-progress",
                "finished",
                "failed"
            ]
        }
    },
}


class JobValidator:
    def validate(self, job_json):
        try:
            validate(instance=job_json, schema=job_schema)
        except:
            return False
        return True


class TenantValidator:
    def validate(self, tenant_json):
        try:
            validate(instance=tenant_json, schema=tenant_schema)
        except:
            return False
        return True


class EntityValidator:
    def validate(self, entity_json):
        try:
            validate(instance=entity_json, schema=entity_schema)
        except:
            return False
        return True


class MediafileValidator:
    def validate(self, mediafile_json):
        try:
            validate(instance=mediafile_json, schema=mediafile_schema)
        except:
            return False
        return True
