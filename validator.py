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


class TenantValidator:
    def validate(self, tenant_json):
        try:
            validate(instance=tenant_json, schema=tenant_schema)
        except:
            return False
        return True
