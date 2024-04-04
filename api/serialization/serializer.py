import app

from serialization.case_converter import camel_to_snake


class Serializer:
    def __call__(
        self, item, *, type, to_format, from_format=None, hide_storage_format=False
    ):
        if not isinstance(item, dict) or not type:
            return item
        if item.get("storage_format"):
            item = item["storage_format"]

        if from_format is None:
            from_format = item.get("schema", {}).get("type", "elody")
        if from_format == to_format:
            return item

        config = app.object_configuration_mapper.get(type)
        serialize = config.serialization(from_format, to_format)
        item = serialize(item)

        if hide_storage_format and item.get("storage_format"):
            del item["storage_format"]
        return item

    def get_format(self, spec: str, request_parameters):
        if spec == "ngsi-ld":
            return f"{spec.replace('-', '_')}_{camel_to_snake(request_parameters.get('options', 'normalized'))}"
        return spec
