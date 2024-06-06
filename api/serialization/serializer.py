from configuration import get_object_configuration_mapper
from serialization.case_converter import camel_to_snake


class Serializer:
    def __call__(
        self,
        item,
        *,
        type,
        to_format,
        from_format=None,
        accept_header="application/json",
        hide_storage_format=False,
    ):
        if from_format == "query_parameter" and to_format == "filter_key":
            return self.__serialize(item, from_format, to_format, type, accept_header)
        if not isinstance(item, dict) or not type:
            return item

        item = item.get("storage_format", item)
        item["type"] = item.get("type", type)
        if from_format is None:
            from_format = item.get("schema", {}).get("type", "elody")
        if from_format == to_format:
            return item

        item = self.__serialize(item, from_format, to_format, type, accept_header)
        if hide_storage_format and item.get("storage_format"):
            del item["storage_format"]
        return item

    def get_format(self, spec: str, request_parameters):
        if spec == "ngsi-ld":
            return f"{spec.replace('-', '_')}_{camel_to_snake(request_parameters.get('options', 'normalized'))}"
        return spec

    def __serialize(self, item, from_format, to_format, type, accept_header):
        config = get_object_configuration_mapper().get(type)
        serialize = config.serialization(from_format, to_format)
        return serialize(
            item,
            accept_header=(
                accept_header if accept_header != "*/*" else "application/json"
            ),
        )
