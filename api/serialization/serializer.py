from copy import deepcopy
from typing import Any

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
        original_item={},
        accept_header="application/json",
    ) -> Any:
        if from_format == "query_parameter" and to_format == "filter_key":
            return self.__serialize(
                item, from_format, to_format, type, original_item, accept_header
            )
        if isinstance(item, list) and item and type:
            return self.__serialize(
                item,
                from_format if from_format else item[0]["schema"]["type"],
                to_format,
                type,
                original_item,
                accept_header,
            )
        if not isinstance(item, dict) or not type:
            return item

        item["type"] = item.get("type", type)
        if from_format is None:
            from_format = item.get("schema", {}).get("type", "elody")
        if from_format == to_format:
            return item

        item = self.__serialize(
            item, from_format, to_format, type, original_item, accept_header
        )
        return item

    def get_format(self, spec: str, request_parameters):
        if spec == "ngsi-ld":
            options = "_".join(
                sorted(request_parameters.get("options", "normalized").split(","))
            )
            return f"{spec.replace('-', '_')}_{camel_to_snake(options)}"
        return spec

    def __serialize(
        self, item, from_format, to_format, type, original_item, accept_header
    ):
        config = get_object_configuration_mapper().get(type)
        serialize = config.serialization(from_format, to_format)
        return serialize(
            deepcopy(item),
            document_type=type,
            original_document=original_item,
            accept_header=(
                accept_header if accept_header != "*/*" else "application/json"
            ),
        )
