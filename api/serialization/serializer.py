import app


class Serializer:
    def __call__(self, item, to_format, hide_storage_format=False):
        if not isinstance(item, dict):
            return item

        if item.get("storage_format"):
            item = item["storage_format"]

        from_format = item.get("schema", {}).get("type", "elody")
        if from_format == to_format:
            return item

        config = app.object_configuration_mapper.get(item["type"])
        serialize = config.serialization(from_format, to_format)
        item = serialize(item)

        if hide_storage_format and item.get("storage_format"):
            del item["storage_format"]
        return item
