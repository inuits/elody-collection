import app


class Serializer:
    def __call__(self, entity, to_format, hide_storage_format=False):
        if entity.get("storage_format"):
            entity = entity["storage_format"]

        from_format = entity.get("schema", {}).get("type", "elody")
        if from_format == to_format:
            return entity

        config = app.object_configuration_mapper.get(entity["type"])
        serialize = config.serialization(from_format, to_format)
        entity = serialize(entity)

        if hide_storage_format and entity.get("storage_format"):
            del entity["storage_format"]
        return entity
