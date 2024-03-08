import app


class Serializer:
    def __call__(self, entity, to_format):
        if entity.get("storage_format"):
            del entity["storage_format"]

        from_format = entity.get("schema", {}).get("type", "elody")
        if from_format == to_format:
            return entity

        config = app.object_configuration_mapper.get(entity["type"])
        serialize = config.serialization(from_format, to_format)
        return serialize(entity)
