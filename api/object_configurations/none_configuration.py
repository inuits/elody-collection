from object_configurations.base_object_configuration import BaseObjectConfiguration


class NoneConfiguration(BaseObjectConfiguration):
    def logging(self, _):
        return {"object_info": {}, "tags": {}}

    def migration(self):
        def migrator(entity):
            return entity

        return migrator

    def serialization(self, from_format, to_format):  # pyright: ignore
        def serializer(entity):
            return entity

        return serializer

    def validation(self):
        return "schema", {}
