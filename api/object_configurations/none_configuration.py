from object_configurations.base_object_configuration import BaseObjectConfiguration


class NoneConfiguration(BaseObjectConfiguration):
    def filtering(self):
        return {
            "parent_keys": {
                "metadata": {"key": "key", "value": "value"},
                "relations": {"key": "type", "value": "key"},
            }
        }

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

    def sorting(self, _):
        return []

    def validation(self):
        return "schema", {}
