from elody.object_configurations.base_object_configuration import (
    BaseObjectConfiguration,
)
from elody.schemas import entity_schema


class EntityConfiguration(BaseObjectConfiguration):
    SCHEMA_TYPE = "entity"
    SCHEMA_VERSION = 1

    def crud(self):
        return super().crud()

    def document_info(self):
        return super().document_info()

    def logging(self, flat_item, **kwargs):
        return super().logging(flat_item, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return super().serialization(from_format, to_format)

    def validation(self):
        return "schema", entity_schema
