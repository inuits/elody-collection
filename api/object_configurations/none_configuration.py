from elody.object_configurations.base_object_configuration import (
    BaseObjectConfiguration,
)


class NoneConfiguration(BaseObjectConfiguration):
    SCHEMA_TYPE = BaseObjectConfiguration.SCHEMA_TYPE
    SCHEMA_VERSION = BaseObjectConfiguration.SCHEMA_VERSION

    def crud(self):
        return super().crud()

    def document_info(self):
        return {"object_lists": {"metadata": "key", "relations": "type"}}

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return super().serialization(from_format, to_format)

    def validation(self):
        return super().validation()
