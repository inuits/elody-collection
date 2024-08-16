from elody.object_configurations.base_object_configuration import (
    BaseObjectConfiguration,
)
from elody.schemas import mediafile_schema


class MediafileConfiguration(BaseObjectConfiguration):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {"collection": "mediafiles"}
        return {**super().crud(), **crud}

    def document_info(self):
        return super().document_info()

    def logging(self, flat_item, **kwargs):
        return super().logging(flat_item, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return super().serialization(from_format, to_format)

    def validation(self):
        return "schema", mediafile_schema
