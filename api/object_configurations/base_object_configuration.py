from abc import ABC, abstractmethod
from migration.base_object_migrator import BaseObjectMigrator


class BaseObjectConfiguration(ABC):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    @abstractmethod
    def crud(self):
        return {
            "collection": "entities",
            "computed_value_patcher": lambda item: None,  # pyright: ignore
            "creator": lambda post_body, **kwargs: post_body,  # pyright: ignore
            "post_crud_hook": lambda *, crud, **kwargs: None,  # pyright: ignore
        }

    @abstractmethod
    def document_info(self):
        return {"object_lists": {"metadata": "key", "relations": "type"}}

    @abstractmethod
    def logging(self, flat_item, **kwargs):
        return {"info_labels": {}, "loki_indexed_info_labels": {}}

    @abstractmethod
    def migration(self):
        return BaseObjectMigrator(status="disabled")

    @abstractmethod
    def serialization(self, from_format, to_format):
        def serializer(item):
            return item

        return serializer

    @abstractmethod
    def validation(self):
        return "schema", {}
