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
    def logging(self, _):
        return {"object_info": {}, "tags": {}}

    @abstractmethod
    def migration(self):
        return BaseObjectMigrator()

    @abstractmethod
    def serialization(self, from_format, to_format):  # pyright: ignore
        def serializer(item):
            return item

        return serializer

    @abstractmethod
    def validation(self):
        return "schema", {}
