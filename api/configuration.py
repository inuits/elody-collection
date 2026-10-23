from __future__ import (
    annotations,  # NOTE: thought this wasn't required from 3.13, but apparently got pushed back to 3.14
)

from typing import TYPE_CHECKING

from config_access import get_config
from object_configurations.object_configuration_mapper import ObjectConfigurationMapper
from plugins import merged

if TYPE_CHECKING:
    from storage.genericstore import GenericStorageManager
_storage_mapper: dict[str, type[GenericStorageManager]] = {}

_object_configuration_mapper = ObjectConfigurationMapper()
_route_mapper: dict[str, str] = {}
_collection_mapper: dict[str, str] = {}
_DEFAULT_FEATURES = {
    "bulk_operations": {
        "delete": {},
        "edit": {},
        "export": {},
        "import": {},
    },
    "specs": {"elody": {"mediafiles": {}}},
}
_features = _DEFAULT_FEATURES


def _default_storage_mapper() -> dict[str, type[GenericStorageManager]]:
    from storage.arangostore import ArangoStorageManager
    from storage.httpstore import HttpStorageManager
    from storage.memorystore import MemoryStorageManager
    from storage.mongostore import MongoStorageManager

    return {
        "arango": ArangoStorageManager,
        "memory": MemoryStorageManager,
        "mongo": MongoStorageManager,
        "http": HttpStorageManager,
    }


def init_mappers():
    global _object_configuration_mapper
    global _route_mapper
    global _collection_mapper
    global _storage_mapper
    global _features

    # Scripts call this before anything else, building the config first makes
    # sure the plugins know the apps manifest
    get_config()

    _object_configuration_mapper = ObjectConfigurationMapper(
        merged(lambda plugin: plugin.object_configurations())
    )
    _route_mapper = merged(lambda plugin: plugin.route_mapper())
    _collection_mapper = merged(lambda plugin: plugin.collection_mapper())
    _storage_mapper = {
        **_default_storage_mapper(),
        **merged(lambda plugin: plugin.storage_mapper()),
    }
    _features = merged(lambda plugin: plugin.features()) or _DEFAULT_FEATURES


def get_object_configuration_mapper() -> ObjectConfigurationMapper:
    global _object_configuration_mapper
    return _object_configuration_mapper


def get_route_mapper() -> dict[str, str]:
    global _route_mapper
    return _route_mapper


def get_collection_mapper() -> dict[str, str]:
    global _collection_mapper
    return _collection_mapper


def get_storage_mapper() -> dict[str, type[GenericStorageManager]]:
    global _storage_mapper
    return _storage_mapper


def get_features() -> dict[str, dict[str, dict]]:
    global _features
    return _features
