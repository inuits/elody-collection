from importlib import import_module
from object_configurations.object_configuration_mapper import ObjectConfigurationMapper

_object_configuration_mapper = ObjectConfigurationMapper()
_route_mapper = {}
_collection_mapper = {}
_storage_mapper = {}
_features = {
    "bulk_operations": {
        "delete": {},
        "edit": {},
        "export": {},
        "import": {},
    },
    "specs": {"elody": {"mediafiles": {}}},
}


def init_mappers():
    global _object_configuration_mapper
    global _route_mapper
    global _collection_mapper
    global _storage_mapper
    global _features

    try:
        mapper_module = import_module("apps.mappers")
        _object_configuration_mapper = ObjectConfigurationMapper(
            mapper_module.OBJECT_CONFIGURATION_MAPPER
        )
        _route_mapper = mapper_module.ROUTE_MAPPER
        _collection_mapper = mapper_module.COLLECTION_MAPPER
        _storage_mapper = mapper_module.STORAGE_MAPPER
        _features = mapper_module.FEATURES
    except ModuleNotFoundError as mapper_error:
        from logging_elody.log import log
        from storage.arangostore import ArangoStorageManager
        from storage.httpstore import HttpStorageManager
        from storage.memorystore import MemoryStorageManager
        from storage.mongostore import MongoStorageManager

        _object_configuration_mapper = ObjectConfigurationMapper()
        _route_mapper = {}
        _collection_mapper = {}
        _storage_mapper = {
            "arango": ArangoStorageManager,
            "memory": MemoryStorageManager,
            "mongo": MongoStorageManager,
            "http": HttpStorageManager,
        }

        log.error(
            f"Configuration: apps.mappers not found with error {mapper_error}, falling back to default config."
        )
    except Exception as unknown_error:
        from logging_elody.log import log

        log.error(
            f"Configuration: Unexpected error when loading mappers: {unknown_error}"
        )
        raise unknown_error


def get_object_configuration_mapper():
    global _object_configuration_mapper
    return _object_configuration_mapper


def get_route_mapper():
    global _route_mapper
    return _route_mapper


def get_collection_mapper():
    global _collection_mapper
    return _collection_mapper


def get_storage_mapper():
    global _storage_mapper
    return _storage_mapper


def get_features():
    global _features
    return _features
