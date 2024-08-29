from importlib import import_module
from object_configurations.object_configuration_mapper import ObjectConfigurationMapper


_object_configuration_mapper = ObjectConfigurationMapper()
_route_mapper = {}
_collection_mapper = {}
_storage_mapper = {}


def init_mappers():
    global _object_configuration_mapper
    global _route_mapper
    global _collection_mapper
    global _storage_mapper

    try:
        mapper_module = import_module("apps.mappers")
        _object_configuration_mapper = ObjectConfigurationMapper(
            mapper_module.OBJECT_CONFIGURATION_MAPPER
        )
        _route_mapper = mapper_module.ROUTE_MAPPER
        _collection_mapper = mapper_module.COLLECTION_MAPPER
        _storage_mapper = mapper_module.STORAGE_MAPPER
    except ModuleNotFoundError:
        from storage.arangostore import ArangoStorageManager
        from storage.memorystore import MemoryStorageManager
        from storage.mongostore import MongoStorageManager
        from storage.httpstore import HttpStorageManager
        _object_configuration_mapper = ObjectConfigurationMapper()
        _route_mapper = {}
        _collection_mapper = {}
        _storage_mapper = {
            "arango": ArangoStorageManager,
            "memory": MemoryStorageManager,
            "mongo": MongoStorageManager,
            "http": HttpStorageManager,
        }


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
    
