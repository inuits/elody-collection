from os import getenv

from configuration import get_object_configuration_mapper, get_storage_mapper
from flask_restful import abort

# The framework's name for "the configured database", deliberately not a
# STORAGE_MAPPER key.
DEFAULT_STORAGE_TYPE = "db"


def uses_external_storage(storage_type) -> bool:
    """Whether this type is served by something other than the default database.

    A configuration may also name a database engine explicitly. When that
    engine is the one the app is running on, it stays on the StorageManager
    singleton -- constructing a second manager per request would open a fresh
    database connection (see MongoStorageManager.__init__).
    """
    if not storage_type:
        return False
    # Mirrors StorageManager.__init__, which is the definition of "default".
    default_engine = getenv("DB_ENGINE", "arango")
    return storage_type not in (DEFAULT_STORAGE_TYPE, default_engine)


def _registered_configuration(*keys):
    """The configuration registered under the first of these keys.

    `ObjectConfigurationMapper.get` answers `NoneConfiguration` for anything it
    does not know, which is the right default for a caller asking "how do I
    treat this document" and the wrong one for a caller asking "is this type
    configured at all". The registry itself is the only place that can say.
    """
    registry = get_object_configuration_mapper().get_all()
    for key in keys:
        if key and key in registry:
            return registry[key]()
    return None


def storage_for(document_type=None, collection=None, default=None):
    """`(storage, collection)` for one document -- engine and the name to use.

    The type wins over the collection when both are known, because a type is
    what a configuration is registered for; the collection is the fallback for
    a lookup that has nothing else to go on. The returned collection name is
    the one the *engine* expects, which for an externally stored type is the
    name its own configuration declares rather than the route's.

    `default` is the caller's own storage, returned untouched when the type is
    served by the database, so a caller can use one code path for both.
    """
    config = _registered_configuration(document_type, collection)
    if config is None:
        return default, collection

    crud = config.crud()
    if not uses_external_storage(crud.get("storage_type")):
        return default, collection
    return get_external_storage(crud["storage_type"]), (
        crud.get("collection") or collection
    )


def external_members_of(route_collection) -> list:
    """`(storage_type, collection)` for the external types a route can hold.

    A `GET /entities/<id>` has no type to route on -- that is what it is asking
    for. So when the database does not have the id, the external engines that
    hold types *addressed through this route* are asked in turn.

    Membership is declared, never inferred: a configuration opts in with

        crud()["routed_through"] = "entities"

    which says "my documents live under my own collection but are reached
    through that collection's routes". Inferring it from "external, and not
    this collection" would drag in every wrapper type a client has -- a vocab
    or a repository read-through with routes of its own -- and make a plain 404
    cost a call to each of them. So a client that declares nothing gets an
    empty list, and nothing changes for it.
    """
    members = []
    seen = set()
    for config in get_object_configuration_mapper().get_all().values():
        try:
            crud = config().crud()
        except Exception:
            # a configuration that cannot be constructed cannot serve a route
            continue
        if crud.get("routed_through") != route_collection:
            continue
        storage_type = crud.get("storage_type")
        name = crud.get("collection")
        if not name or not uses_external_storage(storage_type):
            continue
        if (storage_type, name) in seen:
            continue
        seen.add((storage_type, name))
        members.append((storage_type, name))
    return members


def get_external_storage(storage_type):
    """The storage manager for an external engine, instantiated."""
    manager = get_storage_mapper().get(storage_type)
    if manager is None:
        abort(
            500,
            message=f"No storage engine registered for storage_type '{storage_type}'",
        )
    return manager()
