"""Which storage engine serves a collection.

An object configuration names its engine in `crud()["storage_type"]`. Most
types say `"db"` -- the framework's own name for whatever DB_ENGINE is -- and
are served by the StorageManager singleton. Anything else is looked up in
STORAGE_MAPPER and constructed on the spot.

The awkward case, and the reason the predicate is not simply `!= "db"`: a
configuration may name an engine explicitly that happens to *be* the running
default. vliz does exactly that (`"storage_type": "mongo"` with
`DB_ENGINE=mongo`). Constructing a second MongoStorageManager per request would
open a fresh MongoClient and rebuild indexes, so that case has to stay on the
singleton.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_api_path = Path(__file__).resolve().parents[3]
if str(_api_path) not in sys.path:
    sys.path.insert(0, str(_api_path))


@pytest.fixture
def routing():
    from storage import routing

    return routing


class TestUsesExternalStorage:
    def test_the_framework_default_is_internal(self, routing):
        assert routing.uses_external_storage("db") is False

    def test_an_unset_storage_type_is_internal(self, routing):
        # Defensive: a configuration that somehow lost the key must not be
        # routed to a mapper lookup for the empty string.
        assert routing.uses_external_storage("") is False
        assert routing.uses_external_storage(None) is False

    def test_http_is_external(self, routing):
        assert routing.uses_external_storage("http") is True

    def test_sparql_is_external(self, routing):
        assert routing.uses_external_storage("sparql") is True

    def test_the_running_default_engine_named_explicitly_is_internal(
        self, routing, monkeypatch
    ):
        # The vliz case: every entity type declares "mongo" while DB_ENGINE is
        # mongo. Routing those externally would build a second MongoClient.
        monkeypatch.setenv("DB_ENGINE", "mongo")
        assert routing.uses_external_storage("mongo") is False

    def test_a_database_engine_that_is_not_the_running_one_is_external(
        self, routing, monkeypatch
    ):
        # No client does this today, but if a type deliberately names a
        # different database than the one the app runs on, honour it.
        monkeypatch.setenv("DB_ENGINE", "mongo")
        assert routing.uses_external_storage("arango") is True

    def test_the_default_engine_falls_back_the_way_storagemanager_does(
        self, routing, monkeypatch
    ):
        # storagemanager.py:9 is getenv("DB_ENGINE", "arango"); the predicate
        # has to agree with it or an unconfigured deployment routes wrongly.
        monkeypatch.delenv("DB_ENGINE", raising=False)
        assert routing.uses_external_storage("arango") is False


class TestGetExternalStorage:
    def test_it_returns_an_instance_not_the_class(self, routing):
        manager = MagicMock()
        with patch.object(routing, "get_storage_mapper", return_value={"x": manager}):
            assert routing.get_external_storage("x") is manager.return_value

    def test_an_unregistered_engine_aborts_rather_than_returning_none(self, routing):
        from werkzeug.exceptions import HTTPException

        with patch.object(routing, "get_storage_mapper", return_value={}):
            with pytest.raises(HTTPException) as error:
                routing.get_external_storage("nonexistent")
        assert error.value.code == 500


def _config(storage_type, collection, routed_through=None):
    """A stand-in object configuration: only crud() is ever consulted."""

    class Configuration:
        def crud(self):
            return {
                "storage_type": storage_type,
                "collection": collection,
                "routed_through": routed_through,
            }

    return Configuration


def _registry(routing, mapping):
    mapper = MagicMock()
    mapper.get_all.return_value = mapping
    mapper.get.side_effect = lambda key, schema=None: mapping.get(
        key, _config("db", "entities")
    )()
    return patch.object(
        routing, "get_object_configuration_mapper", return_value=mapper
    )


# The case this exists for: a client keeps one entity type in a triple store.
# It is reached over /entities like every other type, but the engine addresses
# it as `pipelines`, which is how the two are told apart.
MIXED = {
    "entity": _config("db", "entities"),
    "entities": _config("db", "entities"),
    "pipeline": _config("sparql", "pipelines", routed_through="entities"),
    "pipelines": _config("sparql", "pipelines", routed_through="entities"),
    # A wrapper type with routes of its own: external, but never reached
    # through /entities, so a blind lookup there must not ask it.
    "vocab": _config("http", "vocabularies"),
    "vocabularies": _config("http", "vocabularies"),
}


class TestStorageFor:
    def test_a_database_backed_type_stays_on_the_callers_storage(self, routing):
        default = MagicMock()
        with _registry(routing, MIXED):
            storage, collection = routing.storage_for(
                document_type="entity", collection="entities", default=default
            )
        assert storage is default
        assert collection == "entities"

    def test_an_externally_stored_type_is_routed_to_its_engine(self, routing):
        engine = MagicMock()
        with _registry(routing, MIXED), patch.object(
            routing, "get_external_storage", return_value=engine
        ):
            storage, collection = routing.storage_for(
                document_type="pipeline", collection="entities", default=MagicMock()
            )
        assert storage is engine

    def test_it_answers_the_collection_name_the_engine_expects(self, routing):
        """Not the route's. `entities` means nothing to the SPARQL engine --
        the collection is how it finds the configuration that holds its
        endpoint and its graph."""
        with _registry(routing, MIXED), patch.object(
            routing, "get_external_storage", return_value=MagicMock()
        ):
            _, collection = routing.storage_for(
                document_type="pipeline", collection="entities"
            )
        assert collection == "pipelines"

    def test_the_type_wins_over_the_collection(self, routing):
        """Both are known on a write, and only the type is specific enough."""
        engine = MagicMock()
        with _registry(routing, MIXED), patch.object(
            routing, "get_external_storage", return_value=engine
        ):
            storage, _ = routing.storage_for(
                document_type="pipeline", collection="entities", default=MagicMock()
            )
        assert storage is engine

    def test_a_type_nobody_registered_falls_back_to_the_collection(self, routing):
        default = MagicMock()
        with _registry(routing, MIXED):
            storage, collection = routing.storage_for(
                document_type="notathing", collection="entities", default=default
            )
        assert (storage, collection) == (default, "entities")

    def test_an_unregistered_everything_changes_nothing(self, routing):
        """`NoneConfiguration` answers for anything, so "is it registered" can
        only be asked of the registry -- otherwise every unknown type would
        look configured and route by its defaults."""
        default = MagicMock()
        with _registry(routing, MIXED):
            storage, collection = routing.storage_for(
                document_type="nope", collection="alsonope", default=default
            )
        assert (storage, collection) == (default, "alsonope")


class TestExternalMembersOf:
    def test_it_finds_the_external_types_reachable_through_a_route(self, routing):
        with _registry(routing, MIXED):
            assert routing.external_members_of("entities") == [("sparql", "pipelines")]

    def test_each_engine_and_collection_is_listed_once(self, routing):
        """Both `pipeline` and `pipelines` are registered for one type."""
        with _registry(routing, MIXED):
            assert len(routing.external_members_of("entities")) == 1

    def test_a_client_with_nothing_external_gets_nothing_to_ask(self, routing):
        """This is the ordinary case, and it has to cost nothing: the lookup
        runs on every entity-detail miss."""
        with _registry(routing, {"entity": _config("db", "entities")}):
            assert routing.external_members_of("entities") == []

    def test_membership_is_declared_rather_than_inferred(self, routing):
        """An external type with routes of its own is not a member.

        Inferring membership from "external, and not this collection" would
        drag in every wrapper type a client has and make a plain 404 on
        /entities cost a call to each of them -- for vocabularies, an HTTP
        round trip to somebody else's service.
        """
        with _registry(routing, MIXED):
            assert ("http", "vocabularies") not in routing.external_members_of(
                "entities"
            )

    def test_the_routes_own_collection_is_not_one_of_its_members(self, routing):
        """A collection served externally in its own right is already routed
        by name; it is not a hidden member of itself."""
        with _registry(routing, {"alerts": _config("sparql", "alerts")}):
            assert routing.external_members_of("alerts") == []

    def test_a_configuration_that_cannot_be_built_is_skipped(self, routing):
        class Broken:
            def __init__(self):
                raise RuntimeError("missing environment")

        with _registry(routing, {"broken": Broken, **MIXED}):
            assert routing.external_members_of("entities") == [("sparql", "pipelines")]

    def test_a_member_declared_for_another_route_is_not_returned(self, routing):
        with _registry(
            routing,
            {"thing": _config("sparql", "things", routed_through="mediafiles")},
        ):
            assert routing.external_members_of("entities") == []
