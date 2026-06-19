"""Unit tests for Typesense sync/delete queue handlers in queues.py."""

import pytest
from unittest.mock import MagicMock, patch


def make_entity(entity_id, entity_type="work_word"):
    return {"_id": entity_id, "type": entity_type, "identifiers": []}


def make_mock_config(
    ts_enabled=True,
    ts_collection="entities",
    search_fields=None,
    collection="entities",
    denormalized_relations=None,
):
    config = MagicMock()
    typesense = {
        "enabled": ts_enabled,
        "collection": ts_collection,
        "search_fields": search_fields or ["properties.name.value"],
    }
    if denormalized_relations is not None:
        typesense["denormalized_relations"] = denormalized_relations
    config.crud.return_value = {
        "collection": collection,
        "typesense": typesense,
    }
    return config


@pytest.fixture
def storage():
    mock = MagicMock()
    with patch("resources.queues.StorageManager") as sm:
        sm.return_value.get_db_engine.return_value = mock
        yield mock


@pytest.fixture
def mapper():
    mock = MagicMock()
    mock.get.return_value = make_mock_config()
    with patch("resources.queues.get_object_configuration_mapper", return_value=mock):
        yield mock


class TestSyncEntityToTypesense:
    """Tests for the sync_entity_to_typesense queue handler."""

    def _call(self, body):
        from resources.queues import sync_entity_to_typesense

        sync_entity_to_typesense("dams.entity_changed", body, "msg-1")

    def test_syncs_entity_to_typesense(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity

        with patch("search.typesense_client.upsert_document") as mock_upsert, patch(
            "search.typesense_client.prepare_document_for_typesense"
        ) as mock_prepare:
            mock_prepare.return_value = {
                "id": "ent-1",
                "_id": "ent-1",
                "type": "work_word",
            }

            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})

            mock_prepare.assert_called_once_with(
                entity, ["properties.name.value"], facet_fields=[]
            )
            mock_upsert.assert_called_once_with(
                "entities", {"id": "ent-1", "_id": "ent-1", "type": "work_word"}
            )

    def test_skips_when_typesense_not_enabled(self, storage, mapper):
        mapper.get.return_value = make_mock_config(ts_enabled=False)

        with patch("search.typesense_client.upsert_document") as mock_upsert:
            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})
            storage.get_item_from_collection_by_id.assert_not_called()
            mock_upsert.assert_not_called()

    def test_skips_when_entity_not_found(self, storage, mapper):
        storage.get_item_from_collection_by_id.return_value = None

        with patch("search.typesense_client.upsert_document") as mock_upsert:
            self._call(
                {"data": {"location": "/entities/ent-missing", "type": "work_word"}}
            )
            mock_upsert.assert_not_called()

    def test_uses_config_collection(self, storage, mapper):
        entity = make_entity("ent-1", "bibliographic_entity")
        storage.get_item_from_collection_by_id.return_value = entity
        mapper.get.return_value = make_mock_config(
            collection="bibliographic_entities_actual"
        )

        with patch("search.typesense_client.upsert_document") as mock_upsert, patch(
            "search.typesense_client.prepare_document_for_typesense"
        ) as mock_prepare:
            mock_prepare.return_value = {"id": "ent-1"}

            self._call(
                {
                    "data": {
                        "location": "/entities/ent-1",
                        "type": "bibliographic_entity",
                    }
                }
            )

            storage.get_item_from_collection_by_id.assert_called_once_with(
                "bibliographic_entities_actual", "ent-1"
            )
            mock_upsert.assert_called_once()

    def test_skips_malformed_message_no_location(self, storage):
        with patch("search.typesense_client.upsert_document") as mock_upsert:
            self._call({"data": {"type": "work_word"}})

            storage.get_item_from_collection_by_id.assert_not_called()
            mock_upsert.assert_not_called()

    def test_skips_malformed_message_no_type(self, storage):
        with patch("search.typesense_client.upsert_document") as mock_upsert:
            self._call({"data": {"location": "/entities/ent-1"}})

            storage.get_item_from_collection_by_id.assert_not_called()
            mock_upsert.assert_not_called()

    def test_uses_correct_typesense_collection(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity
        mapper.get.return_value = make_mock_config(
            ts_collection="bibliographic", search_fields=["title"]
        )

        with patch("search.typesense_client.upsert_document") as mock_upsert, patch(
            "search.typesense_client.prepare_document_for_typesense"
        ) as mock_prepare:
            mock_prepare.return_value = {"id": "ent-1"}

            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})

            mock_prepare.assert_called_once_with(entity, ["title"], facet_fields=[])
            mock_upsert.assert_called_once_with("bibliographic", {"id": "ent-1"})

    def test_merges_denormalized_relations_into_document(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity
        relations = [
            {
                "ref": "properties.ref_authors.value",
                "source_collection": "entities",
                "target_field": "properties.name.value",
                "as": "author_names",
            }
        ]
        mapper.get.return_value = make_mock_config(denormalized_relations=relations)

        with patch("search.typesense_client.upsert_document") as mock_upsert, patch(
            "search.typesense_client.prepare_document_for_typesense"
        ) as mock_prepare, patch(
            "search.typesense_client.resolve_denormalized_fields"
        ) as mock_resolve:
            mock_prepare.return_value = {"id": "ent-1", "_id": "ent-1"}
            mock_resolve.return_value = {"author_names": ["Rowling, J.K."]}

            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})

            mock_resolve.assert_called_once_with(entity, relations, storage)
            mock_upsert.assert_called_once_with(
                "entities",
                {"id": "ent-1", "_id": "ent-1", "author_names": ["Rowling, J.K."]},
            )

    def test_does_not_resolve_when_no_denormalized_relations(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity

        with patch("search.typesense_client.upsert_document"), patch(
            "search.typesense_client.prepare_document_for_typesense"
        ) as mock_prepare, patch(
            "search.typesense_client.resolve_denormalized_fields"
        ) as mock_resolve:
            mock_prepare.return_value = {"id": "ent-1", "_id": "ent-1"}

            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})

            mock_resolve.assert_not_called()


class TestDeleteEntityFromTypesense:
    """Tests for the delete_entity_from_typesense queue handler."""

    def _call(self, body):
        from resources.queues import delete_entity_from_typesense

        delete_entity_from_typesense("dams.entity_deleted", body, "msg-1")

    def test_deletes_entity_from_typesense(self, mapper):
        with patch("search.typesense_client.delete_document") as mock_delete:
            self._call({"data": {"entity_id": "ent-1", "type": "work_word"}})
            mock_delete.assert_called_once_with("entities", "ent-1")

    def test_uses_id_field_as_fallback(self, mapper):
        with patch("search.typesense_client.delete_document") as mock_delete:
            self._call({"data": {"_id": "ent-2", "type": "work_word"}})
            mock_delete.assert_called_once_with("entities", "ent-2")

    def test_skips_when_no_entity_id(self):
        with patch("search.typesense_client.delete_document") as mock_delete:
            self._call({"data": {"type": "work_word"}})
            mock_delete.assert_not_called()

    def test_skips_when_typesense_not_enabled(self, mapper):
        mapper.get.return_value = make_mock_config(ts_enabled=False)

        with patch("search.typesense_client.delete_document") as mock_delete:
            self._call({"data": {"entity_id": "ent-1", "type": "work_word"}})
            mock_delete.assert_not_called()

    def test_uses_correct_typesense_collection(self, mapper):
        mapper.get.return_value = make_mock_config(ts_collection="bibliographic")

        with patch("search.typesense_client.delete_document") as mock_delete:
            self._call({"data": {"entity_id": "ent-1", "type": "work_word"}})
            mock_delete.assert_called_once_with("bibliographic", "ent-1")


AUTHOR_RELATION = {
    "ref": "properties.ref_authors.value",
    "source_collection": "entities_actual",
    "source_type": "person",
    "target_field": "properties.name.value",
    "as": "author_names",
}


def make_config_class(crud_dict):
    """A stand-in for an object configuration class: calling it returns an
    object whose .crud() yields crud_dict (matching mapper.get_all() values)."""
    cls = MagicMock()
    cls.return_value.crud.return_value = crud_dict
    return cls


def work_crud(relations=None, enabled=True):
    return {
        "collection": "bibliographic_entities_actual",
        "typesense": {
            "enabled": enabled,
            "collection": "entities",
            "search_fields": ["properties.original_headtitle.value"],
            "denormalized_relations": (
                [AUTHOR_RELATION] if relations is None else relations
            ),
        },
    }


def person(name, identifiers=None):
    return {
        "_id": "p1",
        "type": "person",
        "identifiers": identifiers if identifiers is not None else ["PERS-1"],
        "properties": {"name": {"value": name}},
    }


class TestIterDenormalizedSourceRelations:
    def _iter(self, mapper_dict, source_type):
        from resources.queues import _iter_denormalized_source_relations

        mapper = MagicMock()
        mapper.get_all.return_value = mapper_dict
        return list(_iter_denormalized_source_relations(mapper, source_type))

    def test_finds_matching_relation(self):
        rels = self._iter({"work_word": make_config_class(work_crud())}, "person")
        assert len(rels) == 1
        ts_config, collection, relation = rels[0]
        assert collection == "bibliographic_entities_actual"
        assert relation["as"] == "author_names"

    def test_skips_non_matching_source_type(self):
        assert self._iter({"w": make_config_class(work_crud())}, "siso") == []

    def test_skips_disabled_typesense(self):
        cfg = make_config_class(work_crud(enabled=False))
        assert self._iter({"w": cfg}, "person") == []

    def test_deduplicates_same_config_under_multiple_keys(self):
        cfg = make_config_class(work_crud())
        rels = self._iter({"a": cfg, "b": cfg}, "person")
        assert len(rels) == 1


class TestPropagateDenormalizedSourceChange:
    def _call(self, body):
        from resources.queues import propagate_denormalized_source_change

        propagate_denormalized_source_change("dams.entity_changed", body, "msg-1")

    def test_skips_on_create_without_unchanged_entity(self, storage, mapper):
        mapper.get_all.return_value = {"w": make_config_class(work_crud())}
        with patch("resources.queues._index_entity_to_typesense") as idx:
            self._call({"data": {"location": "/entities/p1", "type": "person"}})
            idx.assert_not_called()

    def test_skips_when_no_matching_relation(self, storage, mapper):
        mapper.get_all.return_value = {"w": make_config_class(work_crud())}
        with patch("resources.queues._index_entity_to_typesense") as idx:
            self._call(
                {
                    "data": {
                        "location": "/entities/g1",
                        "type": "genre",
                        "unchanged_entity": {"_id": "g1"},
                    }
                }
            )
            idx.assert_not_called()
            storage.get_items_from_collection.assert_not_called()

    def test_diff_gate_skips_when_name_unchanged(self, storage, mapper):
        mapper.get_all.return_value = {"w": make_config_class(work_crud())}
        storage.get_item_from_collection_by_id.return_value = person("Rowling, J.K.")
        with patch("resources.queues._index_entity_to_typesense") as idx:
            self._call(
                {
                    "data": {
                        "location": "/entities/p1",
                        "type": "person",
                        "unchanged_entity": person("Rowling, J.K."),
                    }
                }
            )
            idx.assert_not_called()
            storage.get_items_from_collection.assert_not_called()

    def test_resyncs_referrers_when_name_changed(self, storage, mapper):
        mapper.get_all.return_value = {"w": make_config_class(work_crud())}
        storage.get_item_from_collection_by_id.return_value = person("Rowling, Joanne")
        storage.get_items_from_collection.return_value = {
            "count": 2,
            "results": [
                {"_id": "w1", "type": "work_word"},
                {"_id": "w2", "type": "work_word"},
            ],
        }
        with patch("resources.queues._index_entity_to_typesense") as idx:
            self._call(
                {
                    "data": {
                        "location": "/entities/p1",
                        "type": "person",
                        "unchanged_entity": person("Rowling, J.K."),
                    }
                }
            )
            storage.get_items_from_collection.assert_called_once()
            assert storage.get_items_from_collection.call_args.kwargs["filters"] == {
                "properties.ref_authors.value": {"$in": ["p1", "PERS-1"]}
            }
            assert idx.call_count == 2

    def test_returns_when_source_entity_missing(self, storage, mapper):
        mapper.get_all.return_value = {"w": make_config_class(work_crud())}
        storage.get_item_from_collection_by_id.return_value = None
        with patch("resources.queues._index_entity_to_typesense") as idx:
            self._call(
                {
                    "data": {
                        "location": "/entities/p1",
                        "type": "person",
                        "unchanged_entity": person("Rowling, J.K."),
                    }
                }
            )
            idx.assert_not_called()
            storage.get_items_from_collection.assert_not_called()

    def test_skips_malformed_message(self, storage, mapper):
        with patch("resources.queues._index_entity_to_typesense") as idx:
            self._call({"data": {"type": "person"}})
            idx.assert_not_called()
