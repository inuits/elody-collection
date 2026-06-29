"""Unit tests for Typesense sync/delete queue handlers in queues.py."""

import pytest
from unittest.mock import MagicMock, patch


def make_entity(entity_id, entity_type="work_word"):
    return {"_id": entity_id, "type": entity_type, "identifiers": []}


def make_mock_config(
    ts_enabled=True, ts_collection="entities", search_fields=None, collection="entities"
):
    config = MagicMock()
    config.crud.return_value = {
        "collection": collection,
        "typesense": {
            "enabled": ts_enabled,
            "collection": ts_collection,
            "search_fields": search_fields or ["properties.name.value"],
        },
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
        ) as mock_prepare, patch(
            "search.typesense_client.get_collection_field_types",
            return_value={"properties_code_value": "string"},
        ) as mock_field_types:
            mock_prepare.return_value = {
                "id": "ent-1",
                "_id": "ent-1",
                "type": "work_word",
            }

            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})

            mock_field_types.assert_called_once_with("entities")
            mock_prepare.assert_called_once_with(
                entity,
                ["properties.name.value"],
                facet_fields=[],
                field_types={"properties_code_value": "string"},
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

    def test_does_not_raise_and_logs_error_when_indexing_fails(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity

        with patch(
            "search.typesense_client.prepare_document_for_typesense",
            side_effect=Exception("denormalization boom"),
        ), patch("search.typesense_client.get_collection_field_types", return_value={}), patch(
            "resources.queues.log"
        ) as mock_log:
            # Must not propagate: a raised exception under auto-ack would
            # silently drop the message and the entity would never be indexed.
            self._call(
                {"data": {"location": "/entities/ent-1", "type": "work_word"}}
            )

        mock_log.error.assert_called_once()

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
        ) as mock_prepare, patch(
            "search.typesense_client.get_collection_field_types", return_value={}
        ) as mock_field_types:
            mock_prepare.return_value = {"id": "ent-1"}

            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})

            mock_field_types.assert_called_once_with("bibliographic")
            mock_prepare.assert_called_once_with(
                entity, ["title"], facet_fields=[], field_types={}
            )
            mock_upsert.assert_called_once_with("bibliographic", {"id": "ent-1"})


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
