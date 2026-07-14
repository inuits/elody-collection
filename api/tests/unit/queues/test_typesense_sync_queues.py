"""Unit tests for Typesense sync/delete queue handlers in queues.py."""

from unittest.mock import MagicMock, patch

import pytest


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

    def _call(self, body, redelivered=False):
        from resources.queues import sync_entity_to_typesense

        message = MagicMock()
        message.json.return_value = body
        message.redelivered = redelivered
        sync_entity_to_typesense(message)
        return message

    def test_syncs_entity_to_typesense_and_acks(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity

        with (
            patch(
                "search.typesense_client.upsert_document", return_value=True
            ) as mock_upsert,
            patch(
                "search.typesense_client.prepare_document_for_typesense"
            ) as mock_prepare,
            patch(
                "search.typesense_client.get_collection_field_types",
                return_value={"properties_code_value": "string"},
            ) as mock_field_types,
        ):
            mock_prepare.return_value = {
                "id": "ent-1",
                "_id": "ent-1",
                "type": "work_word",
            }

            message = self._call(
                {"data": {"location": "/entities/ent-1", "type": "work_word"}}
            )

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
            message.ack.assert_called_once()
            message.nack.assert_not_called()

    def test_skips_when_typesense_not_enabled(self, storage, mapper):
        mapper.get.return_value = make_mock_config(ts_enabled=False)

        with patch("search.typesense_client.upsert_document") as mock_upsert:
            message = self._call(
                {"data": {"location": "/entities/ent-1", "type": "work_word"}}
            )
            storage.get_item_from_collection_by_id.assert_not_called()
            mock_upsert.assert_not_called()
            message.ack.assert_called_once()

    def test_skips_when_entity_not_found(self, storage, mapper):
        storage.get_item_from_collection_by_id.return_value = None

        with patch("search.typesense_client.upsert_document") as mock_upsert:
            message = self._call(
                {"data": {"location": "/entities/ent-missing", "type": "work_word"}}
            )
            mock_upsert.assert_not_called()
            message.ack.assert_called_once()

    def test_uses_config_collection(self, storage, mapper):
        entity = make_entity("ent-1", "bibliographic_entity")
        storage.get_item_from_collection_by_id.return_value = entity
        mapper.get.return_value = make_mock_config(
            collection="bibliographic_entities_actual"
        )

        with (
            patch(
                "search.typesense_client.upsert_document", return_value=True
            ) as mock_upsert,
            patch(
                "search.typesense_client.prepare_document_for_typesense"
            ) as mock_prepare,
        ):
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

    def test_nacks_and_requeues_on_first_failure(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity

        with (
            patch(
                "search.typesense_client.prepare_document_for_typesense",
                side_effect=Exception("denormalization boom"),
            ),
            patch(
                "search.typesense_client.get_collection_field_types", return_value={}
            ),
            patch("resources.queues.log") as mock_log,
        ):
            message = self._call(
                {"data": {"location": "/entities/ent-1", "type": "work_word"}},
                redelivered=False,
            )

        mock_log.error.assert_called_once()
        message.nack.assert_called_once_with(requeue=True)
        message.ack.assert_not_called()

    def test_rejects_without_requeue_after_redelivery(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity

        with (
            patch(
                "search.typesense_client.prepare_document_for_typesense",
                side_effect=Exception("still failing"),
            ),
            patch(
                "search.typesense_client.get_collection_field_types", return_value={}
            ),
            patch("resources.queues.log") as mock_log,
        ):
            message = self._call(
                {"data": {"location": "/entities/ent-1", "type": "work_word"}},
                redelivered=True,
            )

        mock_log.error.assert_called_once()
        message.reject.assert_called_once_with(requeue=False)
        message.ack.assert_not_called()

    def test_nacks_when_upsert_returns_false(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity

        with (
            patch("search.typesense_client.upsert_document", return_value=False),
            patch(
                "search.typesense_client.prepare_document_for_typesense",
                return_value={"id": "ent-1"},
            ),
            patch(
                "search.typesense_client.get_collection_field_types", return_value={}
            ),
            patch("resources.queues.log") as mock_log,
        ):
            message = self._call(
                {"data": {"location": "/entities/ent-1", "type": "work_word"}}
            )

        mock_log.error.assert_called_once()
        message.nack.assert_called_once_with(requeue=True)
        message.ack.assert_not_called()

    def test_skips_malformed_message_no_location(self, storage):
        with patch("search.typesense_client.upsert_document") as mock_upsert:
            message = self._call({"data": {"type": "work_word"}})

            storage.get_item_from_collection_by_id.assert_not_called()
            mock_upsert.assert_not_called()
            message.ack.assert_called_once()

    def test_skips_malformed_message_no_type(self, storage):
        with patch("search.typesense_client.upsert_document") as mock_upsert:
            message = self._call({"data": {"location": "/entities/ent-1"}})

            storage.get_item_from_collection_by_id.assert_not_called()
            mock_upsert.assert_not_called()
            message.ack.assert_called_once()

    def test_uses_correct_typesense_collection(self, storage, mapper):
        entity = make_entity("ent-1", "work_word")
        storage.get_item_from_collection_by_id.return_value = entity
        mapper.get.return_value = make_mock_config(
            ts_collection="bibliographic", search_fields=["title"]
        )

        with (
            patch(
                "search.typesense_client.upsert_document", return_value=True
            ) as mock_upsert,
            patch(
                "search.typesense_client.prepare_document_for_typesense"
            ) as mock_prepare,
            patch(
                "search.typesense_client.get_collection_field_types", return_value={}
            ) as mock_field_types,
        ):
            mock_prepare.return_value = {"id": "ent-1"}

            self._call({"data": {"location": "/entities/ent-1", "type": "work_word"}})

            mock_field_types.assert_called_once_with("bibliographic")
            mock_prepare.assert_called_once_with(
                entity, ["title"], facet_fields=[], field_types={}
            )
            mock_upsert.assert_called_once_with("bibliographic", {"id": "ent-1"})


class TestDeleteEntityFromTypesense:
    """Tests for the delete_entity_from_typesense queue handler."""

    def _call(self, body, redelivered=False):
        from resources.queues import delete_entity_from_typesense

        message = MagicMock()
        message.json.return_value = body
        message.redelivered = redelivered
        delete_entity_from_typesense(message)
        return message

    def test_deletes_entity_from_typesense_and_acks(self, mapper):
        with patch(
            "search.typesense_client.delete_document", return_value=True
        ) as mock_delete:
            message = self._call({"data": {"entity_id": "ent-1", "type": "work_word"}})
            mock_delete.assert_called_once_with("entities", "ent-1")
            message.ack.assert_called_once()

    def test_uses_id_field_as_fallback(self, mapper):
        with patch(
            "search.typesense_client.delete_document", return_value=True
        ) as mock_delete:
            self._call({"data": {"_id": "ent-2", "type": "work_word"}})
            mock_delete.assert_called_once_with("entities", "ent-2")

    def test_acks_when_no_entity_id(self):
        with patch("search.typesense_client.delete_document") as mock_delete:
            message = self._call({"data": {"type": "work_word"}})
            mock_delete.assert_not_called()
            message.ack.assert_called_once()

    def test_skips_when_typesense_not_enabled(self, mapper):
        mapper.get.return_value = make_mock_config(ts_enabled=False)

        with patch("search.typesense_client.delete_document") as mock_delete:
            message = self._call({"data": {"entity_id": "ent-1", "type": "work_word"}})
            mock_delete.assert_not_called()
            message.ack.assert_called_once()

    def test_uses_correct_typesense_collection(self, mapper):
        mapper.get.return_value = make_mock_config(ts_collection="bibliographic")

        with patch(
            "search.typesense_client.delete_document", return_value=True
        ) as mock_delete:
            self._call({"data": {"entity_id": "ent-1", "type": "work_word"}})
            mock_delete.assert_called_once_with("bibliographic", "ent-1")

    def test_nacks_and_requeues_on_failure(self, mapper):
        with (
            patch("search.typesense_client.delete_document", return_value=False),
            patch("resources.queues.log") as mock_log,
        ):
            message = self._call(
                {"data": {"entity_id": "ent-1", "type": "work_word"}},
                redelivered=False,
            )

        mock_log.error.assert_called_once()
        message.nack.assert_called_once_with(requeue=True)
        message.ack.assert_not_called()

    def test_rejects_without_requeue_after_redelivery(self, mapper):
        with (
            patch("search.typesense_client.delete_document", return_value=False),
            patch("resources.queues.log") as mock_log,
        ):
            message = self._call(
                {"data": {"entity_id": "ent-1", "type": "work_word"}},
                redelivered=True,
            )

        mock_log.error.assert_called_once()
        message.reject.assert_called_once_with(requeue=False)
        message.ack.assert_not_called()
