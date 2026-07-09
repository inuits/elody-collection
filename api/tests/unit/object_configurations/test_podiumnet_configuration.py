from datetime import datetime
from unittest.mock import patch

import pytest


def make_podiumnet_config():
    """Instantiate PodiumnetConfiguration with all external deps mocked."""
    with (
        patch(
            "apps.podiumnet.object_configurations.podiumnet_configuration.PodiumnetBaseResource"
        ),
        patch(
            "apps.podiumnet.object_configurations.podiumnet_configuration.get_object_configuration_mapper"
        ),
        patch(
            "apps.podiumnet.object_configurations.podiumnet_configuration.get_rabbit"
        ),
        patch(
            "apps.podiumnet.object_configurations.podiumnet_configuration.get_virtual_properties",
            return_value=[],
        ),
        patch(
            "apps.podiumnet.object_configurations.podiumnet_configuration.send_cloudevent"
        ),
        patch("apps.podiumnet.object_configurations.podiumnet_configuration.request"),
        patch("apps.podiumnet.object_configurations.podiumnet_configuration.g"),
    ):
        from apps.podiumnet.object_configurations.podiumnet_configuration import (
            PodiumnetConfiguration,
        )

        class ConcretePodiumnetConfiguration(PodiumnetConfiguration):
            def crud(self):
                return super().crud()

            def document_info(self):
                return super().document_info()

            def logging(self, flat_document, **kwargs):
                return super().logging(flat_document, **kwargs)

            def migration(self):
                return super().migration()

            def serialization(self, from_format, to_format):
                return super().serialization(from_format, to_format)

            def validation(self):
                return "function", lambda *a, **kw: None

        return ConcretePodiumnetConfiguration()


@pytest.fixture
def config():
    return make_podiumnet_config()


TIMESTAMP = datetime(2024, 1, 1, 0, 0, 0)


class TestDocumentContentPatcherRelations:
    def test_adds_relation_to_empty_relations(self, config):
        document = {"relations": [], "type": "user"}
        content = {
            "relations": [
                {
                    "type": "refOrganizations",
                    "key": "BA-001",
                    "metadata": [{"key": "roles", "value": ["admin"]}],
                }
            ],
            "type": "user",
        }
        result = config._document_content_patcher(
            document=document, content=content, crud="update", timestamp=TIMESTAMP
        )
        assert len(result["relations"]) == 1
        assert result["relations"][0]["key"] == "BA-001"

    def test_preserves_existing_relation_when_adding_different_key_same_type(
        self, config
    ):
        document = {
            "relations": [
                {
                    "type": "refOrganizations",
                    "key": "BA-001",
                    "metadata": [{"key": "roles", "value": ["admin"]}],
                }
            ],
            "type": "user",
        }
        content = {
            "relations": [
                {
                    "type": "refOrganizations",
                    "key": "CO-002",
                    "metadata": [{"key": "roles", "value": ["viewer"]}],
                }
            ],
            "type": "user",
        }
        result = config._document_content_patcher(
            document=document, content=content, crud="update", timestamp=TIMESTAMP
        )
        assert len(result["relations"]) == 2
        keys = {r["key"] for r in result["relations"]}
        assert keys == {"BA-001", "CO-002"}

    def test_updates_existing_relation_with_same_type_and_key(self, config):
        document = {
            "relations": [
                {
                    "type": "refOrganizations",
                    "key": "BA-001",
                    "metadata": [{"key": "roles", "value": ["viewer"]}],
                }
            ],
            "type": "user",
        }
        content = {
            "relations": [
                {
                    "type": "refOrganizations",
                    "key": "BA-001",
                    "metadata": [{"key": "roles", "value": ["admin"]}],
                }
            ],
            "type": "user",
        }
        result = config._document_content_patcher(
            document=document, content=content, crud="update", timestamp=TIMESTAMP
        )
        assert len(result["relations"]) == 1
        assert result["relations"][0]["metadata"][0]["value"] == ["admin"]

    def test_preserves_relations_of_different_types(self, config):
        document = {
            "relations": [
                {"type": "hasTag", "key": "TAG-001", "metadata": []},
                {
                    "type": "refOrganizations",
                    "key": "BA-001",
                    "metadata": [{"key": "roles", "value": ["admin"]}],
                },
            ],
            "type": "user",
        }
        content = {
            "relations": [
                {
                    "type": "refOrganizations",
                    "key": "CO-002",
                    "metadata": [{"key": "roles", "value": ["viewer"]}],
                }
            ],
            "type": "user",
        }
        result = config._document_content_patcher(
            document=document, content=content, crud="update", timestamp=TIMESTAMP
        )
        assert len(result["relations"]) == 3
        types = {r["type"] for r in result["relations"]}
        assert types == {"hasTag", "refOrganizations"}

    def test_multiple_same_type_different_keys_in_single_patch(self, config):
        document = {"relations": [], "type": "user"}
        content = {
            "relations": [
                {
                    "type": "refOrganizations",
                    "key": "BA-001",
                    "metadata": [{"key": "roles", "value": ["admin"]}],
                },
                {
                    "type": "refOrganizations",
                    "key": "CO-002",
                    "metadata": [{"key": "roles", "value": ["viewer"]}],
                },
            ],
            "type": "user",
        }
        result = config._document_content_patcher(
            document=document, content=content, crud="update", timestamp=TIMESTAMP
        )
        assert len(result["relations"]) == 2
        keys = {r["key"] for r in result["relations"]}
        assert keys == {"BA-001", "CO-002"}
