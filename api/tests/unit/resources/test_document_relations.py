from unittest.mock import MagicMock, patch

import pytest
from flask import Flask


@pytest.fixture
def flask_app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def resource():
    from resources.elody.document_relations import ElodyDocumentRelations

    instance = object.__new__(ElodyDocumentRelations)
    instance.storage = MagicMock()
    instance.resource = MagicMock()
    instance.resource.put.return_value = ([{"relations": []}], 200)
    return instance


def _run_patch(resource, flask_app, incoming, raw_document, serialized_document=None):
    """
    serialized_document: what serialize() returns (from properties.ref_*)
    raw_document: what _check_if_collection_and_item_exists returns (raw MongoDB doc)
    If serialized_document is None, it defaults to raw_document with empty relations.
    """
    if serialized_document is None:
        serialized_document = {**raw_document, "relations": []}

    with (
        patch.object(
            resource, "_check_if_collection_and_item_exists", return_value=raw_document
        ),
        patch(
            "resources.elody.document_relations.serialize",
            return_value=serialized_document,
        ),
    ):
        with flask_app.test_request_context("/", method="PATCH", json=incoming):
            resource.patch(id=raw_document["_id"], spec="elody")
            from flask import g

            return list(g.content.get("relations", []))


def test_patch_merges_incoming_with_existing_from_serializer(resource, flask_app):
    """Relations from properties.ref_* (serializer path) are preserved."""
    incoming = [{"type": "refOrganizations", "key": "CO-001"}]
    raw = {"_id": "US-001", "type": "user", "relations": []}
    serialized = {
        "_id": "US-001",
        "type": "user",
        "relations": [{"type": "refOrganizations", "key": "VE-001"}],
    }

    relations = _run_patch(resource, flask_app, incoming, raw, serialized)
    keys = {r["key"] for r in relations}
    assert "VE-001" in keys
    assert "CO-001" in keys


def test_patch_merges_incoming_with_existing_from_serializer_with_same_entity_on_multiple_relations(
    resource, flask_app
):
    """Relations from properties.ref_* (serializer path) are preserved."""
    incoming = [{"type": "refCompanies", "key": "ORG-001"}]
    raw = {"_id": "PROD-001", "type": "production", "relations": []}
    serialized = {
        "_id": "PROD-001",
        "type": "production",
        "relations": [{"type": "refVenues", "key": "ORG-001"}],
    }

    relations = _run_patch(resource, flask_app, incoming, raw, serialized)
    keys = {r["key"] for r in relations}
    assert len(relations) == 2
    assert "ORG-001" in keys
    assert {"type": "refVenues", "key": "ORG-001"} in relations
    assert {"type": "refCompanies", "key": "ORG-001"} in relations


def test_patch_merges_incoming_with_existing_from_raw_relations(resource, flask_app):
    """Relations stored directly in raw document (patcher path) are preserved."""
    incoming = [{"type": "refOrganizations", "key": "CO-001"}]
    raw = {
        "_id": "US-001",
        "type": "user",
        "relations": [{"type": "refOrganizations", "key": "VE-001"}],
    }
    serialized = {
        "_id": "US-001",
        "type": "user",
        "relations": [],
    }  # serializer returns empty

    relations = _run_patch(resource, flask_app, incoming, raw, serialized)
    keys = {r["key"] for r in relations}
    assert "VE-001" in keys
    assert "CO-001" in keys


def test_patch_replaces_existing_relation_with_same_key(resource, flask_app):
    incoming = [
        {
            "type": "refOrganizations",
            "key": "VE-001",
            "metadata": [{"key": "roles", "value": ["admin"]}],
        }
    ]
    raw = {
        "_id": "US-001",
        "type": "user",
        "relations": [
            {
                "type": "refOrganizations",
                "key": "VE-001",
                "metadata": [{"key": "roles", "value": ["viewer"]}],
            }
        ],
    }
    serialized = {"_id": "US-001", "type": "user", "relations": []}

    relations = _run_patch(resource, flask_app, incoming, raw, serialized)
    assert len(relations) == 1
    assert relations[0]["metadata"][0]["value"] == ["admin"]


def test_patch_preserves_different_type_relations(resource, flask_app):
    incoming = [{"type": "refOrganizations", "key": "CO-001"}]
    raw = {
        "_id": "US-001",
        "type": "user",
        "relations": [
            {"type": "refOrganizations", "key": "VE-001"},
            {"type": "hasTag", "key": "TAG-001"},
        ],
    }
    serialized = {"_id": "US-001", "type": "user", "relations": []}

    relations = _run_patch(resource, flask_app, incoming, raw, serialized)
    keys = {r["key"] for r in relations}
    assert "VE-001" in keys
    assert "CO-001" in keys
    assert "TAG-001" in keys


def test_patch_with_empty_existing_adds_incoming(resource, flask_app):
    incoming = [{"type": "refOrganizations", "key": "VE-001"}]
    raw = {"_id": "US-001", "type": "user", "relations": []}
    serialized = {"_id": "US-001", "type": "user", "relations": []}

    relations = _run_patch(resource, flask_app, incoming, raw, serialized)
    assert len(relations) == 1
    assert relations[0]["key"] == "VE-001"
    resource.resource.put.assert_called_once()
