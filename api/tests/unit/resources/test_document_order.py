from unittest.mock import MagicMock, patch

import pytest
from flask import Flask, g, jsonify

PARENT = {
    "_id": "A-1",
    "type": "asset",
    "relations": [
        {
            "key": "M-1",
            "type": "has_mediafile",
            "metadata": [{"key": "order", "value": 2}],
        },
        {
            "key": "M-2",
            "type": "has_mediafile",
            "metadata": [{"key": "order", "value": 1}],
        },
        {"key": "T-1", "type": "has_tag", "metadata": []},
    ],
}


@pytest.fixture
def flask_app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def resource():
    from resources.elody.document_order import ElodyDocumentOrder

    instance = object.__new__(ElodyDocumentOrder)
    instance.storage = MagicMock()
    instance.resource = MagicMock()
    instance.resource.put.return_value = ([{"relations": []}], 200)
    instance.resource.get.side_effect = lambda **_: jsonify(PARENT)
    return instance


@pytest.fixture(autouse=True)
def stored_documents(resource):
    with (
        patch.object(
            resource,
            "_check_if_collection_and_item_exists",
            side_effect=lambda *_: {**PARENT, "relations": list(PARENT["relations"])},
        ),
        patch(
            "resources.elody.document_order.serialize",
            side_effect=lambda document, **_: document,
        ),
    ):
        yield


def test_get_lists_the_relations_of_one_type_in_order(resource, flask_app):
    query = "?relation_type=hasMediafile&return_type=mediafile"
    with flask_app.test_request_context(f"/{query}"):
        response = resource.get(id="A-1", spec="elody")

    assert response.mimetype == "text/csv"
    assert response.get_data(as_text=True).splitlines() == [
        "id,type",
        "M-2,mediafile",
        "M-1,mediafile",
    ]


def test_get_reads_the_parent_as_a_policy_checked_document(resource, flask_app):
    with flask_app.test_request_context("/?relation_type=has_mediafile"):
        resource.get(id="A-1", spec="elody")
    resource.resource.get.assert_called_once_with(id="A-1", spec="elody")
    resource.storage.get_item_from_collection_by_id.assert_not_called()


def test_get_accepts_the_document_read_as_a_tuple(resource, flask_app):
    resource.resource.get.side_effect = lambda **_: (PARENT, 200)
    with flask_app.test_request_context("/?relation_type=has_mediafile"):
        response = resource.get(id="A-1", spec="elody")
    assert response.get_data(as_text=True).splitlines()[1:] == [
        "M-2,",
        "M-1,",
    ]


def test_get_passes_a_refused_document_read_on(resource, flask_app):
    resource.resource.get.side_effect = lambda **_: ("Forbidden", 403)
    with flask_app.test_request_context("/?relation_type=has_mediafile"):
        assert resource.get(id="A-1", spec="elody") == ("Forbidden", 403)


def test_get_needs_a_relation_type(resource, flask_app):
    with flask_app.test_request_context("/?return_type=mediafile"):
        _, status = resource.get(id="A-1", spec="elody")
    assert status == 400


def test_post_writes_the_new_order_on_the_relations(resource, flask_app):
    csv = "id,type\nM-1,mediafile\nM-2,mediafile\n"
    with flask_app.test_request_context(
        "/?relation_type=hasMediafile",
        method="POST",
        data=csv,
        content_type="text/csv",
    ):
        resource.post(id="A-1", spec="elody")
        written = g.content

    assert [relation["key"] for relation in written["relations"]] == [
        "T-1",
        "M-1",
        "M-2",
    ]
    assert written["relations"][1]["metadata"] == [{"key": "order", "value": 1}]
    assert written["relations"][2]["metadata"] == [{"key": "order", "value": 2}]


def test_post_rejects_ids_the_parent_does_not_hold(resource, flask_app):
    csv = "id,type\nM-1,mediafile\nM-9,mediafile\n"
    with flask_app.test_request_context(
        "/?relation_type=has_mediafile",
        method="POST",
        data=csv,
        content_type="text/csv",
    ):
        _, status = resource.post(id="A-1", spec="elody")
    assert status == 400
