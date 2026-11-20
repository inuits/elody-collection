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
    from resources.elody.mediafiles.document_mediafiles import (
        ElodyDocumentMediafiles,
    )

    instance = object.__new__(ElodyDocumentMediafiles)
    instance.document_resource = MagicMock()
    instance.document_resource.get.return_value = {"_id": "AS-1", "type": "asset"}
    instance.documents_resource = MagicMock()
    instance.document_relations_resource = MagicMock()
    return instance


def _link_existing(resource, flask_app, store_relations_on_mediafile):
    existing = {"_id": "uuid-existing", "id": "M-1", "identifiers": ["md5sum", "M-1"]}
    with (
        patch.object(
            resource,
            "_check_if_collection_and_item_exists",
            return_value=existing,
        ) as lookup,
        flask_app.test_request_context("/", method="POST", json={"_id": "md5sum"}),
    ):
        from flask import g

        response = resource.post(
            id="AS-1",
            store_relations_on_mediafile=store_relations_on_mediafile,
            mediafile_relation_type="refMediafiles",
            technical_origin_relation_type_template="refTECHNICAL_ORIGIN",
            spec="elody",
        )
        lookup.assert_called_once_with(None, "md5sum")
        return response, g.content


def test_post_with_existing_id_links_instead_of_creating(resource, flask_app):
    response, relations = _link_existing(resource, flask_app, False)

    resource.documents_resource.post.assert_not_called()
    resource.document_relations_resource.post.assert_called_once_with(
        id="AS-1", spec="elody"
    )
    assert relations == [{"key": "M-1", "type": "refMediafiles"}]
    assert response == ({"_id": "uuid-existing"}, 201)


def test_post_with_existing_id_stores_relation_on_mediafile(resource, flask_app):
    _, relations = _link_existing(resource, flask_app, True)

    resource.documents_resource.post.assert_not_called()
    resource.document_relations_resource.post.assert_called_once_with(
        id="M-1", spec="elody"
    )
    assert relations == [{"key": "AS-1", "type": "refMediafiles"}]
