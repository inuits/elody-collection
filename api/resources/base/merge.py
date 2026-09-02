"""Merging two entities of the same type into one.

A client declares only the shape of its references, in two `crud()` keys:

    "inbound_reference_sources": lambda document_type, **_: [(collection, field)]
    "reference_repointer": lambda document, victim_id, survivor_id, **_: patch | None
"""

from configuration import get_object_configuration_mapper
from flask import request
from resources.base.document import Document
from resources.generic_object import GenericObjectDetailV2
from werkzeug.exceptions import BadRequest, Conflict

REFERENCE_SOURCES = "inbound_reference_sources"
REFERENCE_REPOINTER = "reference_repointer"


def reference_shape(document_type, key):
    crud = get_object_configuration_mapper().get(document_type).crud()
    if not (shape := crud.get(key)):
        raise NotImplementedError(
            f"The object configuration of '{document_type}' declares no "
            f"'{key}', so its inbound references cannot be handled. Declare "
            "both reference shape keys before enabling merge."
        )
    return shape


def inbound_reference_sources(document_type):
    return reference_shape(document_type, REFERENCE_SOURCES)(
        document_type=document_type
    )


def find_documents_referencing(storage, entity_id, document_type):
    seen = set()
    for collection, field in inbound_reference_sources(document_type):
        for document in list(storage.db[collection].find({field: entity_id})):
            if document["_id"] in seen:
                continue
            seen.add(document["_id"])
            yield collection, document


def count_inbound_references(storage, entity_id, document_type):
    document_ids = set()
    for collection, field in inbound_reference_sources(document_type):
        document_ids.update(storage.db[collection].distinct("_id", {field: entity_id}))
    return len(document_ids)


def repoint_inbound_references(storage, victim_id, survivor_id, document_type):
    if victim_id == survivor_id:
        raise ValueError("Cannot merge an entity into itself")

    repointer = reference_shape(document_type, REFERENCE_REPOINTER)

    repointed = 0
    for collection, document in find_documents_referencing(
        storage, victim_id, document_type
    ):
        content = repointer(
            document=document, victim_id=victim_id, survivor_id=survivor_id
        )
        if content is None:
            continue

        storage.patch_item_from_collection_v2(
            collection, document, content, document["schema"]["type"]
        )
        repointed += 1

    return repointed


def assert_mergeable(survivor, victim):
    if survivor["id"] == victim["id"]:
        raise BadRequest("Cannot merge an entity into itself.")
    if survivor["type"] != victim["type"]:
        raise Conflict(
            "Entities of different types cannot be merged: "
            f"{survivor['type']} and {victim['type']}."
        )


class MergeResource(GenericObjectDetailV2):
    def merge(self, survivor_id, spec):
        content = request.get_json()
        victim_id = content.get("victim_id")
        if not victim_id:
            raise BadRequest("victim_id is required.")

        survivor = self._check_if_collection_and_item_exists(None, survivor_id)
        victim = self._check_if_collection_and_item_exists(None, victim_id)
        assert_mergeable(survivor, victim)

        self._apply_chosen_metadata(survivor_id, spec, content)

        request.environ.pop("HTTP_IF_MATCH", None)
        self._carry_over_relations(survivor_id, victim_id, spec, victim["type"])
        repointed = self._repoint_inbound_references(
            victim_id, survivor_id, victim["type"]
        )

        self._delete_victim(victim_id, spec)

        return {
            "survivor_id": survivor_id,
            "victim_id": victim_id,
            "repointed_references": repointed,
        }, 200

    def _apply_chosen_metadata(self, survivor_id, spec, content):
        if metadata := content.get("metadata"):
            super().patch(
                collection=None,
                id=survivor_id,
                content={"metadata": metadata},
                spec=spec,
            )

    def _carry_over_relations(self, survivor_id, victim_id, spec, document_type):
        raise NotImplementedError(
            "Implement relation carry-over for this client's relations resource "
            "before enabling merge."
        )

    def _delete_victim(self, victim_id, spec):
        Document().delete(id=victim_id, spec=spec)

    def _repoint_inbound_references(self, victim_id, survivor_id, document_type):
        return repoint_inbound_references(
            self.storage, victim_id, survivor_id, document_type
        )


class InboundReferenceCountResource(GenericObjectDetailV2):
    def inbound_reference_count(self, id):
        document = self._check_if_collection_and_item_exists(None, id)
        return {
            "count": self._count_inbound_references(id, document["type"]),
        }, 200

    def _count_inbound_references(self, id, document_type):
        return count_inbound_references(self.storage, id, document_type)
