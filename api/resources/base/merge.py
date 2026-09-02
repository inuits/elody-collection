"""Merging two entities of the same type into one.

The survivor keeps the values the user chose, everything referencing the victim
is repointed at the survivor, and only then is the victim deleted.

How inbound references are found and rewritten depends entirely on how a client
stores relations, so that step is left to the subclass. Everything else — the
validation, the ordering and the response — is the same for every client and
lives here.
"""

from flask import request
from resources.base.document import Document
from resources.generic_object import GenericObjectDetailV2
from werkzeug.exceptions import BadRequest, Conflict


def assert_mergeable(survivor, victim):
    """Raises unless these two entities may be merged into one another."""
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
        raise NotImplementedError(
            "Implement inbound-reference repointing for this client's relation "
            "storage model before enabling merge."
        )


class InboundReferenceCountResource(GenericObjectDetailV2):
    def inbound_reference_count(self, id):
        document = self._check_if_collection_and_item_exists(None, id)
        return {
            "count": self._count_inbound_references(id, document["type"]),
        }, 200

    def _count_inbound_references(self, id, document_type):
        raise NotImplementedError(
            "Implement inbound-reference counting for this client's relation "
            "storage model."
        )
