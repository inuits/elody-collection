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
    """POST /<collection>/<survivor_id>/merge

    Body: {"victim_id": ..., "metadata": [...]}

    Only metadata is chosen by the caller. Relations are unioned by the merge
    itself: a list of authors cannot be reconciled by picking one of the two.
    """

    def merge(self, survivor_id, spec):
        content = request.get_json()
        victim_id = content.get("victim_id")
        if not victim_id:
            raise BadRequest("victim_id is required.")

        survivor = self._check_if_collection_and_item_exists(None, survivor_id)
        victim = self._check_if_collection_and_item_exists(None, victim_id)
        assert_mergeable(survivor, victim)

        self._apply_chosen_metadata(survivor_id, spec, content)

        # If-Match guards the survivor the caller fetched, not the survivor as
        # the merge leaves it, nor the unrelated documents referencing the
        # victim.
        request.environ.pop("HTTP_IF_MATCH", None)
        self._carry_over_relations(survivor_id, victim_id, spec, victim["type"])
        repointed = self._repoint_inbound_references(
            victim_id, survivor_id, victim["type"]
        )

        # Deleted last, so a failure part-way through leaves a correct survivor
        # and a recoverable duplicate rather than orphaned references.
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
        """Adds the victim's own relations to the survivor's.

        No default: dropping them silently would lose exactly the links the
        merge exists to preserve, while still reporting success. Implement it
        against the client's own relations resource.
        """
        raise NotImplementedError(
            "Implement relation carry-over for this client's relations resource "
            "before enabling merge."
        )

    def _delete_victim(self, victim_id, spec):
        Document().delete(id=victim_id, spec=spec)

    def _repoint_inbound_references(self, victim_id, survivor_id, document_type):
        """Points every reference to the victim at the survivor instead, and
        returns how many documents were rewritten.

        No default: a no-op would silently orphan every inbound reference while
        reporting a successful merge. Implement it against the client's own
        relation storage model.
        """
        raise NotImplementedError(
            "Implement inbound-reference repointing for this client's relation "
            "storage model before enabling merge."
        )


class InboundReferenceCountResource(GenericObjectDetailV2):
    """GET /<collection>/<id>/inbound-reference-count

    Informational only: what a merge would handle on its own for this entity.
    """

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
