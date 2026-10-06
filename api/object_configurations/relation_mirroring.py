"""Opt-in relation mirroring for object-configured entity types.

The classic storage path stores every relation on both entities: has<X> on
one, its mirror is<X>For on the other (MongoStorageManager.__add_child_relations).
Entity types with an object configuration write only the side a request
names. A configuration that mixes in RelationMirroring keeps the other side
in its post-CRUD hook, so a relation can be read and written from either
entity, as an inverse path in a SHACL UI form needs:

    class EntityConfiguration(RelationMirroring, ElodyConfiguration): ...

Mirrors are written directly on the related documents, not through their own
CRUD path, so mirroring does not mirror back and does not trigger their hooks.
"""

import re

from logging_elody.log import log

# named pairs that do not follow the has<X> / is<X>For convention
_NAMED_MIRRORS = {
    "authored": "authoredBy",
    "authoredBy": "authored",
    "belongsTo": "hasMediafile",
    "BelongsToParent": "hasChild",
    "components": "parent",
    "contains": "isIn",
    "definedBy": "defines",
    "defines": "definedBy",
    "hasChild": "belongsToParent",
    "hasMediafile": "belongsTo",
}


def mirror_relation_type(relation_type):
    """The relation type stored on the other entity, or None when it has none."""
    if mirrored := _NAMED_MIRRORS.get(relation_type):
        return mirrored
    if match := re.match(r"^is(.*)For$", relation_type):
        return f"has{match.group(1)}"
    if match := re.match(r"^has(.*)$", relation_type):
        return f"is{match.group(1)}For"
    return None


def _relation_pairs(document):
    return {
        (relation["key"], relation["type"])
        for relation in (document or {}).get("relations") or []
        if relation.get("key") and relation.get("type")
    }


def mirror_changes(unpatched_document, document):
    """The mirrors to add and to remove after a document changed its relations.

    Either document may be None (created, deleted). Returns two lists of
    (related document id, mirror relation) in a stable order.
    """
    _id = (document or unpatched_document or {}).get("_id")
    before = _relation_pairs(unpatched_document)
    after = _relation_pairs(document)

    def mirrors(pairs):
        return [
            (key, {"key": _id, "type": mirrored})
            for key, relation_type in sorted(pairs)
            if (mirrored := mirror_relation_type(relation_type))
        ]

    return mirrors(after - before), mirrors(before - after)


def apply_mirror_changes(storage, collection, added, removed):
    """Write the mirrors on the related documents of the same collection."""
    if storage.is_dry_run():
        return
    for key, relation in removed:
        storage.db[collection].update_one(
            storage._get_id_query(key), {"$pull": {"relations": relation}}
        )
    for key, relation in added:
        result = storage.db[collection].update_one(
            {
                **storage._get_id_query(key),
                "relations": {"$not": {"$elemMatch": relation}},
            },
            {"$push": {"relations": relation}},
        )
        if result.matched_count == 0 and not storage.db[collection].find_one(
            storage._get_id_query(key), {"_id": 1}
        ):
            log.warning(f"relation mirror: no document {key} in {collection}")


class RelationMirroring:
    """Mix into an object configuration to keep relation mirrors (opt-in)."""

    def _post_crud_hook(self, **kwargs):
        super()._post_crud_hook(**kwargs)
        storage = kwargs.get("storage")
        if storage is None:
            return
        added, removed = mirror_changes(
            kwargs.get("unpatched_document") if kwargs.get("crud") != "create" else None,
            kwargs.get("document") if kwargs.get("crud") != "delete" else None,
        )
        if added or removed:
            apply_mirror_changes(storage, self.crud()["collection"], added, removed)
