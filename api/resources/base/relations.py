def merge_relation_metadata(stored_relations, relations):
    """Keep relation metadata the request does not mention.

    A metadata write is a patch: a key absent from the body keeps its stored
    value. That is what lets a key a role may not read - stripped from the
    response by its key_restrictions - survive an edit untouched. A relation
    write replaces the relation wholesale, so the same key would be written back
    as absent and lost, by a client that never saw it. Merging per key gives
    relation metadata the same semantics as metadata: a write only changes what
    it carries. Send an empty value to clear a key.
    """
    # TODO: this merge belongs in PATCH, not here.  # noqa: FIX002
    # PUT is specified as a full replace, so omitting a key should delete it;
    # today it cannot, and a client that wants to remove one has to send an
    # empty value. The relation endpoints do not honour that split: PATCH
    # deletes the relation by key and re-adds the incoming one wholesale, which
    # is a replace too, and the PWA has to PUT the entire relation list because
    # there is no way to delete a single relation. Give PATCH real merge
    # semantics and PUT real replace semantics, add a relation delete, and this
    # function moves to the PATCH path where omission stops being ambiguous.
    stored_metadata = {
        (relation.get("key"), relation.get("type")): {
            item.get("key"): item for item in relation.get("metadata") or []
        }
        for relation in stored_relations
    }
    for relation in relations:
        stored = stored_metadata.get((relation.get("key"), relation.get("type")))
        if not stored:
            continue
        metadata = relation.get("metadata") or []
        mentioned = {item.get("key") for item in metadata}
        relation["metadata"] = [
            *metadata,
            *(item for key, item in stored.items() if key not in mentioned),
        ]
    return relations
