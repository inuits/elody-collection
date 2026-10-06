"""Opt-in relation mirroring for object-configured entity types.

The classic storage path stores every relation on both entities (has<X> on
one, is<X>For on the other). Entity types with an object configuration write
only the side the request names; RelationMirroring keeps the other side, so
an inverse path (SHACL UI: sh:path [ sh:inversePath ex:member ]) can be read
and written from either entity.
"""

from object_configurations.relation_mirroring import (
    mirror_changes,
    mirror_relation_type,
)


def doc(_id, *relations):
    return {"_id": _id, "relations": [{"key": k, "type": t} for t, k in relations]}


class TestMirrorRelationType:
    def test_has_and_is_for_are_each_others_mirror(self):
        assert mirror_relation_type("hasMember") == "isMemberFor"
        assert mirror_relation_type("isMemberFor") == "hasMember"

    def test_the_named_pairs(self):
        assert mirror_relation_type("hasMediafile") == "belongsTo"
        assert mirror_relation_type("belongsTo") == "hasMediafile"
        assert mirror_relation_type("authored") == "authoredBy"

    def test_a_type_without_a_mirror(self):
        assert mirror_relation_type("relatedTo") is None


class TestMirrorChanges:
    def test_a_created_document_mirrors_every_relation(self):
        added, removed = mirror_changes(None, doc("book", ("isMemberFor", "c1")))
        assert added == [("c1", {"key": "book", "type": "hasMember"})]
        assert removed == []

    def test_an_update_mirrors_only_what_changed(self):
        before = doc("book", ("isMemberFor", "c1"), ("isMemberFor", "c2"), ("hasFeaturedIn", "c1"))
        after = doc("book", ("isMemberFor", "c1"), ("isMemberFor", "c3"), ("hasFeaturedIn", "c1"))
        added, removed = mirror_changes(before, after)
        assert added == [("c3", {"key": "book", "type": "hasMember"})]
        assert removed == [("c2", {"key": "book", "type": "hasMember"})]

    def test_two_relation_types_to_one_entity_are_kept_apart(self):
        before = doc("book", ("isMemberFor", "c1"), ("hasFeaturedIn", "c1"))
        after = doc("book", ("hasFeaturedIn", "c1"))
        added, removed = mirror_changes(before, after)
        assert added == []
        assert removed == [("c1", {"key": "book", "type": "hasMember"})]

    def test_a_deleted_document_removes_every_mirror(self):
        added, removed = mirror_changes(doc("book", ("isMemberFor", "c1")), None)
        assert added == []
        assert removed == [("c1", {"key": "book", "type": "hasMember"})]

    def test_relations_without_a_mirror_type_are_left_alone(self):
        added, removed = mirror_changes(None, doc("book", ("relatedTo", "c1")))
        assert (added, removed) == ([], [])

    def test_relation_metadata_changes_alone_mirror_nothing(self):
        before = {"_id": "book", "relations": [{"key": "c1", "type": "isMemberFor", "metadata": []}]}
        after = {"_id": "book", "relations": [{"key": "c1", "type": "isMemberFor", "metadata": [{"key": "order", "value": 1}]}]}
        assert mirror_changes(before, after) == ([], [])
