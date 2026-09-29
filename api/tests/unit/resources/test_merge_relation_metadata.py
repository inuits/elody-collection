from resources.base.relations import merge_relation_metadata


def relation(key, metadata, type="hasOrganization"):
    return {"type": type, "key": key, "metadata": metadata}


def item(key, value):
    return {"key": key, "value": value}


STORED = [
    relation("ORG-1", [item("roles", ["admin"]), item("function", ["technical"])]),
    relation("ORG-2", [item("roles", ["member"])]),
]


def metadata_of(relation, key):
    for stored in relation["metadata"]:
        if stored["key"] == key:
            return stored["value"]
    return None


def test_a_key_the_request_does_not_mention_keeps_its_stored_value():
    relations = merge_relation_metadata(
        STORED, [relation("ORG-1", [item("function", ["communication"])])]
    )

    assert metadata_of(relations[0], "roles") == ["admin"]
    assert metadata_of(relations[0], "function") == ["communication"]


def test_a_key_the_request_does_mention_wins():
    relations = merge_relation_metadata(
        STORED, [relation("ORG-1", [item("roles", ["member"])])]
    )

    assert metadata_of(relations[0], "roles") == ["member"]


def test_an_explicitly_emptied_key_is_not_refilled():
    relations = merge_relation_metadata(STORED, [relation("ORG-1", [item("roles", [])])])

    assert metadata_of(relations[0], "roles") == []


def test_a_relation_that_is_not_stored_yet_is_left_as_sent():
    new = relation("ORG-NEW", [item("roles", ["admin"])])

    assert merge_relation_metadata(STORED, [new]) == [new]


def test_a_relation_absent_from_the_request_is_not_re_added():
    relations = merge_relation_metadata(STORED, [relation("ORG-1", [])])

    assert [candidate["key"] for candidate in relations] == ["ORG-1"]


def test_a_relation_of_another_type_with_the_same_key_does_not_borrow_metadata():
    relations = merge_relation_metadata(
        STORED, [relation("ORG-1", [], type="hasProduction")]
    )

    assert relations[0]["metadata"] == []


def test_a_relation_without_metadata_still_gets_the_stored_keys():
    relations = merge_relation_metadata(
        STORED, [{"type": "hasOrganization", "key": "ORG-2"}]
    )

    assert metadata_of(relations[0], "roles") == ["member"]


def test_nothing_stored_leaves_the_request_untouched():
    sent = [relation("ORG-1", [item("roles", ["admin"])])]

    assert merge_relation_metadata([], sent) == sent
