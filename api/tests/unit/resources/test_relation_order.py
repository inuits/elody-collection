import pytest

from resources.base.relation_order import (
    apply_order,
    is_relation_type,
    order_rows_csv,
    parse_order_csv,
    sort_by_order,
)


def relation(key, order=None, relation_type="hasMediafile", **metadata):
    items = [{"key": name, "value": value} for name, value in metadata.items()]
    if order is not None:
        items.append({"key": "order", "value": order})
    return {"key": key, "type": relation_type, "metadata": items}


def order_of(relation_item):
    return next(
        item["value"] for item in relation_item["metadata"] if item["key"] == "order"
    )


class TestSortByOrder:
    def test_ordered_first_then_unordered_in_stored_order(self):
        relations = [relation("c"), relation("a", 2), relation("d"), relation("b", 1)]
        keys = [item["key"] for item in sort_by_order(relations)]
        assert keys == ["b", "a", "c", "d"]

    def test_stored_numbers_may_be_floats(self):
        relations = [relation("a", 2.0), relation("b", 1.0)]
        assert [item["key"] for item in sort_by_order(relations)] == ["b", "a"]


class TestApplyOrder:
    def test_numbers_and_sorts_the_relations_of_one_type(self):
        relations = [
            relation("x", relation_type="isIn"),
            relation("a", 1, coordinates={"x": 1}),
            relation("b", 2),
            relation("c"),
        ]
        result = apply_order(relations, "hasMediafile", ["c", "a", "b"])

        mediafiles = [item for item in result if item["type"] == "hasMediafile"]
        assert [item["key"] for item in mediafiles] == ["c", "a", "b"]
        assert [order_of(item) for item in mediafiles] == [1, 2, 3]
        assert {"key": "coordinates", "value": {"x": 1}} in mediafiles[1]["metadata"]
        assert result[0]["key"] == "x"

    def test_matches_snake_and_camel_case_relation_types(self):
        relations = [
            relation("a", relation_type="has_mediafile"),
            relation("b", relation_type="has_mediafile"),
        ]
        result = apply_order(relations, "hasMediafile", ["b", "a"])
        assert [item["key"] for item in result] == ["b", "a"]
        assert {item["type"] for item in result} == {"has_mediafile"}

    def test_keeps_a_relation_that_is_stored_twice(self):
        relations = [relation("a"), relation("b"), relation("a")]
        result = apply_order(relations, "hasMediafile", ["a", "b", "a"])
        assert [item["key"] for item in result] == ["a", "b", "a"]
        assert [order_of(item) for item in result] == [1, 2, 3]

    def test_rejects_keys_the_parent_does_not_hold(self):
        with pytest.raises(ValueError):
            apply_order([relation("a")], "hasMediafile", ["a", "zzz"])

    def test_rejects_a_list_that_leaves_relations_out(self):
        with pytest.raises(ValueError):
            apply_order([relation("a"), relation("b")], "hasMediafile", ["a"])


class TestCsv:
    def test_writes_one_row_per_item(self):
        csv = order_rows_csv([("M-1", "mediafile"), ("M-2", "mediafile")])
        assert csv.splitlines() == ["id,type", "M-1,mediafile", "M-2,mediafile"]

    def test_reads_the_ids_in_order(self):
        assert parse_order_csv("id,type,name\nS-2,frame,Two\nS-1,frame,One\n") == [
            "S-2",
            "S-1",
        ]


class TestIsRelationType:
    def test_compares_snake_and_camel_case(self):
        assert is_relation_type({"type": "has_mediafile"}, "hasMediafile")
        assert is_relation_type({"type": "hasMediafile"}, "has_mediafile")
        assert not is_relation_type({"type": "has_tag"}, "hasMediafile")
