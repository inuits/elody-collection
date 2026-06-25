"""Tests for the Typesense-group_by distinct_by dropdown path.

A distinct_by request populates a filter dropdown. On a faceted field the
distinct values + one representative document id per value come from a Typesense
group_by (instant); the ids are sorted by value, paginated, and hydrated from
Mongo BY _id (indexed) — not by the field value (unindexed). These tests cover
the routing decision, sorting/pagination by value, and the fallback to Mongo.
"""

import resources.base_filter_resource as mod


def _make_resource(monkeypatch, group_pairs):
    res = mod.BaseFilterResource.__new__(mod.BaseFilterResource)
    monkeypatch.setattr(mod, "typesense_group_values", lambda *a, **k: group_pairs)
    res._resolve_mongo_collections = lambda tv, c: {"bibliographic_entities_actual"}
    # hydrate-by-id stub: returns a doc per requested id, preserving order.
    res._fetch_documents_from_mongo = lambda ids, collections: [
        {"_id": i, "value": i} for i in ids
    ]
    res._add_cors_headers = lambda: None
    res._add_pagination_links = lambda items, skip, limit, collection: items
    return res


_QUERY = [
    {"type": "text", "value": "*", "distinct_by": "properties.material_type.value"},
    {"type": "selection", "key": "type", "value": ["manifestation_word"], "match_exact": True},
]


class TestDistinctOptions:
    def test_returns_ids_sorted_by_value_with_total_count(self, monkeypatch):
        # group_by yields (value, representative_id), out of value order
        pairs = [("BOEK", "id_boek"), ("ARTIKEL", "id_art"), ("DVD", "id_dvd")]
        res = _make_resource(monkeypatch, pairs)

        out = res._execute_typesense_distinct_options(
            _QUERY, "entities", {"collection": "entities"},
            "properties.material_type.value", 0, 20, True,
        )

        assert out["count"] == 3  # total distinct values, not the page size
        # hydrated by _id, ordered by value ascending (ARTIKEL, BOEK, DVD)
        assert [d["_id"] for d in out["results"]] == ["id_art", "id_boek", "id_dvd"]

    def test_descending_sort_and_pagination(self, monkeypatch):
        pairs = [("A", "ia"), ("B", "ib"), ("C", "ic"), ("D", "id")]
        res = _make_resource(monkeypatch, pairs)

        out = res._execute_typesense_distinct_options(
            _QUERY, "entities", {"collection": "entities"},
            "properties.material_type.value", 1, 2, False,  # desc, skip 1, limit 2
        )

        assert out["count"] == 4
        # desc [D,C,B,A] -> skip 1, limit 2 -> C, B
        assert [d["_id"] for d in out["results"]] == ["ic", "ib"]

    def test_falls_back_to_mongo_when_group_by_unavailable(self, monkeypatch):
        res = _make_resource(monkeypatch, None)  # group_values returns None
        res._execute_advanced_search_with_query_v2 = (
            lambda q, c: {"results": [], "count": 0, "_fallback": True}
        )

        out = res._execute_typesense_distinct_options(
            _QUERY, "entities", {"collection": "entities"},
            "properties.material_type.value", 0, 20, True,
        )

        assert out.get("_fallback") is True

    def test_falls_back_to_mongo_when_unexpressible_filter_present(self, monkeypatch):
        res = _make_resource(monkeypatch, [("A", "ia")])
        res._execute_advanced_search_with_query_v2 = (
            lambda q, c: {"results": [], "count": 0, "_fallback": True}
        )
        # an exact-match selection filter Typesense would need to express
        query = _QUERY + [
            {"type": "selection", "key": "properties.genre.value", "value": "x", "match_exact": True}
        ]

        out = res._execute_typesense_distinct_options(
            query, "entities", {"collection": "entities"},
            "properties.material_type.value", 0, 20, True,
        )

        assert out.get("_fallback") is True
