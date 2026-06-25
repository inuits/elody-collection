"""Tests for the Typesense-facet distinct_by dropdown path.

A distinct_by request populates a filter dropdown. On a faceted field the distinct
values come from a Typesense facet (instant) instead of a Mongo $group scan over
the whole collection; one representative document per value is then hydrated from
Mongo. These tests cover the routing decision, sorting/pagination and the fallback
to Mongo when the query carries filters Typesense cannot express.
"""

import resources.base_filter_resource as mod


class _FakeColl:
    def __init__(self, docs_by_value):
        self._docs = docs_by_value

    def find_one(self, query):
        value = query["properties.material_type.value"]
        return self._docs.get(value)


class _FakeDB:
    def __init__(self, coll):
        self._coll = coll

    def __getitem__(self, name):
        return self._coll


class _FakeStorage:
    def __init__(self, coll):
        self.db = _FakeDB(coll)

    def _prepare_mongo_document(self, doc, create_sortable_metadata=True):
        return doc


def _make_resource(monkeypatch, facet, storage):
    res = mod.BaseFilterResource.__new__(mod.BaseFilterResource)
    monkeypatch.setattr(mod, "typesense_facet_values", lambda *a, **k: facet)
    monkeypatch.setattr(
        mod, "StorageManager", lambda: type("M", (), {"get_db_engine": lambda s: storage})()
    )
    res._resolve_mongo_collections = lambda tv, c: {"bibliographic_entities_actual"}
    res._add_cors_headers = lambda: None
    res._add_pagination_links = lambda items, skip, limit, collection: items
    return res


def _doc(value):
    return {"_id": value, "properties": {"material_type": {"value": value}}}


_QUERY = [
    {"type": "text", "value": "*", "distinct_by": "properties.material_type.value"},
    {"type": "selection", "key": "type", "value": ["manifestation_word"], "match_exact": True},
]


class TestDistinctOptions:
    def test_returns_facet_values_sorted_with_total_count(self, monkeypatch):
        facet = [("ARTIKEL", 300), ("BOEK", 1620), ("DVD", 125)]
        storage = _FakeStorage(_FakeColl({v: _doc(v) for v, _ in facet}))
        res = _make_resource(monkeypatch, facet, storage)

        out = res._execute_typesense_distinct_options(
            _QUERY, "entities", {"collection": "entities"},
            "properties.material_type.value", 0, 20, True,
        )

        assert out["count"] == 3  # total distinct values, not the page size
        values = [r["properties"]["material_type"]["value"] for r in out["results"]]
        assert values == ["ARTIKEL", "BOEK", "DVD"]  # sorted ascending by value

    def test_descending_sort_and_pagination(self, monkeypatch):
        facet = [("A", 1), ("B", 1), ("C", 1), ("D", 1)]
        storage = _FakeStorage(_FakeColl({v: _doc(v) for v, _ in facet}))
        res = _make_resource(monkeypatch, facet, storage)

        out = res._execute_typesense_distinct_options(
            _QUERY, "entities", {"collection": "entities"},
            "properties.material_type.value", 1, 2, False,  # desc, skip 1, limit 2
        )

        assert out["count"] == 4
        values = [r["properties"]["material_type"]["value"] for r in out["results"]]
        assert values == ["C", "B"]  # desc [D,C,B,A] -> skip 1, limit 2

    def test_skips_values_without_a_representative_document(self, monkeypatch):
        facet = [("A", 1), ("GONE", 1), ("B", 1)]
        storage = _FakeStorage(_FakeColl({"A": _doc("A"), "B": _doc("B")}))  # GONE missing
        res = _make_resource(monkeypatch, facet, storage)

        out = res._execute_typesense_distinct_options(
            _QUERY, "entities", {"collection": "entities"},
            "properties.material_type.value", 0, 20, True,
        )

        assert out["count"] == 3  # count still reflects all distinct facet values
        values = [r["properties"]["material_type"]["value"] for r in out["results"]]
        assert values == ["A", "B"]

    def test_falls_back_to_mongo_when_facet_unavailable(self, monkeypatch):
        storage = _FakeStorage(_FakeColl({}))
        res = _make_resource(monkeypatch, None, storage)  # facet returns None
        res._execute_advanced_search_with_query_v2 = (
            lambda q, c: {"results": [], "count": 0, "_fallback": True}
        )

        out = res._execute_typesense_distinct_options(
            _QUERY, "entities", {"collection": "entities"},
            "properties.material_type.value", 0, 20, True,
        )

        assert out.get("_fallback") is True

    def test_falls_back_to_mongo_when_unexpressible_filter_present(self, monkeypatch):
        storage = _FakeStorage(_FakeColl({}))
        res = _make_resource(monkeypatch, [("A", 1)], storage)
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
