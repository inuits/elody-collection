"""Tests for the whole-collection count short-circuit in mongo_filters.

A broad type listing (e.g. the home page) filters on every type a collection
holds, so the ``$count`` aggregation scans the entire index on every request.
When the filter provably covers all of the collection's types, the count equals
the collection size and is read from metadata via ``estimated_document_count``
(O(1), no scan). These tests cover the detection logic and the count decision.

Run from the api/ dir:

    pytest tests/test_mongo_filters_count.py -vv
"""

from filters_v2.mongo_filters import MongoFilters, get_type_only_filter_values


class TestGetTypeOnlyFilterValues:
    def test_in_list_on_type_returns_values(self):
        match = [{"$match": {"type": {"$in": ["work_word", "manifestation_word"]}}}]
        assert get_type_only_filter_values(match, []) == [
            "work_word",
            "manifestation_word",
        ]

    def test_scalar_type_returns_single_value(self):
        assert get_type_only_filter_values([{"$match": {"type": "person"}}], []) == [
            "person"
        ]

    def test_non_empty_group_disqualifies(self):
        match = [{"$match": {"type": {"$in": ["a"]}}}]
        group = [{"$group": {"_id": "$properties.x.value"}}]
        assert get_type_only_filter_values(match, group) is None

    def test_extra_predicate_key_disqualifies(self):
        match = [{"$match": {"type": {"$in": ["a"]}, "metadata.x": "y"}}]
        assert get_type_only_filter_values(match, []) is None

    def test_multiple_match_stages_disqualify(self):
        match = [
            {"$match": {"type": {"$in": ["a"]}}},
            {"$match": {"properties.y.value": "z"}},
        ]
        assert get_type_only_filter_values(match, []) is None

    def test_lookup_stage_disqualifies(self):
        match = [{"$lookup": {"from": "entities", "as": "rel"}}]
        assert get_type_only_filter_values(match, []) is None

    def test_non_in_operator_on_type_disqualifies(self):
        match = [{"$match": {"type": {"$ne": "person"}}}]
        assert get_type_only_filter_values(match, []) is None

    def test_empty_match_disqualifies(self):
        assert get_type_only_filter_values([], []) is None


class _FakeCollection:
    def __init__(self, types, estimated, agg_count):
        self._types = types
        self._estimated = estimated
        self._agg_count = agg_count
        self.estimated_calls = 0
        self.aggregate_calls = 0

    def distinct(self, field):
        assert field == "type"
        return list(self._types)

    def estimated_document_count(self):
        self.estimated_calls += 1
        return self._estimated

    def aggregate(self, pipeline, **kwargs):
        self.aggregate_calls += 1
        self.last_pipeline = list(pipeline)
        return iter([{"count": self._agg_count}])


class _FakeDB:
    def __init__(self, collection):
        self._collection = collection

    def __getitem__(self, name):
        return self._collection


class _FakeStorage:
    def __init__(self, collection):
        self.db = _FakeDB(collection)
        self.allow_disk_use = False


def _make_filters(collection):
    mf = MongoFilters.__new__(MongoFilters)
    mf.storage = _FakeStorage(collection)
    mf._distinct_types_cache = {}
    return mf


def _count(mf, match, group):
    # __count is name-mangled; output only matters for the empty-result fallback.
    return mf._MongoFilters__count("entities_actual", match, group, {"results": []})


class TestCountDecision:
    def test_uses_estimated_count_when_filter_covers_all_types(self):
        col = _FakeCollection(
            types={"work_word", "person"}, estimated=999, agg_count=42
        )
        mf = _make_filters(col)
        match = [{"$match": {"type": {"$in": ["work_word", "person", "extra_unused"]}}}]

        result = _count(mf, match, [])

        assert result == 999
        assert col.estimated_calls == 1
        assert col.aggregate_calls == 0

    def test_falls_back_to_aggregate_when_filter_misses_a_type(self):
        col = _FakeCollection(
            types={"work_word", "person", "siso"}, estimated=999, agg_count=42
        )
        mf = _make_filters(col)
        match = [{"$match": {"type": {"$in": ["work_word", "person"]}}}]

        result = _count(mf, match, [])

        assert result == 42
        assert col.estimated_calls == 0
        assert col.aggregate_calls == 1

    def test_falls_back_to_aggregate_for_non_type_only_match(self):
        col = _FakeCollection(types={"work_word"}, estimated=999, agg_count=42)
        mf = _make_filters(col)
        match = [{"$match": {"type": {"$in": ["work_word"]}, "metadata.x": "y"}}]

        result = _count(mf, match, [])

        assert result == 42
        assert col.aggregate_calls == 1

    def test_distinct_types_are_cached_between_calls(self):
        calls = {"n": 0}
        col = _FakeCollection(types={"work_word"}, estimated=5, agg_count=1)
        original = col.distinct

        def counting_distinct(field):
            calls["n"] += 1
            return original(field)

        col.distinct = counting_distinct
        mf = _make_filters(col)
        match = [{"$match": {"type": {"$in": ["work_word"]}}}]

        _count(mf, match, [])
        _count(mf, match, [])

        assert calls["n"] == 1  # second call served from the TTL cache


class TestFilteredCountCap:
    def test_filtered_count_pipeline_has_limit_cap_before_count(self):
        import filters_v2.mongo_filters as mf_mod

        col = _FakeCollection(types={"a", "b", "c"}, estimated=999, agg_count=101)
        mf = _make_filters(col)
        match = [{"$match": {"type": {"$in": ["a", "b"]}}}]  # narrower -> filtered

        _count(mf, match, [])

        # $limit (cap + 1) must sit directly before $count so the scan stops early.
        assert col.last_pipeline[-2:] == [
            {"$limit": mf_mod.LISTING_COUNT_CAP + 1},
            {"$count": "count"},
        ]

    def test_capped_count_value_is_returned_as_is(self):
        # agg_count == cap + 1 is the "<cap>+" sentinel; returned unchanged.
        import filters_v2.mongo_filters as mf_mod

        sentinel = mf_mod.LISTING_COUNT_CAP + 1
        col = _FakeCollection(types={"a", "b", "c"}, estimated=999, agg_count=sentinel)
        mf = _make_filters(col)

        result = _count(mf, [{"$match": {"type": {"$in": ["a"]}}}], [])

        assert result == sentinel

    def test_whole_collection_count_is_not_capped(self):
        # covers all types -> estimated_document_count, exact and uncapped.
        col = _FakeCollection(types={"a", "b"}, estimated=5_000_000, agg_count=101)
        mf = _make_filters(col)

        result = _count(mf, [{"$match": {"type": {"$in": ["a", "b"]}}}], [])

        assert result == 5_000_000
        assert col.aggregate_calls == 0

    def test_cap_disabled_omits_limit_stage(self, monkeypatch):
        import filters_v2.mongo_filters as mf_mod

        monkeypatch.setattr(mf_mod, "LISTING_COUNT_CAP", 0)
        col = _FakeCollection(types={"a", "b", "c"}, estimated=999, agg_count=700712)
        mf = _make_filters(col)

        result = _count(mf, [{"$match": {"type": {"$in": ["a"]}}}], [])

        assert result == 700712
        assert col.last_pipeline == [
            {"$match": {"type": {"$in": ["a"]}}},
            {"$count": "count"},
        ]
