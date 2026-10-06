"""Tests for ordering distinct_by groups by the requested sort field.

A distinct_by request returns one representative document per distinct value
(``$group`` + ``$first``). When an ``order_by`` is given, the representative must
be the first document in that order, so the groups themselves can be ordered by
it (e.g. comment categories by most recent activity). The sort therefore runs
both before the ``$group`` (to pick the representative) and after it (``$group``
does not preserve order). Without an ``order_by`` the pipeline is unchanged.

Run from the api/ dir:

    pytest tests/test_mongo_filters_group_order.py -vv
"""

import filters_v2.mongo_filters as mod
from filters_v2.mongo_filters import MongoFilters

MATCH = [{"$match": "match"}]
GROUP = [{"$group": "group"}, {"$replaceRoot": "replace_root"}]
SORT = [{"$sort": "sort"}]
SKIP = [{"$skip": "skip"}]
LIMIT = [{"$limit": "limit"}]


def _build(monkeypatch, *, distinct_by, order_by):
    monkeypatch.setattr(mod.match_stage, "build", lambda *a, **k: MATCH)
    monkeypatch.setattr(
        mod.group_stage, "build", lambda key: GROUP if key else []
    )
    monkeypatch.setattr(
        mod.sort_stage, "build", lambda order_by, *a, **k: SORT if order_by else []
    )
    monkeypatch.setattr(mod.skip_stage, "build", lambda *a, **k: SKIP)
    monkeypatch.setattr(mod.limit_stage, "build", lambda *a, **k: LIMIT)
    monkeypatch.setattr(mod, "get_distinct_by", lambda body: distinct_by)
    monkeypatch.setattr(mod, "has_bucket_filter", lambda body: None)

    mf = MongoFilters.__new__(MongoFilters)
    mf.storage = None
    pipeline, match, group = mf._MongoFilters__build_aggregation_query(
        [], 0, 20, order_by, False, {}, [], True
    )
    return pipeline, match, group


class TestDistinctByGroupOrder:
    def test_sorts_before_and_after_group_when_order_by_is_given(self, monkeypatch):
        pipeline, _, _ = _build(
            monkeypatch,
            distinct_by="properties.category.value",
            order_by="properties.last_activity_at.value",
        )

        assert pipeline == [*MATCH, *SORT, *GROUP, *SORT, *SKIP, *LIMIT]

    def test_count_pipeline_parts_exclude_the_pre_group_sort(self, monkeypatch):
        _, match, group = _build(
            monkeypatch,
            distinct_by="properties.category.value",
            order_by="properties.last_activity_at.value",
        )

        assert match == MATCH
        assert group == GROUP

    def test_distinct_by_without_order_by_is_unchanged(self, monkeypatch):
        pipeline, _, _ = _build(
            monkeypatch, distinct_by="properties.category.value", order_by=""
        )

        assert pipeline == [*MATCH, *GROUP, *SKIP, *LIMIT]

    def test_order_by_without_distinct_by_is_unchanged(self, monkeypatch):
        pipeline, _, _ = _build(
            monkeypatch,
            distinct_by=None,
            order_by="properties.last_activity_at.value",
        )

        assert pipeline == [*MATCH, *SORT, *SKIP, *LIMIT]
