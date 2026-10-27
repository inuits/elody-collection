"""Tests for grouping filter options on several distinct_by keys.

Run from the api/ dir:

    pytest tests/test_group_stage_multiple_keys.py -vv
"""

import filters_v2.stages.group_stage as group_stage
from filters_v2.helpers.base_helper import get_distinct_by_keys


def _no_object_lists(monkeypatch):
    monkeypatch.setattr(group_stage.add_fields_stage, "build", lambda key: [])


class TestGroupStageMultipleKeys:
    def test_groups_on_the_first_non_empty_key(self, monkeypatch):
        _no_object_lists(monkeypatch)

        stages = group_stage.build(
            ["properties.name.value", "properties.title.value", "properties.code.value"]
        )

        assert stages == [
            {
                "$group": {
                    "_id": {
                        "$ifNull": [
                            "$properties.name.value",
                            {
                                "$ifNull": [
                                    "$properties.title.value",
                                    "$properties.code.value",
                                ]
                            },
                        ]
                    },
                    "document": {"$first": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$document"}},
        ]

    def test_does_not_unwind_a_combined_key(self, monkeypatch):
        _no_object_lists(monkeypatch)

        stages = group_stage.build(
            ["properties.name.value", "properties.title.value"], unwind=True
        )

        assert not any("$unwind" in stage for stage in stages)

    def test_a_single_key_list_groups_like_a_plain_key(self, monkeypatch):
        _no_object_lists(monkeypatch)

        assert group_stage.build(["properties.tags.value"]) == group_stage.build(
            "properties.tags.value"
        )

    def test_builds_nothing_without_keys(self):
        assert group_stage.build([]) == []


class TestGetDistinctByKeys:
    def test_returns_every_distinct_by_once_in_request_order(self):
        body = [
            {"type": "text", "distinct_by": "properties.name.value"},
            {"type": "text", "distinct_by": "properties.title.value"},
            {"type": "text", "distinct_by": "properties.name.value"},
            {"type": "selection", "key": "type"},
        ]

        assert get_distinct_by_keys(body) == [
            "properties.name.value",
            "properties.title.value",
        ]

    def test_returns_nothing_without_distinct_by(self):
        assert get_distinct_by_keys([{"type": "text"}]) == []
