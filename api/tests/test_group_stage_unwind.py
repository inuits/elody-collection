"""Tests for unwinding list values before grouping on a distinct_by field.

Run from the api/ dir:

    pytest tests/test_group_stage_unwind.py -vv
"""

import filters_v2.stages.group_stage as group_stage

GROUP = [
    {"$group": {"_id": "$properties.tags.value", "document": {"$first": "$$ROOT"}}},
    {"$replaceRoot": {"newRoot": "$document"}},
]


def _no_object_lists(monkeypatch):
    monkeypatch.setattr(group_stage.add_fields_stage, "build", lambda key: [])


class TestGroupStageUnwind:
    def test_unwinds_the_field_before_grouping_when_requested(self, monkeypatch):
        _no_object_lists(monkeypatch)

        stages = group_stage.build("properties.tags.value", unwind=True)

        assert stages == [
            {
                "$unwind": {
                    "path": "$properties.tags.value",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            *GROUP,
        ]

    def test_does_not_unwind_by_default(self, monkeypatch):
        _no_object_lists(monkeypatch)

        assert group_stage.build("properties.tags.value") == GROUP

    def test_builds_nothing_without_a_key(self):
        assert group_stage.build("", unwind=True) == []
