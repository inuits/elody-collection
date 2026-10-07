"""
An "and" filter on a key that an "or" filter already uses must narrow the
or-combination instead of being dropped as a duplicate key.

Run from the api/ dir:

    pytest tests/unit/filters/test_and_filter_next_to_or_filter.py -vv
"""

from filters_v2.matchers.base_matchers import BaseMatchers
from filters_v2.stages import match_stage

USERS = "vlacc:1|properties.thread_tagged_users.value"
GROUPS = "vlacc:1|properties.thread_tagged_groups.value"

TYPE_FILTER = {
    "type": "selection",
    "key": "type",
    "value": ["comment"],
    "match_exact": True,
}
TAGGED_ME_OR_MY_GROUPS = [
    {"type": "text", "key": [USERS], "value": "U-ME", "operator": "or"},
    {
        "type": "selection",
        "key": [GROUPS],
        "value": ["GRP-1", "GRP-2"],
        "match_exact": True,
        "operator": "or",
    },
]


def _match(filter_request_body):
    with BaseMatchers.context(
        collection="entities", type_name="comment", force_base=True
    ):
        pipeline = match_stage.build(filter_request_body, True)
    return [stage["$match"] for stage in pipeline if "$match" in stage][0]


class TestAndFilterNextToOrFilter:
    def test_keeps_an_and_filter_on_a_key_used_by_an_or_filter(self):
        match = _match(
            [
                TYPE_FILTER,
                *TAGGED_ME_OR_MY_GROUPS,
                {
                    "type": "selection",
                    "key": [GROUPS],
                    "value": ["GRP-1"],
                    "match_exact": True,
                },
            ]
        )

        assert len(match["$or"]) == 2
        assert match["properties.thread_tagged_groups.value"] == "GRP-1"

    def test_still_drops_a_second_and_filter_on_the_same_key(self):
        match = _match(
            [
                TYPE_FILTER,
                {
                    "type": "selection",
                    "key": [GROUPS],
                    "value": ["GRP-1"],
                    "match_exact": True,
                },
                {
                    "type": "selection",
                    "key": [GROUPS],
                    "value": ["GRP-2"],
                    "match_exact": True,
                },
            ]
        )

        assert match["properties.thread_tagged_groups.value"] == "GRP-1"
