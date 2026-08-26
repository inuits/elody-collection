"""
A permission restriction on a relation (`key@type-key`) becomes a nested `or`
filter whose sub-pipeline starts with $lookup/$unwind instead of $match.
Extracting the OR condition must not assume the $match is the first stage, and
the lookups it depends on must end up in the parent pipeline.

Run from the api/ dir:

    pytest tests/unit/filters/test_nested_or_with_lookup.py -vv
"""

import os

import pytest

from filters_v2.stages import match_stage


LOOKUP = {
    "from": "entities",
    "local_field": "properties.ref_productions.value",
    "foreign_field": "identifiers",
    "as": "__lookup.virtual_relations.production",
    "preserve_null_and_empty_arrays": True,
}


def _restriction(policy_signature):
    return {
        "lookup": LOOKUP,
        "type": "selection",
        "key": [
            "podiumnet:1|?__lookup.virtual_relations.production.properties.ref_booking_agency.value"
        ],
        "value": ["ORG-3W79ZAGB"],
        "match_exact": True,
        "or": [],
        "policy_signature": policy_signature,
    }


def _filter_request_body(policy_signature):
    # Mirrors what PodiumnetFilterPolicy hangs the restriction filters onto.
    return [
        {
            "type": "selection",
            "key": "type",
            "value": ["mediafile"],
            "match_exact": True,
        },
        {
            "type": "boolean",
            "key": "alwaysFalse",
            "value": True,
            "match_exact": True,
            "policy_signature": policy_signature,
            "or": [_restriction(policy_signature)],
        },
    ]


@pytest.fixture
def static_jwt(monkeypatch):
    monkeypatch.setitem(os.environ, "STATIC_JWT", "static-jwt")
    return "static-jwt"


class TestNestedOrFilterWithLookup:
    def test_lookup_is_hoisted_before_the_match_that_uses_it(self, static_jwt):
        pipeline = match_stage.build(_filter_request_body(static_jwt), True)

        lookup_index = next(
            index for index, stage in enumerate(pipeline) if "$lookup" in stage
        )
        or_match_index = next(
            index
            for index, stage in enumerate(pipeline)
            if "$or" in stage.get("$match", {})
        )
        assert lookup_index < or_match_index
        assert pipeline[lookup_index]["$lookup"]["as"] == LOOKUP["as"]

    def test_restriction_ends_up_in_the_or_condition(self, static_jwt):
        pipeline = match_stage.build(_filter_request_body(static_jwt), True)

        or_conditions = [
            stage["$match"]["$or"]
            for stage in pipeline
            if "$or" in stage.get("$match", {})
        ][0]
        assert {
            f"{LOOKUP['as']}.properties.ref_booking_agency.value": "ORG-3W79ZAGB"
        } in or_conditions

    def test_unwind_is_not_hoisted_into_the_listing_pipeline(self, static_jwt):
        # An $unwind here would repeat a mediafile once per linked production,
        # and nothing downstream groups the duplicates back away.
        pipeline = match_stage.build(_filter_request_body(static_jwt), True)

        assert not [stage for stage in pipeline if "$unwind" in stage]

    def test_works_without_policy_signatures(self):
        # Same shape, but nothing gets split off into the policy-signatured match.
        pipeline = match_stage.build(_filter_request_body(None), True)

        assert any("$lookup" in stage for stage in pipeline)
        assert any("$or" in stage.get("$match", {}) for stage in pipeline)
