"""Authorization for the merge endpoints.

A merge deletes an entity, so update on the survivor is not enough on its own,
and the endpoint must never be reachable without a verdict.
"""

from unittest.mock import patch

import pytest
from flask import Flask
from inuits_policy_based_auth.contexts.policy_context import PolicyContext

from policies.authorization.merge_policy import MergePolicy


class FakeTenant:
    roles = ["editor"]


class FakeUserContext:
    bag = {}
    x_tenant = FakeTenant()


class FakeRequestContext:
    def __init__(self, http_request):
        self.http_request = http_request


@pytest.fixture
def granted_permissions():
    """Records what the policy asked permission for, and grants everything."""
    asked = []

    def handle(user_context, item, permissions, permission, *args, **kwargs):
        asked.append((item, permission))
        return True

    with (
        patch(
            "policies.authorization.merge_policy.get_item",
            side_effect=lambda _storage, _bag, view_args: view_args["id"],
        ),
        patch(
            "policies.authorization.merge_policy.get_permissions",
            return_value={"any": True},
        ),
        patch(
            "policies.authorization.merge_policy.handle_single_item_request",
            side_effect=handle,
        ),
        patch("policies.authorization.merge_policy.StorageManager"),
    ):
        yield asked


def authorize(path, body=None, method="POST", content=None):
    app = Flask(__name__)
    with app.test_request_context(path, method=method, json=body or {}):
        from flask import g, request

        request.view_args = {"id": path.split("/")[2]}
        if content is not None:
            g.content = content
        policy_context = MergePolicy().authorize(
            PolicyContext(), FakeUserContext(), FakeRequestContext(request)
        )
    return policy_context


class TestMergePolicy:
    def test_requires_update_on_the_survivor_and_delete_on_the_victim(
        self, granted_permissions
    ):
        verdict = authorize("/entities/PERS-1/merge", {"victim_id": "PERS-2"})

        assert verdict.access_verdict is True
        assert granted_permissions == [("PERS-1", "update"), ("PERS-2", "delete")]

    def test_requires_read_to_count_inbound_references(self, granted_permissions):
        verdict = authorize("/entities/PERS-1/inbound-reference-count", method="GET")

        assert verdict.access_verdict is True
        assert granted_permissions == [("PERS-1", "read")]

    def test_leaves_paths_it_does_not_own_alone(self, granted_permissions):
        verdict = authorize("/entities/PERS-1", method="GET")

        assert verdict.access_verdict is None
        assert granted_permissions == []

    def test_survives_a_g_content_belonging_to_another_resource(
        self, granted_permissions
    ):
        """A merge calls into the relations resource, which puts its own list in
        g.content — the policy runs again on that same request."""
        verdict = authorize(
            "/entities/PERS-1/merge",
            {"victim_id": "PERS-2"},
            content=[{"key": "W-1", "type": "refAuthors"}],
        )

        assert verdict.access_verdict is True
        assert granted_permissions == [("PERS-1", "update"), ("PERS-2", "delete")]

    def test_checks_the_survivor_when_no_victim_was_given(self, granted_permissions):
        """The handler answers 400 for a body without a victim; denying it here
        would report the wrong reason."""
        verdict = authorize("/entities/PERS-1/merge", {})

        assert verdict.access_verdict is True
        assert granted_permissions == [("PERS-1", "update")]
