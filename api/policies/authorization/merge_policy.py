import re as regex

from elody.policies.helpers import get_item
from elody.policies.permission_handler import (
    get_permissions,
    handle_single_item_request,
)
from flask import g, Request  # pyright: ignore
from inuits_policy_based_auth import BaseAuthorizationPolicy  # pyright: ignore
from inuits_policy_based_auth.contexts.policy_context import (  # pyright: ignore
    PolicyContext,
)
from inuits_policy_based_auth.contexts.user_context import (  # pyright: ignore
    UserContext,
)
from storage.storagemanager import StorageManager  # pyright: ignore


class MergePolicy(BaseAuthorizationPolicy):
    """A merge updates the survivor and deletes the victim.

    Both permissions are required — update alone would otherwise let a user
    delete an entity by merging it away.
    """

    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not regex.match("^(/[^/]+/v[0-9]+)?/[^/]+/[^/]+/merge$", request.path):
            return policy_context

        survivor = get_item(StorageManager(), user_context.bag, request.view_args)
        victim = get_item(
            StorageManager(), user_context.bag, {"id": self.__victim_id(request)}
        )

        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            policy_context.access_verdict = bool(
                handle_single_item_request(
                    user_context, survivor, permissions, "update"
                )
                and handle_single_item_request(
                    user_context, victim, permissions, "delete"
                )
            )
            if policy_context.access_verdict:
                return policy_context

        return policy_context

    def __victim_id(self, request: Request):
        content = g.get("content") or request.get_json(silent=True) or {}
        return content.get("victim_id")
