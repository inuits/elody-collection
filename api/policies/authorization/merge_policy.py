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


MERGE_PATH = regex.compile("^(/[^/]+/v[0-9]+)?/[^/]+/[^/]+/merge$")
INBOUND_REFERENCE_COUNT_PATH = regex.compile(
    "^(/[^/]+/v[0-9]+)?/[^/]+/[^/]+/inbound-reference-count$"
)


class MergePolicy(BaseAuthorizationPolicy):
    """A merge updates the survivor and deletes the victim.

    Both permissions are required — update alone would otherwise let a user
    delete an entity by merging it away.
    """

    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if MERGE_PATH.match(request.path):
            requirements = self.__merge_requirements(user_context, request)
        elif INBOUND_REFERENCE_COUNT_PATH.match(request.path):
            requirements = [(self.__item(user_context, request.view_args), "read")]
        else:
            return policy_context

        for role in user_context.x_tenant.roles:
            permissions = get_permissions(role, user_context)
            if not permissions:
                continue

            policy_context.access_verdict = all(
                handle_single_item_request(user_context, item, permissions, permission)
                for item, permission in requirements
            )
            if policy_context.access_verdict:
                return policy_context

        return policy_context

    def __merge_requirements(self, user_context: UserContext, request: Request):
        requirements = [(self.__item(user_context, request.view_args), "update")]
        if victim_id := self.__victim_id(request):
            requirements.append(
                (self.__item(user_context, {"id": victim_id}), "delete")
            )
        return requirements

    def __item(self, user_context: UserContext, view_args):
        return get_item(StorageManager(), user_context.bag, view_args)

    def __victim_id(self, request: Request):
        content = g.get("content")
        if not isinstance(content, dict):
            content = request.get_json(silent=True) or {}
        return content.get("victim_id")
