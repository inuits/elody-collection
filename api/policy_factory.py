from flask import g
from importlib import import_module
from inuits_policy_based_auth import PolicyFactory, RequestContext
from inuits_policy_based_auth.exceptions import NoUserContextException
import os
import sys


def _ensure_global_policies_path():
    policies_dir = os.path.join(os.path.dirname(__file__), "policies")
    if os.path.isdir(policies_dir) and policies_dir not in sys.path:
        sys.path.insert(0, policies_dir)


def init_policy_factory():
    from elody.loader import load_policies

    global _policy_factory

    _ensure_global_policies_path()

    try:
        permissions_module = import_module("apps.permissions")
        load_policies(
            _policy_factory,
            None,
            permissions_module.PERMISSIONS,
            permissions_module.PLACEHOLDERS,
        )
    except (ModuleNotFoundError, AttributeError):
        load_policies(_policy_factory, None)


def apply_policies(request_context: RequestContext):
    global _policy_factory
    return _policy_factory.apply_policies(request_context)


def authenticate(request_context: RequestContext):
    global _policy_factory
    return _policy_factory.authenticate(request_context)


def get_user_context():
    try:
        user_context = g.get("user_context")
        if not user_context:
            raise NoUserContextException()
    except Exception as exception:
        raise exception

    return user_context


def user_context_setter(user_context):
    g.user_context = user_context


_policy_factory = PolicyFactory(user_context_setter)
