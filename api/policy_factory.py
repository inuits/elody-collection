from app_context import g
from flask import has_request_context
from inuits_policy_based_auth import PolicyFactory, RequestContext
from inuits_policy_based_auth.exceptions import NoUserContextException
from logging_elody.log import log
from plugins import concatenated, merged


def init_policy_factory():
    from elody.loader import load_policies

    global _policy_factory
    load_policies(
        _policy_factory,
        log,
        merged(lambda plugin: plugin.permissions()),
        concatenated(lambda plugin: plugin.permission_placeholders()),
    )


def apply_policies(request_context: RequestContext, user_context_getter=None):
    global _policy_factory
    return _policy_factory.apply_policies(request_context, user_context_getter)


def authenticate(request_context: RequestContext, user_context_getter=None):
    global _policy_factory
    return _policy_factory.authenticate(request_context, user_context_getter)


def get_user_context():
    try:
        user_context = g.get("user_context")
        if not user_context:
            raise NoUserContextException()
    except Exception as exception:
        raise exception

    return user_context


def get_user_context_for_current_request():
    if not has_request_context():
        return None

    return g.get("user_context")


def user_context_setter(user_context):
    g.user_context = user_context


_policy_factory = PolicyFactory(user_context_setter)
