from importlib import import_module
from inuits_policy_based_auth import PolicyFactory, RequestContext


_policy_factory = PolicyFactory()


def init_policy_factory():
    from elody.loader import load_policies

    global _policy_factory
    try:
        permissions_module = import_module("apps.permissions")
        load_policies(_policy_factory, None, permissions_module.PERMISSIONS)
    except (ModuleNotFoundError, AttributeError):
        load_policies(_policy_factory, None)


def apply_policies(request_context: RequestContext):
    global _policy_factory
    return _policy_factory.apply_policies(request_context)


def authenticate(request_context: RequestContext):
    global _policy_factory
    return _policy_factory.authenticate(request_context)


def get_user_context():
    global _policy_factory
    return _policy_factory.get_user_context()
