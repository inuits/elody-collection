from flask import Request
from inuits_policy_based_auth.authentication.base_authentication_policy import (
    BaseAuthenticationPolicy,
)


class XUserHeadersPolicy(BaseAuthenticationPolicy):
    def authenticate(self, user_context, request_context):
        request: Request = request_context.http_request
        user_context.id = request.headers.get("X-User-Id", user_context.id)
        user_context.email = request.headers.get("X-User-Email", user_context.email)
        return user_context
