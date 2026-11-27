import re

from flask import Request, jsonify

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
# POST is also how the filter endpoints read: /filter, /filter_deprecated, /filter/matchers...
READ_ONLY_POST = re.compile(r"/filter[^/]*(/|$)")


def block_writes(request: Request, maintenance_mode: bool):
    if (
        not maintenance_mode
        or request.method in SAFE_METHODS
        or (request.method == "POST" and READ_ONLY_POST.search(request.path))
    ):
        return None

    response = jsonify(
        message="Elody is in maintenance mode: changes are not possible right now."
    )
    response.status_code = 503
    response.headers["Retry-After"] = "600"
    return response
