from flask import Request, Response

ALLOWED_METHODS = "GET, POST, PUT, PATCH, DELETE, OPTIONS"


def add_cors_headers(
    request: Request, response: Response, allowed_origins: list[str]
) -> Response:
    origin = request.headers.get("Origin")
    if not origin or ("*" not in allowed_origins and origin not in allowed_origins):
        return response

    response.headers["Access-Control-Allow-Origin"] = origin
    response.vary.add("Origin")
    if request.method == "OPTIONS":
        response.headers["Access-Control-Allow-Methods"] = ALLOWED_METHODS
        if requested_headers := request.headers.get("Access-Control-Request-Headers"):
            response.headers["Access-Control-Allow-Headers"] = requested_headers
        response.headers["Access-Control-Max-Age"] = "600"
    return response
