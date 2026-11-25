import pytest
from cors import add_cors_headers
from flask import Flask, jsonify, make_response, request


def build_client(allowed_origins):
    app = Flask(__name__)

    @app.after_request
    def cors(response):
        return add_cors_headers(request, response, allowed_origins)

    @app.after_request
    def replace_401(response):
        if response.status_code == 401:
            return make_response(jsonify(message="replaced"), 401)
        return response

    @app.route("/entities/filter", methods=["POST"])
    def entities_filter():
        return jsonify(results=[])

    @app.route("/unauthorized")
    def unauthorized():
        return jsonify(message="no token"), 401

    return app.test_client()


DOCS = "https://docs.example.com"


class TestAllowedOrigin:
    def test_response_echoes_the_origin(self):
        response = build_client([DOCS]).post(
            "/entities/filter", json=[], headers={"Origin": DOCS}
        )

        assert response.headers["Access-Control-Allow-Origin"] == DOCS
        assert "Origin" in response.headers["Vary"]

    def test_preflight_allows_the_requested_method_and_headers(self):
        response = build_client([DOCS]).options(
            "/entities/filter",
            headers={
                "Origin": DOCS,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization,content-type,x-tenant-id",
            },
        )

        assert response.status_code == 200
        assert response.headers["Access-Control-Allow-Origin"] == DOCS
        assert "POST" in response.headers["Access-Control-Allow-Methods"]
        assert (
            response.headers["Access-Control-Allow-Headers"]
            == "authorization,content-type,x-tenant-id"
        )

    def test_headers_survive_a_hook_that_replaces_the_response(self):
        response = build_client([DOCS]).get("/unauthorized", headers={"Origin": DOCS})

        assert response.status_code == 401
        assert response.headers["Access-Control-Allow-Origin"] == DOCS

    def test_wildcard_allows_any_origin(self):
        response = build_client(["*"]).post(
            "/entities/filter", json=[], headers={"Origin": "https://other.example"}
        )

        assert (
            response.headers["Access-Control-Allow-Origin"] == "https://other.example"
        )


class TestNoCors:
    @pytest.mark.parametrize(
        "allowed_origins, headers",
        [
            ([DOCS], {"Origin": "https://evil.example"}),
            ([DOCS], {}),
            ([], {"Origin": DOCS}),
        ],
    )
    def test_no_cors_headers(self, allowed_origins, headers):
        response = build_client(allowed_origins).options(
            "/entities/filter",
            headers={**headers, "Access-Control-Request-Method": "POST"},
        )

        assert "Access-Control-Allow-Origin" not in response.headers
        assert "Access-Control-Allow-Methods" not in response.headers
