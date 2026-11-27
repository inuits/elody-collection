import pytest
from flask import Flask, jsonify, request
from maintenance import block_writes


def build_client(maintenance_mode):
    app = Flask(__name__)

    @app.before_request
    def maintenance():
        return block_writes(request, maintenance_mode)

    @app.route(
        "/<path:path>", methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"]
    )
    def anything(path):
        return jsonify(path=path)

    return app.test_client()


WRITES = ["post", "put", "patch", "delete"]


class TestMaintenanceMode:
    @pytest.mark.parametrize("method", WRITES)
    def test_writes_are_refused(self, method):
        response = getattr(build_client(True), method)("/entities/abc", json={})

        assert response.status_code == 503
        assert response.headers["Retry-After"]
        assert "maintenance" in response.get_json()["message"]

    @pytest.mark.parametrize("method", ["get", "head", "options"])
    def test_reads_pass(self, method):
        response = getattr(build_client(True), method)("/entities/abc")

        assert response.status_code == 200

    @pytest.mark.parametrize(
        "path",
        [
            "/entities/filter",
            "/elody/v1/entities/filter",
            "/mediafiles/filter_deprecated",
            "/entities/filter/resolved",
            "/filter/matchers",
        ],
    )
    def test_filter_posts_are_reads(self, path):
        response = build_client(True).post(path, json=[])

        assert response.status_code == 200

    @pytest.mark.parametrize("method", WRITES)
    def test_writes_pass_when_off(self, method):
        response = getattr(build_client(False), method)("/entities/abc", json={})

        assert response.status_code == 200
