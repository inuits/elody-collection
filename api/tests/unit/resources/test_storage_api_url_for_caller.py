import pytest
from flask import Flask

INTERNAL = "http://storage.elody-podiumnet-dev.svc.cluster.local:8000"
EXTERNAL = "https://podiumnet-dev.elody.eu/storage/v1"

app = Flask(__name__)


@pytest.fixture
def resource():
    from resources.base_resource import BaseResource

    resource = object.__new__(BaseResource)
    resource.storage_api_url = INTERNAL
    resource.storage_api_url_ext = EXTERNAL
    return resource


class TestStorageApiUrlForCaller:
    def test_in_cluster_service_gets_the_internal_url(self, resource):
        with app.test_request_context(
            "/batch", headers={"X-From-Service": "filesystem-importer-service"}
        ):
            assert resource.storage_api_url_for_caller == INTERNAL

    def test_everyone_else_gets_the_external_url(self, resource):
        with app.test_request_context("/batch"):
            assert resource.storage_api_url_for_caller == EXTERNAL

    def test_trailing_slash_is_stripped(self, resource):
        resource.storage_api_url_ext = f"{EXTERNAL}/"
        with app.test_request_context("/batch"):
            assert resource.storage_api_url_for_caller == EXTERNAL
