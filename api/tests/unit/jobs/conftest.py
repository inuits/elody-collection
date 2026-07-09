# api/tests/unit/jobs/conftest.py
import sys
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask


@pytest.fixture
def mock_dependencies():
    """
    Mock external dependencies.
    """
    with patch("policy_factory.apply_policies") as mock_policy:
        mock_policy.side_effect = lambda *args, **kwargs: lambda func: func

        with patch("elody.job.init_job", autospec=True) as mock_init_job, patch(
            "elody.job.start_job", autospec=True
        ) as mock_start_job, patch(
            "elody.job.finish_job", autospec=True
        ) as mock_finish_job, patch(
            "elody.job.fail_job", autospec=True
        ) as mock_fail_job, patch(
            "elody.job.finish_job_with_warning", autospec=True
        ) as mock_warn_job, patch(
            "elody.job.add_document_to_job", autospec=True
        ) as mock_add_doc, patch(
            "rabbit.get_rabbit"
        ) as mock_rabbit:
            yield {
                "rabbit": mock_rabbit,
                "init_job": mock_init_job,
                "start_job": mock_start_job,
                "finish_job": mock_finish_job,
                "fail_job": mock_fail_job,
                "warn_job": mock_warn_job,
                "add_doc": mock_add_doc,
            }


@pytest.fixture(autouse=True)
def mock_storage_manager():
    with patch("resources.base_resource.StorageManager") as mock_sm:
        mock_instance = mock_sm.return_value
        mock_instance.get_db_engine.return_value = MagicMock()
        yield mock_sm


@pytest.fixture(autouse=True)
def mock_route_mapper():
    with patch("configuration.get_route_mapper") as mock_get_mapper:
        mock_mapper = mock_get_mapper.return_value
        # When mapper.get("ClassName", "/default/route") is called, return "/default/route"
        mock_mapper.get.side_effect = lambda name, route: route
        yield mock_get_mapper


@pytest.fixture
def client(mock_dependencies, mock_storage_manager, mock_route_mapper):
    """
    Setup the Flask test client using the REAL init_api function.
    This could potentially require more mocking, but tests pass currently.
    """
    sys.modules.pop("init_api", None)
    sys.modules.pop("resources.job", None)

    from init_api import init_api

    app = Flask(__name__)

    init_api(app)

    return app.test_client()
