import threading
import pytest
from unittest.mock import patch

from filters_v2.matchers.base_matchers import BaseMatchers


@pytest.fixture
def mock_config_mapper():
    """
    Mock the configuration mapper function where it is used in basematchers.py
    """
    with patch(
        "filters_v2.matchers.base_matchers.get_object_configuration_mapper"
    ) as mock_get:
        # Setup the mock to return a safe dummy structure so calls don't crash
        mock_config_obj = mock_get.return_value.get.return_value

        # Mock .crud()["nested_matcher_builder"]
        mock_config_obj.crud.return_value = {"nested_matcher_builder": "mock_builder"}

        # Mock .document_info().get("object_lists")
        mock_config_obj.document_info.return_value = {"object_lists": {}}

        yield mock_get


def test_basematchers_thread_safety(mock_config_mapper):
    """
    Test that BaseMatchers context is isolated per thread.
    We use a Barrier to force two threads to be active inside the context manager
    at the exact same time, ensuring no data leaks between them.
    """
    barrier = threading.Barrier(2)

    results = {
        "thread_1_collection": None,
        "thread_1_type": None,
        "thread_2_collection": None,
        "thread_2_type": None,
    }

    def worker_1():
        # Enters context with "entities"
        with BaseMatchers.context(collection="entities", type_name="entity"):
            # Wait for Worker 2 to also enter their context (High Concurrency Simulation)
            barrier.wait()

            # Read values (should be entities/entity)
            results["thread_1_collection"] = BaseMatchers.collection
            results["thread_1_type"] = BaseMatchers.type

    def worker_2():
        # Enters context with "mediafiles"
        with BaseMatchers.context(collection="mediafiles", type_name="mediafile"):
            # Wait for Worker 1 to also enter their context
            barrier.wait()

            # Read values (should be mediafiles/mediafile)
            results["thread_2_collection"] = BaseMatchers.collection
            results["thread_2_type"] = BaseMatchers.type

    t1 = threading.Thread(target=worker_1)
    t2 = threading.Thread(target=worker_2)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # 5. Assertions
    # Verify Thread 1 saw ONLY its own data
    assert results["thread_1_collection"] == "entities"
    assert results["thread_1_type"] == "entity"

    # Verify Thread 2 saw ONLY its own data
    assert results["thread_2_collection"] == "mediafiles"
    assert results["thread_2_type"] == "mediafile"


def test_basematchers_defaults_outside_context(mock_config_mapper):
    """
    Test that outside of any context, we get the default values.
    """
    # Verify defaults defined in your ContextVars
    assert BaseMatchers.collection == "entities"
    assert BaseMatchers.type == ""
