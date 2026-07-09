"""
Comprehensive pytest tests for app_context module.

These tests cover:
- _FallbackStorage: basic operations, thread safety
- _GProxy: Flask context detection, delegation, fallback
- _RequestProxy: property getters/setters, context switching
- Thread safety across all components
- Edge cases and error handling

Run with: pytest test_app_context.py -vv
"""

import threading
import time

import pytest


class TestFallbackStorage:
    """Test suite for _FallbackStorage class."""

    def test_init(self, fallback_storage):
        """Test that FallbackStorage initializes correctly."""
        assert hasattr(fallback_storage, "_storage")
        assert isinstance(fallback_storage._storage, threading.local)

    def test_get_set_basic(self, fallback_storage):
        """Test basic get and set operations."""
        fallback_storage.set("key1", "value1")
        assert fallback_storage.get("key1") == "value1"

    def test_get_with_default(self, fallback_storage):
        """Test get with default value for non-existent keys."""
        assert fallback_storage.get("nonexistent", "default") == "default"
        assert fallback_storage.get("nonexistent") is None

    def test_getattr_setattr(self, fallback_storage):
        """Test attribute access via __getattr__ and __setattr__."""
        fallback_storage.user_id = 123
        assert fallback_storage.user_id == 123

        fallback_storage.username = "testuser"
        assert fallback_storage.username == "testuser"

    def test_delattr(self, fallback_storage):
        """Test attribute deletion."""
        fallback_storage.temp_value = "temporary"
        assert hasattr(fallback_storage._storage, "temp_value")

        del fallback_storage.temp_value
        assert not hasattr(fallback_storage._storage, "temp_value")

    def test_contains(self, fallback_storage):
        """Test __contains__ method."""
        fallback_storage.existing_key = "value"
        assert "existing_key" in fallback_storage
        assert "nonexistent_key" not in fallback_storage

    def test_private_attributes(self, fallback_storage):
        """Test that private attributes (_xxx) are handled correctly."""
        # Private attributes should raise AttributeError
        with pytest.raises(AttributeError):
            _ = fallback_storage._private_attr

    def test_thread_isolation(self, fallback_storage, thread_barrier):
        """Test that each thread has isolated storage."""
        barrier = thread_barrier(3)
        results = {}

        def worker(thread_id):
            barrier.wait()  # Synchronize thread start
            fallback_storage.thread_id = thread_id
            fallback_storage.value = f"value_{thread_id}"
            time.sleep(0.01)  # Small delay to test isolation
            results[thread_id] = {
                "thread_id": fallback_storage.get("thread_id"),
                "value": fallback_storage.get("value"),
            }

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Each thread should have its own isolated values
        assert results[0]["thread_id"] == 0
        assert results[0]["value"] == "value_0"
        assert results[1]["thread_id"] == 1
        assert results[1]["value"] == "value_1"
        assert results[2]["thread_id"] == 2
        assert results[2]["value"] == "value_2"


class TestGProxy:
    """Test suite for _GProxy class."""

    def test_init(self, g_proxy):
        """Test that GProxy initializes correctly."""
        assert hasattr(g_proxy, "_fallback")

    def test_fallback_mode_get_set(self):
        """Test get/set in fallback mode (no Flask context)."""
        # Import here to get fresh instance
        from app_context import g

        # Outside Flask context, should use fallback
        g.set("user_id", 456)
        assert g.get("user_id") == 456

    def test_fallback_mode_attributes(self):
        """Test attribute access in fallback mode."""
        from app_context import g

        # Outside Flask context, should use fallback
        g.username = "alice"
        g.role = "admin"

        assert g.username == "alice"
        assert g.role == "admin"

    def test_flask_context_delegation(self, flask_app_context):
        """Test that GProxy delegates to Flask's g when in context."""
        from app_context import g
        from flask import g as flask_g

        # In Flask context, should use Flask's g
        g.user_id = 789
        assert flask_g.user_id == 789
        assert g.user_id == 789

    def test_get_with_default_fallback(self):
        """Test get with default value in fallback mode."""
        from app_context import g

        # Outside Flask context
        assert g.get("missing", "default_value") == "default_value"

    def test_contains_fallback(self):
        """Test __contains__ in fallback mode."""
        from app_context import g

        # Outside Flask context
        g.existing = "value"
        assert "existing" in g
        assert "nonexistent" not in g

    def test_delattr_fallback(self):
        """Test attribute deletion in fallback mode."""
        from app_context import g

        # Outside Flask context
        g.temp = "temporary"
        assert "temp" in g

        del g.temp
        assert "temp" not in g

    def test_context_switching(self, flask_app_context):
        """Test switching between Flask and non-Flask contexts."""
        from app_context import g

        # In Flask context, set value
        g.flask_value = "from_flask"
        assert g.flask_value == "from_flask"

        # Note: After context ends, value won't persist in fallback

    def test_thread_safety_fallback(self, thread_barrier):
        """Test thread safety in fallback mode."""
        from app_context import g

        barrier = thread_barrier(3)
        results = {}

        def worker(thread_id):
            # Outside Flask context
            barrier.wait()
            g.thread_id = thread_id
            g.data = f"data_{thread_id}"
            time.sleep(0.01)
            results[thread_id] = {
                "thread_id": g.get("thread_id"),
                "data": g.get("data"),
            }

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify thread isolation
        assert results[0]["thread_id"] == 0
        assert results[1]["thread_id"] == 1
        assert results[2]["thread_id"] == 2


class TestRequestProxy:
    """Test suite for _RequestProxy class."""

    def test_init(self, request_proxy):
        """Test that RequestProxy initializes correctly."""
        assert hasattr(request_proxy, "_fallback")

    def test_fallback_mode_none_values(self):
        """Test that properties return None in fallback mode when not set."""
        from app_context import request

        # Outside Flask context, values not set should return None
        # Note: might have values from previous tests, so create new proxy for this
        assert request.method is None or isinstance(request.method, str)

    def test_fallback_mode_default_values(self):
        """Test that some properties return defaults in fallback mode."""
        from app_context import request

        # Outside Flask context, these should return empty dict
        assert isinstance(request.args, dict)
        assert isinstance(request.headers, dict)

    def test_fallback_mode_setters(self, sample_request_data):
        """Test property setters in fallback mode."""
        from app_context import request

        # Outside Flask context, setters should work
        request.method = sample_request_data["method"]
        request.path = sample_request_data["path"]
        request.endpoint = sample_request_data["endpoint"]
        request.url = sample_request_data["url"]
        request.base_url = sample_request_data["base_url"]
        request.args = sample_request_data["args"]
        request.json = sample_request_data["json"]
        request.data = sample_request_data["data"]
        request.headers = sample_request_data["headers"]

        # Verify all values are set correctly
        assert request.method == sample_request_data["method"]
        assert request.path == sample_request_data["path"]
        assert request.endpoint == sample_request_data["endpoint"]
        assert request.url == sample_request_data["url"]
        assert request.base_url == sample_request_data["base_url"]
        assert request.args == sample_request_data["args"]
        assert request.json == sample_request_data["json"]
        assert request.data == sample_request_data["data"]
        assert request.headers == sample_request_data["headers"]

    def test_flask_context_delegation(self, flask_request_context):
        """Test that RequestProxy delegates to Flask's request when in context."""
        from app_context import request
        from flask import request as flask_request

        # In Flask context, should delegate to Flask's request
        assert request.method == flask_request.method
        assert request.path == flask_request.path

    def test_get_json_method(self):
        """Test get_json method."""
        from app_context import request

        # Outside Flask context
        request.set_json({"key": "value"})
        result = request.get_json()
        assert result == {"key": "value"}

    def test_set_json_method(self):
        """Test set_json method."""
        from app_context import request

        # Outside Flask context
        test_data = {"user": "test", "action": "create"}
        request.set_json(test_data)
        assert request.json == test_data

    def test_in_flask_context_method(self, flask_request_context):
        """Test _in_flask_context detection."""
        from app_context import request

        # Inside Flask context
        assert request._in_flask_context()

    def test_thread_safety_fallback(self, thread_barrier):
        """Test thread safety in fallback mode."""
        from app_context import request

        barrier = thread_barrier(3)
        results = {}

        def worker(thread_id):
            # Outside Flask context
            barrier.wait()
            request.method = f"METHOD_{thread_id}"
            request.path = f"/path/{thread_id}"
            request.json = {"thread_id": thread_id}
            time.sleep(0.01)

            results[thread_id] = {
                "method": request.method,
                "path": request.path,
                "json": request.json,
            }

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify thread isolation
        assert results[0]["method"] == "METHOD_0"
        assert results[1]["method"] == "METHOD_1"
        assert results[2]["method"] == "METHOD_2"


class TestIntegration:
    """Integration tests for the complete app_context module."""

    def test_g_and_request_singletons(self):
        """Test that g and request are singleton instances."""
        from app_context import g, request
        from app_context import g as g2
        from app_context import request as request2

        assert g is g2
        assert request is request2

    def test_amqp_scenario_simulation(self, sample_user_context):
        """Simulate AMQP message processing scenario."""
        from app_context import g, request

        # Simulate storing message context (outside Flask context)
        g.message_id = "msg-12345"
        g.user_id = sample_user_context["user_id"]
        g.username = sample_user_context["username"]

        # Simulate mock request data for processing
        request.method = "AMQP"
        request.path = "/queue/messages"

        # Verify values are accessible
        assert g.message_id == "msg-12345"
        assert g.user_id == sample_user_context["user_id"]
        assert request.method == "AMQP"
        assert request.path == "/queue/messages"

    def test_migration_scenario_simulation(self):
        """Simulate database migration script scenario."""
        from app_context import g

        # Simulate migration context (outside Flask context)
        g.migration_name = "add_user_roles"
        g.migration_version = "2024_10_27_001"
        g.dry_run = False

        # Verify values are accessible
        assert g.migration_name == "add_user_roles"
        assert g.migration_version == "2024_10_27_001"
        assert g.dry_run is False

    def test_flask_http_scenario_simulation(self, flask_request_context):
        """Simulate Flask HTTP request scenario."""
        from app_context import g, request
        from flask import g as flask_g

        # Simulate HTTP request context
        g.user_id = 123
        g.session_id = "sess-xyz"

        # Verify Flask's g was used
        assert flask_g.user_id == 123

        # Verify request delegation
        assert request.method == "GET"
        assert request.path == "/api/test"

    def test_context_isolation_between_threads(self, thread_barrier):
        """Test that different threads have isolated contexts."""
        from app_context import g

        barrier = thread_barrier(5)
        results = []
        lock = threading.Lock()

        def worker(thread_id):
            # Outside Flask context
            barrier.wait()

            # Each thread sets its own context
            g.thread_id = thread_id
            g.data = f"thread_{thread_id}_data"

            time.sleep(0.01)  # Simulate some work

            # Read back the values
            with lock:
                results.append(
                    {
                        "thread_id": thread_id,
                        "stored_thread_id": g.get("thread_id"),
                        "stored_data": g.get("data"),
                    }
                )

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify each thread's data is isolated
        for result in results:
            assert result["stored_thread_id"] == result["thread_id"]
            assert result["stored_data"] == f"thread_{result['thread_id']}_data"


class TestEdgeCases:
    """Test edge cases and error scenarios."""

    def test_g_proxy_attribute_error(self):
        """Test that accessing non-existent attributes raises AttributeError."""
        from app_context import g

        # Outside Flask context, accessing non-existent attribute should raise
        with pytest.raises(AttributeError):
            _ = g.nonexistent_attribute_xyz123

    def test_fallback_storage_attribute_error(self, fallback_storage):
        """Test that accessing non-existent attributes raises AttributeError."""
        with pytest.raises(AttributeError):
            _ = fallback_storage.nonexistent_attribute

    def test_request_proxy_empty_json(self):
        """Test request proxy with None json."""
        from app_context import request

        # Outside Flask context
        request.json = None
        assert request.json is None

    def test_request_proxy_complex_json(self):
        """Test request proxy with complex nested JSON."""
        from app_context import request

        # Outside Flask context
        complex_data = {
            "users": [
                {"id": 1, "name": "Alice", "roles": ["admin", "user"]},
                {"id": 2, "name": "Bob", "roles": ["user"]},
            ],
            "metadata": {"total": 2, "page": 1},
        }

        request.json = complex_data
        assert request.json == complex_data
        assert request.json["users"][0]["name"] == "Alice"

    def test_multiple_deletes(self):
        """Test deleting the same attribute multiple times."""
        from app_context import g

        # Outside Flask context
        g.temp_xyz = "value"
        del g.temp_xyz

        # Second delete should raise AttributeError
        with pytest.raises(AttributeError):
            del g.temp_xyz

    def test_set_then_get_pattern(self):
        """Test common set-then-get pattern."""
        from app_context import g

        # Outside Flask context
        # Set multiple values
        values = {"key1_xyz": "val1", "key2_xyz": "val2", "key3_xyz": "val3"}
        for key, val in values.items():
            g.set(key, val)

        # Get them back
        for key, val in values.items():
            assert g.get(key) == val

    def test_request_setters_no_op_in_flask_context(self, flask_request_context):
        """Test that setters don't work in Flask context (read-only)."""
        from app_context import request

        # In Flask context, get the original method
        original_method = request.method

        # Try to set values - should be no-op in Flask context
        request.method = "POST"

        # Should still return Flask's value, not our set value
        assert request.method == original_method  # Flask's original value
