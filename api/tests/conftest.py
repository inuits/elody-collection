"""
Pytest configuration and fixtures for app_context tests.
"""

import pytest
import sys
import threading


@pytest.fixture
def flask_app():
    """
    Create a Flask app for testing Flask context scenarios.
    """
    try:
        from flask import Flask
        app = Flask(__name__)
        app.config['TESTING'] = True
        return app
    except ImportError:
        pytest.skip("Flask not installed")


@pytest.fixture
def flask_app_context(flask_app):
    """
    Provide Flask application context.
    """
    with flask_app.app_context():
        yield flask_app


@pytest.fixture
def flask_request_context(flask_app):
    """
    Provide Flask request context.
    """
    with flask_app.test_request_context(
        '/api/test',
        method='GET',
        json={"data": "test"},
        headers={"Content-Type": "application/json"}
    ):
        yield flask_app


@pytest.fixture
def thread_barrier():
    """
    Create a threading barrier for synchronizing multiple threads in tests.
    
    Usage:
        def test_thread_safety(thread_barrier):
            barrier = thread_barrier(3)  # For 3 threads
            
            def worker():
                barrier.wait()  # All threads start together
                # Do work
            
            threads = [threading.Thread(target=worker) for _ in range(3)]
            for t in threads: t.start()
            for t in threads: t.join()
    """
    def create_barrier(num_threads):
        return threading.Barrier(num_threads)
    
    return create_barrier


@pytest.fixture
def clean_app_context():
    """
    Ensure app_context module is in a clean state before each test.
    This fixture reimports the module to reset singleton instances.
    """
    # Remove app_context from sys.modules if it exists
    if 'app_context' in sys.modules:
        del sys.modules['app_context']
    
    yield
    
    # Clean up after test
    if 'app_context' in sys.modules:
        del sys.modules['app_context']


@pytest.fixture
def fallback_storage():
    """
    Create a fresh _FallbackStorage instance for testing.
    """
    from app_context import _FallbackStorage
    return _FallbackStorage()


@pytest.fixture
def g_proxy():
    """
    Create a fresh _GProxy instance for testing.
    """
    from app_context import _GProxy
    return _GProxy()


@pytest.fixture
def request_proxy():
    """
    Create a fresh _RequestProxy instance for testing.
    """
    from app_context import _RequestProxy
    return _RequestProxy()


@pytest.fixture
def sample_user_context():
    """
    Sample user context data for testing.
    """
    return {
        "user_id": 123,
        "username": "testuser",
        "email": "test@example.com",
        "roles": ["admin", "user"]
    }


@pytest.fixture
def sample_request_data():
    """
    Sample request data for testing.
    """
    return {
        "method": "POST",
        "path": "/api/users",
        "endpoint": "create_user",
        "url": "http://example.com/api/users",
        "base_url": "http://example.com",
        "args": {"filter": "active"},
        "json": {"name": "John Doe", "email": "john@example.com"},
        "data": b'{"name": "John Doe"}',
        "headers": {"Authorization": "Bearer token123"}
    }