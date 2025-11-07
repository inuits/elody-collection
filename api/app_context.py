"""
This module provides wrappers around Flask context objects (g, request, etc.)
to allow the application to work in both HTTP contexts (Flask) and non-HTTP
contexts (AMQP message processing).

When Flask context is available, it delegates to Flask's objects.
When Flask context is not available, it provides fallback implementations.
"""

import threading
from typing import Any, Optional


class _FallbackStorage:
    """
    Thread-local storage fallback for when Flask context is not available.
    Mimics Flask's g object behavior.
    """

    def __init__(self):
        self._storage = threading.local()

    def get(self, name: str, default: Any = None) -> Any:
        """Get a value from storage, with optional default."""
        return getattr(self._storage, name, default)

    def set(self, name: str, value: Any) -> None:
        """Set a value in storage."""
        setattr(self._storage, name, value)

    def __getattr__(self, name: str) -> Any:
        """Get attribute from storage."""
        if name.startswith("_"):
            # Avoid recursion for private attributes
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        try:
            return getattr(self._storage, name)
        except AttributeError:
            raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name: str, value: Any) -> None:
        """Set attribute in storage."""
        if name.startswith("_"):
            # Private attributes go to the instance itself
            object.__setattr__(self, name, value)
        else:
            setattr(self._storage, name, value)

    def __delattr__(self, name: str) -> None:
        """Delete attribute from storage."""
        if name.startswith("_"):
            object.__delattr__(self, name)
        else:
            delattr(self._storage, name)

    def __contains__(self, name: str) -> bool:
        """Check if attribute exists in storage."""
        return hasattr(self._storage, name)


class _GProxy:
    """
    Proxy for Flask's g object that works in both Flask and non-Flask contexts.
    """

    def __init__(self):
        self._fallback = _FallbackStorage()

    def _get_target(self):
        """Get the appropriate storage target (Flask's g or fallback)."""
        try:
            from flask import g as flask_g

            # Try to access flask.g to see if we're in a Flask context
            _ = flask_g.__dict__
            return flask_g
        except (ImportError, RuntimeError, AttributeError):
            # Not in Flask context, use fallback
            return self._fallback

    def get(self, name: str, default: Any = None) -> Any:
        """Get a value, with optional default."""
        target = self._get_target()
        if hasattr(target, "get"):
            return target.get(name, default)
        else:
            return getattr(target, name, default)

    def set(self, name: str, value: Any) -> None:
        """Set a value."""
        target = self._get_target()
        if hasattr(target, "set"):
            target.set(name, value)
        else:
            setattr(target, name, value)

    def __getattr__(self, name: str) -> Any:
        """Get attribute from the appropriate storage."""
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        return getattr(self._get_target(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set attribute in the appropriate storage."""
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._get_target(), name, value)

    def __delattr__(self, name: str) -> None:
        """Delete attribute from the appropriate storage."""
        if name.startswith("_"):
            object.__delattr__(self, name)
        else:
            delattr(self._get_target(), name)

    def __contains__(self, name: str) -> bool:
        """Check if attribute exists."""
        return hasattr(self._get_target(), name)


class _RequestProxy:
    """
    Proxy for Flask's request object that works in both Flask and non-Flask contexts.

    In Flask context: delegates to Flask's request (read-only).
    Outside Flask context: allows setting values for testing/mocking.
    """

    def __init__(self):
        self._fallback = _FallbackStorage()

    def _in_flask_context(self) -> bool:
        """Check if we're in Flask request context."""
        try:
            from flask import request as flask_request

            # Try to access to verify we're in request context
            _ = flask_request.method
            return True
        except (ImportError, RuntimeError, AttributeError):
            return False

    @property
    def _flask_request(self):
        """Get Flask's request object if available."""
        if self._in_flask_context():
            from flask import request as flask_request

            return flask_request
        return None

    # Method property with getter and setter
    @property
    def method(self) -> Optional[str]:
        """Get request method."""
        req = self._flask_request
        if req:
            return req.method
        return self._fallback.get("method")

    @method.setter
    def method(self, value: str) -> None:
        """Set request method (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("method", value)

    # Path property with getter and setter
    @property
    def path(self) -> Optional[str]:
        """Get request path."""
        req = self._flask_request
        if req:
            return req.path
        return self._fallback.get("path")

    @path.setter
    def path(self, value: str) -> None:
        """Set request path (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("path", value)

    # Endpoint property with getter and setter
    @property
    def endpoint(self) -> Optional[str]:
        """Get request endpoint."""
        req = self._flask_request
        if req:
            return req.endpoint
        return self._fallback.get("endpoint")

    @endpoint.setter
    def endpoint(self, value: str) -> None:
        """Set request endpoint (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("endpoint", value)

    # URL property with getter and setter
    @property
    def url(self) -> Optional[str]:
        """Get request URL."""
        req = self._flask_request
        if req:
            return req.url
        return self._fallback.get("url")

    @url.setter
    def url(self, value: str) -> None:
        """Set request URL (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("url", value)

    # Base URL property with getter and setter
    @property
    def base_url(self) -> Optional[str]:
        """Get request base URL."""
        req = self._flask_request
        if req:
            return req.base_url
        return self._fallback.get("base_url")

    @base_url.setter
    def base_url(self, value: str) -> None:
        """Set request base URL (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("base_url", value)

    # Args property with getter and setter
    @property
    def args(self):
        """Get request query parameters."""
        req = self._flask_request
        if req:
            return req.args
        return self._fallback.get("args", {})

    @args.setter
    def args(self, value: dict) -> None:
        """Set request query parameters (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("args", value)

    # JSON property with getter and setter
    @property
    def json(self):
        """Get request JSON data."""
        req = self._flask_request
        if req:
            return req.json
        return self._fallback.get("json")

    @json.setter
    def json(self, value: Any) -> None:
        """Set request JSON data (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("json", value)

    # Data property with getter and setter
    @property
    def data(self):
        """Get request raw data."""
        req = self._flask_request
        if req:
            return req.data
        return self._fallback.get("data")

    @data.setter
    def data(self, value: Any) -> None:
        """Set request raw data (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("data", value)

    # Headers property with getter and setter
    @property
    def headers(self):
        """Get request headers."""
        req = self._flask_request
        if req:
            return req.headers
        return self._fallback.get("headers", {})

    @headers.setter
    def headers(self, value: dict) -> None:
        """Set request headers (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("headers", value)

    def get_json(self, *args, **kwargs):
        """Get request JSON data with options."""
        req = self._flask_request
        if req:
            return req.get_json(*args, **kwargs)
        return self._fallback.get("json")

    def set_json(self, value: Any) -> None:
        """Set request JSON data (only works outside Flask context)."""
        if not self._in_flask_context():
            self._fallback.set("json", value)


# Create singleton instances
g = _GProxy()
request = _RequestProxy()
