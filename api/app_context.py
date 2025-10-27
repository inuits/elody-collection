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
    
    def __getattr__(self, name: str) -> Any:
        """Get attribute from storage."""
        if name.startswith('_'):
            # Avoid recursion for private attributes
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        try:
            return getattr(self._storage, name)
        except AttributeError:
            raise AttributeError(f"No such attribute: {name}")
    
    def __setattr__(self, name: str, value: Any) -> None:
        """Set attribute in storage."""
        if name.startswith('_'):
            # Private attributes go to the instance itself
            object.__setattr__(self, name, value)
        else:
            setattr(self._storage, name, value)
    
    def __delattr__(self, name: str) -> None:
        """Delete attribute from storage."""
        if name.startswith('_'):
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
        if hasattr(target, 'get'):
            return target.get(name, default)
        else:
            return getattr(target, name, default)
    
    def __getattr__(self, name: str) -> Any:
        """Get attribute from the appropriate storage."""
        if name.startswith('_'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return getattr(self._get_target(), name)
    
    def __setattr__(self, name: str, value: Any) -> None:
        """Set attribute in the appropriate storage."""
        if name.startswith('_'):
            object.__setattr__(self, name, value)
        else:
            setattr(self._get_target(), name, value)
    
    def __delattr__(self, name: str) -> None:
        """Delete attribute from the appropriate storage."""
        if name.startswith('_'):
            object.__delattr__(self, name)
        else:
            delattr(self._get_target(), name)
    
    def __contains__(self, name: str) -> bool:
        """Check if attribute exists."""
        return hasattr(self._get_target(), name)


class _RequestProxy:
    """
    Proxy for Flask's request object that works in both Flask and non-Flask contexts.
    Returns None or sensible defaults when not in Flask context.
    """
    
    @property
    def _flask_request(self):
        """Get Flask's request object if available."""
        try:
            from flask import request as flask_request
            # Try to access to verify we're in request context
            _ = flask_request.method
            return flask_request
        except (ImportError, RuntimeError, AttributeError):
            return None
    
    @property
    def method(self) -> Optional[str]:
        """Get request method."""
        req = self._flask_request
        return req.method if req else None
    
    @property
    def path(self) -> Optional[str]:
        """Get request path."""
        req = self._flask_request
        return req.path if req else None
    
    @property
    def endpoint(self) -> Optional[str]:
        """Get request endpoint."""
        req = self._flask_request
        return req.endpoint if req else None
    
    @property
    def url(self) -> Optional[str]:
        """Get request URL."""
        req = self._flask_request
        return req.url if req else None
    
    @property
    def base_url(self) -> Optional[str]:
        """Get request base URL."""
        req = self._flask_request
        return req.base_url if req else None
    
    @property
    def args(self):
        """Get request query parameters."""
        req = self._flask_request
        return req.args if req else {}
    
    @property
    def json(self):
        """Get request JSON data."""
        req = self._flask_request
        return req.json if req else None
    
    @property
    def data(self):
        """Get request raw data."""
        req = self._flask_request
        return req.data if req else None
    
    @property
    def headers(self):
        """Get request headers."""
        req = self._flask_request
        return req.headers if req else {}
    
    def get_json(self, *args, **kwargs):
        """Get request JSON data with options."""
        req = self._flask_request
        return req.get_json(*args, **kwargs) if req else None


# Create singleton instances
g = _GProxy()
request = _RequestProxy()
