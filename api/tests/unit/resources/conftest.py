"""Mock StorageManager and auth before any resource imports so tests can run cleanly."""
import sys
import os
from unittest.mock import MagicMock, patch
from functools import wraps

# Ensure /app/api is on the path (pytest rootdir is /app)
_api_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../"))
if _api_path not in sys.path:
    sys.path.insert(0, _api_path)

# Mock StorageManager before resource imports
_mock_sm_instance = MagicMock()
_mock_sm_instance.get_db_engine.return_value = MagicMock()
patch("storage.storagemanager.StorageManager", return_value=_mock_sm_instance).start()

# Patch policy_factory so decorators are pass-through at definition AND runtime
def _passthrough_decorator(request_context):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper
    return decorator

patch("policy_factory.apply_policies", side_effect=_passthrough_decorator).start()
patch("policy_factory.authenticate", side_effect=_passthrough_decorator).start()
