"""Mock RabbitMQ before queues.py is imported, so @get_rabbit().queue() decorators work."""

import sys
from unittest.mock import MagicMock, patch

# Mock get_rabbit() to return a mock whose .queue() is a pass-through decorator
_mock_rabbit = MagicMock()
_mock_rabbit.queue.side_effect = lambda **kwargs: lambda func: func

_patcher = patch("rabbit.get_rabbit", return_value=_mock_rabbit)
_patcher.start()

# Ensure queues module is freshly imported with mock in place
sys.modules.pop("resources.queues", None)
