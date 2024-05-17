import app
import inspect

from elody.util import flatten_dict
from logging_loki import JsonLokiLogger
from os import getenv


class Logger:
    def __init__(self):
        self._logger = JsonLokiLogger(app.logger)

    def debug(self, message: str, item={}, **kwargs):
        self._log(
            "debug",
            message,
            item,
            frame_info=inspect.getframeinfo(inspect.stack()[1][0]),
            **kwargs,
        )

    def info(self, message: str, item={}, **kwargs):
        self._log(
            "info",
            message,
            item,
            frame_info=inspect.getframeinfo(inspect.stack()[1][0]),
            **kwargs,
        )

    def warning(self, message: str, item={}, **kwargs):
        self._log(
            "warning",
            message,
            item,
            frame_info=inspect.getframeinfo(inspect.stack()[1][0]),
            **kwargs,
        )

    def error(self, message: str, item={}, **kwargs):
        self._log(
            "error",
            message,
            item,
            frame_info=inspect.getframeinfo(inspect.stack()[1][0]),
            **kwargs,
        )

    def critical(self, message: str, item={}, **kwargs):
        self._log(
            "critical",
            message,
            item,
            frame_info=inspect.getframeinfo(inspect.stack()[1][0]),
            **kwargs,
        )

    def exception(self, message: str, item={}, *, exc_info=None, **kwargs):
        self._log(
            "exception",
            message,
            item,
            frame_info=inspect.getframeinfo(inspect.stack()[1][0]),
            exc_info=exc_info,
            **kwargs,
        )

    def _log(
        self, severity, message: str, item={}, *, frame_info, exc_info=None, **kwargs
    ):
        if item is None:
            item = {}
        config = app.object_configuration_mapper.get(item.get("type", "_default"))
        info = config.logging(
            flatten_dict(
                config.document_info()["object_lists"], item.get("storage_format", item)
            ),
            **kwargs,
        )
        tags = info["loki_indexed_info_labels"]
        extra_json_properties = info["info_labels"]
        extra_json_properties.update(
            {
                "frame_info": f"Logged from file: {frame_info.filename}, line: {frame_info.lineno}, in function: {frame_info.function}"
            }
        )
        if not getenv("LOKI_URL", None):
            extra_json_properties.update(tags)

        log = getattr(self._logger, severity)
        if exc_info:
            log(message, tags, extra_json_properties, exc_info)
        else:
            log(message, tags, extra_json_properties)
