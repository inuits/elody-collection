import app

from elody.util import flatten_dict
from logging_loki import JsonLokiLogger
from os import getenv


class Logger:
    def __init__(self):
        self._logger = JsonLokiLogger(app.logger)  # pyright: ignore

    def debug(self, message: str, item={}, **kwargs):
        self._log("debug", message, item, **kwargs)

    def info(self, message: str, item={}, **kwargs):
        self._log("info", message, item, **kwargs)

    def warning(self, message: str, item={}, **kwargs):
        self._log("warning", message, item, **kwargs)

    def error(self, message: str, item={}, **kwargs):
        self._log("error", message, item, **kwargs)

    def critical(self, message: str, item={}, **kwargs):
        self._log("critical", message, item, **kwargs)

    def exception(self, message: str, item={}, *, exc_info=None, **kwargs):
        self._log("exception", message, item, exc_info=exc_info, **kwargs)

    def _log(self, severity, message: str, item={}, *, exc_info=None, **kwargs):
        config = app.object_configuration_mapper.get(item.get("type", "none"))
        info = config.logging(
            flatten_dict(
                config.document_info()["object_lists"], item.get("storage_format", item)
            ),
            **kwargs
        )
        tags = info["loki_indexed_info_labels"]
        extra_json_properties = info["info_labels"]
        if int(getenv("LOKI_LOGGER", 0)) == 0:
            extra_json_properties.update(tags)

        log = getattr(self._logger, severity)
        if exc_info:
            log(message, tags, extra_json_properties, exc_info)
        else:
            log(message, tags, extra_json_properties)
