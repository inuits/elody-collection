import inspect
import logging
from config_access import config

from configuration import get_object_configuration_mapper
from elody.util import flatten_dict
from logging_loki import JsonLokiLogger, LokiLogger


class Logger:
    def __init__(self):
        log_level = config["LOG_LEVEL"]

        match log_level.lower():
            case "info":
                log_level = logging.INFO
            case "debug":
                log_level = logging.DEBUG
            case "warning":
                log_level = logging.DEBUG
            case "error":
                log_level = logging.ERROR
            case _:
                log_level = logging.INFO
        logger = LokiLogger(
            loki_url=config["LOKI_URL"] or None,
            default_loki_labels={
                "service_name": config["NOMAD_GROUP_NAME"],
                "env": config["NOMAD_JOB_NAME"],
                "service_type": config["SERVICE_TYPE"],
                "category": config["SERVICE_TYPE_CATEGORY"],
            },
            headers={"X-Scope-OrgID": config["LOKI_TENANT_ID"]},
            log_level=log_level,
        )
        self.logger = JsonLokiLogger(logger)

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
        self,
        severity,
        message: str,
        item={},
        *,
        frame_info,
        exc_info=None,
        **kwargs,
    ):
        tags, extra_json_properties = None, None
        if not kwargs.get("only_log_message", False):
            if item is None:
                item = {}
            object_config = get_object_configuration_mapper().get(
                item.get("type", "_default")
            )
            info = object_config.logging(
                flatten_dict(
                    object_config.document_info().get("object_lists", {}),
                    item,
                ),
                **kwargs,
            )
            tags = info["loki_indexed_info_labels"]
            extra_json_properties = info["info_labels"]
            extra_json_properties.update(
                {
                    "frame_info": f"Logged from file: {frame_info.filename}, line: {frame_info.lineno}, in function: {frame_info.function}",
                },
            )
            if info_labels := kwargs.get("info_labels"):
                extra_json_properties.update(info_labels)
            if not config["LOKI_URL"]:
                extra_json_properties.update(tags)

        log = getattr(self.logger, severity)
        if exc_info:
            log(message, tags, extra_json_properties, exc_info)
        else:
            log(message, tags, extra_json_properties)
