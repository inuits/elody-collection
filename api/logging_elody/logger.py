import inspect

from configuration import get_object_configuration_mapper
from elody.util import flatten_dict
from logging_loki import JsonLokiLogger, LokiLogger
from os import getenv


class Logger:
    def __init__(self):
        logger = LokiLogger(
            loki_url=getenv("LOKI_URL", None),
            default_loki_labels={
                "service_name": getenv("NOMAD_GROUP_NAME", "nomad_group_name"),
                "env": getenv("NOMAD_JOB_NAME", "nomad_job_name-env").split("-")[-1],
                "service_type": getenv("SERVICE_TYPE", "api"),
                "category": getenv("SERVICE_TYPE_CATEGORY", "collection"),
            },
            headers={"X-Scope-OrgID": getenv("LOKI_TENANT_ID", "infra")},
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
        self, severity, message: str, item={}, *, frame_info, exc_info=None, **kwargs
    ):
        tags, extra_json_properties = None, None
        if not kwargs.get("only_log_message", False):
            if item is None:
                item = {}
            config = get_object_configuration_mapper().get(item.get("type", "_default"))
            info = config.logging(
                flatten_dict(
                    config.document_info().get("object_lists", {}),
                    item.get("storage_format", item),
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
            if info_labels := kwargs.get("info_labels"):
                extra_json_properties.update(info_labels)
            if not getenv("LOKI_URL", None):
                extra_json_properties.update(tags)

        log = getattr(self.logger, severity)
        if exc_info:
            log(message, tags, extra_json_properties, exc_info)
        else:
            log(message, tags, extra_json_properties)
