import json
import logging
import logging.handlers
from multiprocessing import Queue

import logging_loki

from loki_logs.loki_logger import LokiLogger


class JsonLokiLogger(LokiLogger):
    def __init__(
            self,
            loki_url: str| None = None,
            default_loki_labels: dict | None = None,
            headers: dict | None = None,
            log_format: str | None = None,
            log_dateformat: str | None = None
    ):
        super().__init__(loki_url, default_loki_labels, headers, log_format, log_dateformat)

    def _log(self, level, msg: str, tags: dict | None = None, extra_json_properties: dict | None = None,
             exc_info=None):
        super_log_func = getattr(super(), level)
        dict_msg = {"message": msg}
        if extra_json_properties is not None:
            dict_msg.update(extra_json_properties)
        if level == 'exception':
            super_log_func(json.dumps(dict_msg), tags, exc_info=exc_info)
        else:
            super_log_func(json.dumps(dict_msg), tags)

    def debug(self, msg: str, tags: dict | None = None, extra_json_properties: dict | None = None):
        self._log('debug', msg, tags=tags, extra_json_properties=extra_json_properties)

    def info(self, msg: str, tags: dict | None = None, extra_json_properties: dict | None = None):
        self._log('info', msg, tags=tags, extra_json_properties=extra_json_properties)

    def warning(self, msg: str, tags: dict | None = None, extra_json_properties: dict | None = None):
        self._log('warning', msg, tags=tags, extra_json_properties=extra_json_properties)

    def error(self, msg: str, tags: dict | None = None, extra_json_properties: dict | None = None):
        self._log('error', msg, tags=tags, extra_json_properties=extra_json_properties)

    def critical(self, msg: str, tags: dict | None = None, extra_json_properties: dict | None = None):
        self._log('critical', msg, tags=tags, extra_json_properties=extra_json_properties)

    def exception(self, msg, tags: dict | None = None, extra_json_properties: dict | None = None, exc_info=None):
        self._log('exception', msg, tags=tags, extra_json_properties=extra_json_properties, exc_info=exc_info)
