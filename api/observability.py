"""Errors and logs to the OpenTelemetry collector (HyperDX), in place of Sentry.

An unexpected error is recorded on the request's span, so it is found next to
the trace it broke (and, through the trace, next to the browser session that
caused it); client errors are left out, as Sentry's filter did. Logs are
exported over OTLP when OpenTelemetry is on, each carrying its trace and span.
"""

import logging
from os import environ
from typing import Mapping

from elody.exceptions import NotFoundException
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from werkzeug.exceptions import HTTPException, NotFound

_NOT_REPORTED = (NotFoundException, NotFound)


def is_reportable(exception: BaseException) -> bool:
    """An unexpected error, not a client's: not a 4xx, not a missing entity."""
    if isinstance(exception, _NOT_REPORTED):
        return False
    if isinstance(exception, HTTPException):
        return (exception.code or 500) >= 500
    return True


def record_exception(exception: BaseException) -> None:
    """Records a reportable error on the current span, which then ends in error."""
    if not is_reportable(exception):
        return
    span = trace.get_current_span()
    if not span.is_recording():
        return
    span.record_exception(exception)
    span.set_status(Status(StatusCode.ERROR, f"{exception.__class__.__name__}: {exception}"))


def log_export_enabled(env: Mapping[str, str] = environ) -> bool:
    """Logs go out over OTLP when a collector is configured and OpenTelemetry is on."""
    return (
        bool(env.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
        and env.get("OTEL_SDK_DISABLED", "false").lower() != "true"
        and env.get("OTEL_LOGS_EXPORTER", "otlp").lower() != "none"
    )


def init_log_export(level: int = logging.INFO) -> bool:
    """Sends the root logger's records to the collector; returns whether it does."""
    if not log_export_enabled():
        return False
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    provider = LoggerProvider(resource=Resource.create())
    provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    set_logger_provider(provider)
    logging.getLogger().addHandler(LoggingHandler(level=level, logger_provider=provider))
    return True
