from contextlib import contextmanager
from importlib import import_module
from os import getenv

from logging_elody.log import log
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)


class DummyTracer:
    @contextmanager
    def start_as_current_span(self, *args, **kwargs):
        yield None


provider = TracerProvider()
processor = BatchSpanProcessor(OTLPSpanExporter())
provider.add_span_processor(processor)

# Sets the global default tracer provider
trace.set_tracer_provider(provider)


_tracer = DummyTracer()
_mongoInstrumentor = None


def init_tracer():
    global _tracer
    _tracer = trace.get_tracer("collection-api.tracer")
    return _tracer


def get_tracer():
    global _tracer
    return _tracer


def init_mongo_instrumentation():
    if bool(getenv("INSTRUMENT_MONGODB", False)):
        global _mongoInstrumentor
        try:
            instrumentation_library = import_module(
                "opentelemetry.instrumentation.pymongo"
            )
            if not _mongoInstrumentor:
                _mongoInstrumentor = instrumentation_library.PymongoInstrumentor()
                provider = trace.get_tracer_provider()
                _mongoInstrumentor.instrument(
                    tracer_provider=provider, capture_statement=True
                )
        except Exception as e:
            log.exception("Mongo instrumentation initialization failed", exc_info=e)
            raise e
