from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from os import getenv
from importlib import import_module


provider = TracerProvider()
processor = BatchSpanProcessor(OTLPSpanExporter())
provider.add_span_processor(processor)

# Sets the global default tracer provider
trace.set_tracer_provider(provider)


_tracer = None
_mongoInstrumentor = None


def init_tracer():
    global _tracer
    _tracer = trace.get_tracer("collection-api.tracer")
    return _tracer


def get_tracer() -> trace.Tracer:
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
            print("WE'RE NOT INSTRUMENTING ACTUALLY", "\n\n\n", flush=True)
            raise e
