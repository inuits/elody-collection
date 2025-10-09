from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)

provider = TracerProvider()
processor = BatchSpanProcessor(OTLPSpanExporter())
provider.add_span_processor(processor)

# Sets the global default tracer provider
trace.set_tracer_provider(provider)


_tracer = None



def init_tracer():
    global _tracer
    _tracer = trace.get_tracer("collection-api.tracer")
    return _tracer


def get_tracer() -> trace.Tracer:
    global _tracer
    return _tracer
