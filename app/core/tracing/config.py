# Open telemetry metrics (Distributed Tracing)
from app.core import settings
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_tracer(name: str, version: str = ""):
    if settings.TRACING_ENABLED:
        resource = Resource.create(
            {
                "service.name": name,
                "service.version": version,
            }
        )
        tracer_provider = TracerProvider(resource=resource)
        cloud_trace_exporter = CloudTraceSpanExporter()
        tracer_provider.add_span_processor(
            # BatchSpanProcessor buffers spans and sends them in batches in a
            # background thread. The default parameters are sensible, but can be
            # tweaked to optimize your performance
            BatchSpanProcessor(cloud_trace_exporter)
        )
        trace.set_tracer_provider(tracer_provider)
    return trace.get_tracer(name, version)
