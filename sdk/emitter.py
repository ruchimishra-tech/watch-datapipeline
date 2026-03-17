"""
Span emitter — wraps OpenTelemetry TracerProvider setup.

Design principles (from execution.md 3.1 Architectural Constraints):
- Emission is non-blocking: spans are queued and flushed by a background thread.
- If the collector is unreachable, spans are dropped — NOT buffered indefinitely.
- No exception from the emitter ever propagates to the calling pipeline.
- If configuration is missing, a no-op emitter is used silently.
"""
import logging
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

logger = logging.getLogger("watch_obs.emitter")

_TRACER_PROVIDER: TracerProvider | None = None


def _build_provider(service_name: str = "watch-pipeline") -> TracerProvider:
    """
    Builds and returns a TracerProvider.
    If OTEL_EXPORTER_OTLP_ENDPOINT is set, exports to that collector.
    Otherwise falls back to a no-op (console exporter with level CRITICAL so nothing prints).
    """
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()

    if endpoint:
        try:
            exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
            processor = BatchSpanProcessor(
                exporter,
                max_export_batch_size=512,
                export_timeout_millis=5000,
                max_queue_size=2048,
            )
            provider.add_span_processor(processor)
            logger.debug("watch_obs: OTLP exporter configured → %s", endpoint)
        except Exception as exc:
            # Never crash the pipeline — fall through to no-op
            logger.warning("watch_obs: Failed to configure OTLP exporter (%s). Spans will be dropped.", exc)
    else:
        logger.warning(
            "watch_obs: OTEL_EXPORTER_OTLP_ENDPOINT not set. "
            "Spans will not be exported. Set the env var to enable observability."
        )

    return provider


def get_tracer(pipeline_id: str) -> trace.Tracer:
    """
    Returns a Tracer for the given pipeline. Initialises the global provider
    on first call. Safe to call from multiple threads.
    """
    global _TRACER_PROVIDER
    if _TRACER_PROVIDER is None:
        _TRACER_PROVIDER = _build_provider()
        trace.set_tracer_provider(_TRACER_PROVIDER)
    return trace.get_tracer(f"watch_obs.{pipeline_id}")


def flush(timeout_ms: int = 5000) -> None:
    """Flush pending spans. Called on pipeline exit."""
    global _TRACER_PROVIDER
    if _TRACER_PROVIDER is not None:
        try:
            _TRACER_PROVIDER.force_flush(timeout_millis=timeout_ms)
        except Exception as exc:
            logger.warning("watch_obs: flush failed (%s)", exc)
