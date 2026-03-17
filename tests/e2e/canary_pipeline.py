"""
Synthetic canary pipeline — Phase 0, Test T0.6.

Simulates a 3-step batch pipeline (extract → transform → load) and emits
spans via raw OTLP to the local collector. Used to verify the full stack
is running and spans flow through to Tempo.

Usage:
    python tests/e2e/canary_pipeline.py              # emit and exit
    python tests/e2e/canary_pipeline.py --verify     # emit then verify spans in Tempo

Requirements:
    pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc requests
"""
import argparse
import time
import uuid
import sys
import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import StatusCode


COLLECTOR_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
TEMPO_HTTP_URL = os.getenv("TEMPO_HTTP_URL", "http://localhost:3200")


def build_tracer() -> trace.Tracer:
    resource = Resource.create({"service.name": "watch-canary-pipeline"})
    exporter = OTLPSpanExporter(endpoint=COLLECTOR_ENDPOINT, insecure=True)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer("watch.canary")


def run_canary(tracer: trace.Tracer) -> str:
    """
    Runs a synthetic 3-phase batch pipeline and returns the pipeline_run_id.
    Emits: 1 root span + 3 phase spans (extract, transform, load).
    """
    run_id = str(uuid.uuid4())
    pipeline_id = "canary-pipeline"
    pipeline_name = "Watch Canary Pipeline"
    environment = "dev"

    print(f"[canary] Starting run: pipeline_run_id={run_id}")

    # ── Root span ────────────────────────────────────────────────────────────
    with tracer.start_as_current_span("pipeline.run") as root_span:
        root_span.set_attribute("pipeline.run_id", run_id)
        root_span.set_attribute("pipeline.name", pipeline_name)
        root_span.set_attribute("pipeline.id", pipeline_id)
        root_span.set_attribute("pipeline.type", "batch")
        root_span.set_attribute("deployment.environment", environment)
        root_span.set_attribute("pipeline.status", "running")

        # ── Extract phase ────────────────────────────────────────────────────
        with tracer.start_as_current_span("etl.extract") as extract_span:
            extract_span.set_attribute("pipeline.run_id", run_id)
            extract_span.set_attribute("pipeline.name", pipeline_name)
            extract_span.set_attribute("pipeline.id", pipeline_id)
            extract_span.set_attribute("pipeline.type", "batch")
            extract_span.set_attribute("deployment.environment", environment)
            extract_span.set_attribute("pipeline.status", "running")
            extract_span.set_attribute("etl.phase", "extract")
            extract_span.set_attribute("extract.source.system", "postgres")
            extract_span.set_attribute("extract.query.type", "incremental")
            extract_span.set_attribute("extract.query.incremental_key", "updated_at")
            extract_span.set_attribute("extract.query.incremental_from", "2026-03-15T00:00:00Z")
            extract_span.set_attribute("extract.query.incremental_to", "2026-03-16T00:00:00Z")
            extract_span.set_attribute("extract.rows_read", 10000)
            extract_span.set_attribute("extract.bytes_read", 5242880)
            extract_span.set_attribute("extract.duration_ms", 1200)
            print("[canary]   extract phase complete: 10,000 rows read")
            time.sleep(0.1)

        # ── Transform phase ──────────────────────────────────────────────────
        with tracer.start_as_current_span("etl.transform") as transform_span:
            transform_span.set_attribute("pipeline.run_id", run_id)
            transform_span.set_attribute("pipeline.name", pipeline_name)
            transform_span.set_attribute("pipeline.id", pipeline_id)
            transform_span.set_attribute("pipeline.type", "batch")
            transform_span.set_attribute("deployment.environment", environment)
            transform_span.set_attribute("pipeline.status", "running")
            transform_span.set_attribute("etl.phase", "transform")
            transform_span.set_attribute("transform.engine", "python")
            transform_span.set_attribute("transform.rows_in", 10000)
            transform_span.set_attribute("transform.rows_out", 9800)
            transform_span.set_attribute("transform.rows_dropped", 200)
            transform_span.set_attribute("transform.rows_dropped_reason", "null_account_id")
            transform_span.set_attribute("transform.schema_drift_detected", False)
            transform_span.set_attribute("transform.duration_ms", 850)
            print("[canary]   transform phase complete: 9,800 rows out (200 dropped)")
            time.sleep(0.1)

        # ── Load phase ───────────────────────────────────────────────────────
        with tracer.start_as_current_span("etl.load") as load_span:
            load_span.set_attribute("pipeline.run_id", run_id)
            load_span.set_attribute("pipeline.name", pipeline_name)
            load_span.set_attribute("pipeline.id", pipeline_id)
            load_span.set_attribute("pipeline.type", "batch")
            load_span.set_attribute("deployment.environment", environment)
            load_span.set_attribute("pipeline.status", "running")
            load_span.set_attribute("etl.phase", "load")
            load_span.set_attribute("load.target.system", "snowflake")
            load_span.set_attribute("load.write_mode", "append")
            load_span.set_attribute("load.rows_written", 9800)
            load_span.set_attribute("load.bytes_written", 4816896)
            load_span.set_attribute("load.post_check.passed", True)
            load_span.set_attribute("load.duration_ms", 600)
            print("[canary]   load phase complete: 9,800 rows written to snowflake")
            time.sleep(0.1)

        # Mark root span as success
        root_span.set_attribute("pipeline.status", "success")
        root_span.set_status(StatusCode.OK)

    print(f"[canary] Run complete. pipeline_run_id={run_id}")

    # Flush spans before returning
    trace.get_tracer_provider().force_flush(timeout_millis=5000)  # type: ignore

    return run_id


def verify_spans_in_tempo(run_id: str, max_wait_seconds: int = 30) -> bool:
    """
    Polls Tempo until spans for the given run_id appear, or timeout.
    Returns True if spans found, False if timeout.
    """
    import requests

    url = f"{TEMPO_HTTP_URL}/api/search"
    params = {"tags": f"pipeline.run_id={run_id}", "limit": 10}

    print(f"[canary] Waiting for spans in Tempo (run_id={run_id}, timeout={max_wait_seconds}s)...")
    deadline = time.time() + max_wait_seconds

    while time.time() < deadline:
        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                body = resp.json()
                traces = body.get("traces", [])
                if traces:
                    print(f"[canary] ✓ Found {len(traces)} trace(s) in Tempo for run_id={run_id}")
                    return True
        except Exception as e:
            print(f"[canary]   Tempo not ready yet: {e}")
        time.sleep(2)

    print(f"[canary] ✗ No spans found in Tempo within {max_wait_seconds}s for run_id={run_id}")
    return False


def main():
    parser = argparse.ArgumentParser(description="Watch canary pipeline")
    parser.add_argument("--verify", action="store_true",
                        help="After emitting spans, verify they appear in Tempo")
    parser.add_argument("--max-wait", type=int, default=30,
                        help="Max seconds to wait for spans in Tempo (default: 30)")
    args = parser.parse_args()

    tracer = build_tracer()
    run_id = run_canary(tracer)

    if args.verify:
        found = verify_spans_in_tempo(run_id, max_wait_seconds=args.max_wait)
        sys.exit(0 if found else 1)


if __name__ == "__main__":
    main()
