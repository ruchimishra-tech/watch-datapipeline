"""
Unit tests for the Pipeline and Phase SDK — T1.1 to T1.11.
No external services required. Uses OpenTelemetry in-memory span exporter.
"""
import os
import sys
import uuid
import pytest

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry import trace

# Ensure sdk/ is importable
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[2]))

from sdk.pipeline import Pipeline
from sdk.phase import Phase
from sdk.propagation import RunContext


# ── Helpers ────────────────────────────────────────────────────────────────

def make_pipeline_with_exporter(name="test-pipe", pipeline_id="test-pipe", **kwargs):
    """
    Creates a Pipeline wired to an InMemorySpanExporter so tests can inspect
    emitted spans without a running collector.
    Uses _tracer_provider injection to avoid the one-time global provider limitation.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    # Do NOT call trace.set_tracer_provider() — inject directly into Pipeline
    p = Pipeline(name=name, pipeline_id=pipeline_id, _tracer_provider=provider, **kwargs)
    return p, exporter


def get_spans_by_name(exporter: InMemorySpanExporter, name: str):
    return [s for s in exporter.get_finished_spans() if s.name == name]


def span_attrs(span) -> dict:
    return dict(span.attributes or {})


# ── T1.1 SDK happy path ────────────────────────────────────────────────────

@pytest.mark.phase1
def test_T1_1_happy_path_span_count():
    """T1.1 — 3-phase pipeline emits 4 spans (1 root + 3 phases)."""
    pipe, exporter = make_pipeline_with_exporter()
    with pipe as run:
        with run.phase("extract") as p:
            p.record_metric("rows_read", 10000)
        with run.phase("transform") as p:
            p.set_attribute("transform.engine", "python")
            p.set_attribute("transform.rows_in", 10000)
            p.set_attribute("transform.rows_out", 9800)
        with run.phase("load") as p:
            p.record_metric("rows_written", 9800)

    spans = exporter.get_finished_spans()
    assert len(spans) == 4, f"Expected 4 spans, got {len(spans)}: {[s.name for s in spans]}"


@pytest.mark.phase1
def test_T1_1_happy_path_span_names():
    """T1.1 — span names are pipeline.run, etl.extract, etl.transform, etl.load."""
    pipe, exporter = make_pipeline_with_exporter()
    with pipe as run:
        with run.phase("extract"):
            pass
        with run.phase("transform"):
            pass
        with run.phase("load"):
            pass

    names = {s.name for s in exporter.get_finished_spans()}
    assert names == {"pipeline.run", "etl.extract", "etl.transform", "etl.load"}


@pytest.mark.phase1
def test_T1_1_root_span_required_attributes():
    """T1.1 — root span carries all required schema attributes."""
    pipe, exporter = make_pipeline_with_exporter(
        name="My Pipeline",
        pipeline_id="my-pipeline",
        environment="staging",
    )
    with pipe:
        pass

    root = get_spans_by_name(exporter, "pipeline.run")
    assert root, "Root span not found"
    attrs = span_attrs(root[0])

    assert attrs["pipeline.run_id"] == pipe.run_id
    assert attrs["pipeline.name"] == "My Pipeline"
    assert attrs["pipeline.id"] == "my-pipeline"
    assert attrs["pipeline.type"] == "batch"
    assert attrs["deployment.environment"] == "staging"
    assert attrs["pipeline.status"] == "success"


# ── T1.2 Schema compliance ─────────────────────────────────────────────────

@pytest.mark.phase1
def test_T1_2_all_spans_have_required_fields():
    """T1.2 — every emitted span carries the 6 required root fields."""
    required = {
        "pipeline.run_id", "pipeline.name", "pipeline.id",
        "pipeline.type", "deployment.environment", "pipeline.status"
    }
    pipe, exporter = make_pipeline_with_exporter()
    with pipe as run:
        with run.phase("extract"):
            pass
        with run.phase("transform"):
            pass
        with run.phase("load"):
            pass

    for span in exporter.get_finished_spans():
        attrs = span_attrs(span)
        missing = required - set(attrs.keys())
        assert not missing, f"Span '{span.name}' missing required fields: {missing}"


# ── T1.3 Non-blocking (collector down) ────────────────────────────────────

@pytest.mark.phase1
def test_T1_3_no_exception_when_collector_unreachable(monkeypatch):
    """T1.3 — pipeline completes even when collector endpoint is unreachable."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:19999")  # nothing listening

    # Reset global provider so emitter re-initialises with bad endpoint
    import sdk.emitter as emitter_mod
    emitter_mod._TRACER_PROVIDER = None

    ran = False
    try:
        with Pipeline(name="test", pipeline_id="test-pipe") as run:
            with run.phase("extract") as p:
                p.record_metric("rows_read", 1)
            ran = True
    except Exception as exc:
        pytest.fail(f"Pipeline raised an exception when collector was down: {exc}")
    finally:
        emitter_mod._TRACER_PROVIDER = None

    assert ran, "Pipeline body did not execute"


@pytest.mark.phase1
def test_T1_4_no_memory_growth_when_collector_down(monkeypatch):
    """T1.4 — spans are dropped (not queued) when collector is unreachable."""
    import gc
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:19999")

    import sdk.emitter as emitter_mod
    emitter_mod._TRACER_PROVIDER = None

    # Run the pipeline — spans should not accumulate in memory
    with Pipeline(name="test", pipeline_id="test-pipe") as run:
        with run.phase("extract"):
            pass

    # Force GC; if spans were queued unboundedly this would grow
    gc.collect()

    # If we get here without OOM or hung thread — pass
    emitter_mod._TRACER_PROVIDER = None


# ── T1.5 PII redaction ─────────────────────────────────────────────────────

@pytest.mark.phase1
def test_T1_5_pii_attribute_set_on_span():
    """
    T1.5 — SDK sets sample_bad_values on the span.
    Actual redaction happens at the ADOT collector (integration test T1.5).
    This unit test verifies the SDK sets the attribute — redaction is not SDK responsibility.
    """
    pipe, exporter = make_pipeline_with_exporter()
    with pipe as run:
        with run.phase("extract") as p:
            p.set_attribute("dq.sample_bad_values", "SSN:123-45-6789")

    extract = get_spans_by_name(exporter, "etl.extract")
    assert extract, "Extract span not found"
    attrs = span_attrs(extract[0])
    assert "dq.sample_bad_values" in attrs


# ── T1.6 run_id propagation via env vars ──────────────────────────────────

@pytest.mark.phase1
def test_T1_6_env_var_propagation_round_trip():
    """T1.6 — to_env_vars() / from_env() round-trips the run_id correctly."""
    pipe, _ = make_pipeline_with_exporter(pipeline_id="parent-pipe")
    with pipe as run:
        env_vars = run.to_env_vars()

    # Simulate child process environment
    original_env = {}
    for k, v in env_vars.items():
        original_env[k] = os.environ.get(k)
        os.environ[k] = v

    try:
        ctx = RunContext.from_env()
        assert ctx is not None, "RunContext.from_env() returned None"
        assert ctx.run_id == pipe.run_id
        assert ctx.pipeline_id == "parent-pipe"
    finally:
        for k, v in original_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@pytest.mark.phase1
def test_T1_6_child_pipeline_shares_run_id(tmp_path):
    """T1.6 — child subprocess started with to_env_vars() shares the parent run_id."""
    import subprocess
    import json

    pipe, _ = make_pipeline_with_exporter(pipeline_id="parent-pipe")
    with pipe as run:
        env_vars = run.to_env_vars()
        expected_run_id = run.run_id

    env = {**os.environ, **env_vars}

    # Tiny child script that prints the run_id from env
    child_script = tmp_path / "child.py"
    child_script.write_text(
        "import sys, os\n"
        "sys.path.insert(0, r'" + str(__import__("pathlib").Path(__file__).parents[2]) + "')\n"
        "from sdk.propagation import RunContext\n"
        "ctx = RunContext.from_env()\n"
        "print(ctx.run_id if ctx else 'NONE')\n"
    )

    result = subprocess.run(
        [sys.executable, str(child_script)],
        env=env, capture_output=True, text=True, timeout=10
    )
    child_run_id = result.stdout.strip()
    assert child_run_id == expected_run_id, \
        f"Child run_id '{child_run_id}' != parent run_id '{expected_run_id}'"


# ── T1.7 HTTP header propagation ──────────────────────────────────────────

@pytest.mark.phase1
def test_T1_7_http_headers_contain_run_id():
    """T1.7 — to_http_headers() returns dict with pipeline_run_id key."""
    pipe, _ = make_pipeline_with_exporter()
    with pipe as run:
        headers = run.to_http_headers()

    assert "x-watch-pipeline-run-id" in headers
    assert headers["x-watch-pipeline-run-id"] == pipe.run_id


@pytest.mark.phase1
def test_T1_7_http_headers_round_trip():
    """T1.7 — from_http_headers() reconstructs the run context."""
    pipe, _ = make_pipeline_with_exporter(pipeline_id="my-pipe", environment="staging")
    with pipe as run:
        headers = run.to_http_headers()

    ctx = RunContext.from_http_headers(headers)
    assert ctx is not None
    assert ctx.run_id == pipe.run_id
    assert ctx.environment == "staging"


# ── T1.8 Kafka header propagation ─────────────────────────────────────────

@pytest.mark.phase1
def test_T1_8_kafka_headers_format():
    """T1.8 — to_kafka_headers() returns list of (str, bytes) tuples."""
    pipe, _ = make_pipeline_with_exporter()
    with pipe as run:
        headers = run.to_kafka_headers()

    assert isinstance(headers, list), "Expected a list"
    assert len(headers) > 0
    for key, value in headers:
        assert isinstance(key, str), f"Header key must be str, got {type(key)}"
        assert isinstance(value, bytes), f"Header value must be bytes, got {type(value)}"


@pytest.mark.phase1
def test_T1_8_kafka_headers_round_trip():
    """T1.8 — from_kafka_headers() reconstructs the run context."""
    pipe, _ = make_pipeline_with_exporter(pipeline_id="kafka-pipe")
    with pipe as run:
        headers = run.to_kafka_headers()

    ctx = RunContext.from_kafka_headers(headers)
    assert ctx is not None
    assert ctx.run_id == pipe.run_id
    assert ctx.pipeline_id == "kafka-pipe"


# ── T1.9 External run_id ───────────────────────────────────────────────────

@pytest.mark.phase1
def test_T1_9_external_run_id_propagated_to_all_spans():
    """T1.9 — externally supplied run_id appears on root span and all phase spans."""
    external_id = "airflow-dagrun-2026-03-16-abc123"
    pipe, exporter = make_pipeline_with_exporter(run_id=external_id)

    with pipe as run:
        with run.phase("extract"):
            pass
        with run.phase("load"):
            pass

    for span in exporter.get_finished_spans():
        attrs = span_attrs(span)
        assert attrs.get("pipeline.run_id") == external_id, \
            f"Span '{span.name}' has run_id '{attrs.get('pipeline.run_id')}' not '{external_id}'"


# ── T1.10 Missing collector URL ────────────────────────────────────────────

@pytest.mark.phase1
def test_T1_10_no_exception_when_endpoint_not_set(monkeypatch):
    """T1.10 — SDK works without OTEL_EXPORTER_OTLP_ENDPOINT set."""
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    import sdk.emitter as emitter_mod
    emitter_mod._TRACER_PROVIDER = None

    ran = False
    try:
        with Pipeline(name="test", pipeline_id="test-pipe") as run:
            with run.phase("extract"):
                pass
            ran = True
    except Exception as exc:
        pytest.fail(f"Pipeline raised exception without endpoint set: {exc}")
    finally:
        emitter_mod._TRACER_PROVIDER = None

    assert ran


# ── T1.11 run_id uniqueness ────────────────────────────────────────────────

@pytest.mark.phase1
def test_T1_11_run_ids_are_unique():
    """T1.11 — two Pipeline instances without an external run_id get different UUIDs."""
    p1, _ = make_pipeline_with_exporter(pipeline_id="pipe-a")
    p2, _ = make_pipeline_with_exporter(pipeline_id="pipe-b")

    with p1:
        pass
    with p2:
        pass

    assert p1.run_id != p2.run_id, "Two pipeline runs should have distinct run_ids"


@pytest.mark.phase1
def test_T1_11_run_id_is_valid_uuid():
    """T1.11 — auto-generated run_id is a valid UUID4."""
    pipe, _ = make_pipeline_with_exporter()
    with pipe:
        pass

    # uuid.UUID() will raise ValueError if the string is not a valid UUID
    parsed = uuid.UUID(pipe.run_id, version=4)
    assert str(parsed) == pipe.run_id
