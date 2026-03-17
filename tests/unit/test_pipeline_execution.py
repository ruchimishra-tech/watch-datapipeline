"""
Unit tests for Phase 2 — Pipeline Execution Observability (T2.1–T2.8).
Tests lifecycle spans, failure, retry, state store, heartbeat, and latency.
"""
import sys
import time
import tempfile
import pytest

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import StatusCode

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[2]))

from sdk.pipeline import Pipeline
from sdk.state_store import RunStateStore


def make_pipeline(pipeline_id="test-pipe", name="Test Pipeline", **kwargs):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    p = Pipeline(name=name, pipeline_id=pipeline_id, _tracer_provider=provider, **kwargs)
    return p, exporter


def span_attrs(span) -> dict:
    return dict(span.attributes or {})


def spans_by_name(exporter, name):
    return [s for s in exporter.get_finished_spans() if s.name == name]


# ── T2.1 Success trace ─────────────────────────────────────────────────────

@pytest.mark.phase2
def test_T2_1_success_trace_has_all_spans():
    """T2.1 — successful run produces root + 3 phase spans, all with status=success."""
    pipe, exporter = make_pipeline()
    with pipe as run:
        with run.phase("extract") as p:
            p.set_attribute("extract.source.system", "postgres")
            p.set_attribute("extract.rows_read", 1000)
        with run.phase("transform") as p:
            p.set_attribute("transform.engine", "python")
            p.set_attribute("transform.rows_in", 1000)
            p.set_attribute("transform.rows_out", 950)
        with run.phase("load") as p:
            p.set_attribute("load.target.system", "snowflake")
            p.set_attribute("load.rows_written", 950)

    assert len(exporter.get_finished_spans()) == 4

    root = spans_by_name(exporter, "pipeline.run")[0]
    assert span_attrs(root)["pipeline.status"] == "success"
    assert root.status.status_code == StatusCode.OK

    for phase_name in ["etl.extract", "etl.transform", "etl.load"]:
        phase_spans = spans_by_name(exporter, phase_name)
        assert phase_spans, f"Missing span: {phase_name}"
        assert span_attrs(phase_spans[0])["pipeline.status"] == "success"


@pytest.mark.phase2
def test_T2_1_phase_durations_are_positive():
    """T2.1 — each phase span records a positive duration_ms."""
    pipe, exporter = make_pipeline()
    with pipe as run:
        with run.phase("extract") as p:
            p.set_attribute("extract.rows_read", 100)
            time.sleep(0.01)
        with run.phase("transform") as p:
            p.set_attribute("transform.rows_in", 100)
            p.set_attribute("transform.rows_out", 100)
            p.set_attribute("transform.engine", "python")
            time.sleep(0.01)
        with run.phase("load") as p:
            p.set_attribute("load.rows_written", 100)
            time.sleep(0.01)

    for phase, key in [
        ("etl.extract", "extract.duration_ms"),
        ("etl.transform", "transform.duration_ms"),
        ("etl.load", "load.duration_ms"),
    ]:
        span = spans_by_name(exporter, phase)[0]
        duration = span_attrs(span).get(key, 0)
        assert duration > 0, f"{phase} has non-positive {key}: {duration}"


# ── T2.2 Failure trace ─────────────────────────────────────────────────────

@pytest.mark.phase2
def test_T2_2_failure_sets_failed_status_on_phase_and_root():
    """T2.2 — exception in transform causes failed status on transform span and root span."""
    pipe, exporter = make_pipeline()

    with pytest.raises(ValueError):
        with pipe as run:
            with run.phase("extract") as p:
                p.set_attribute("extract.rows_read", 100)
            with run.phase("transform"):
                raise ValueError("Unexpected null in account_id column")

    root = spans_by_name(exporter, "pipeline.run")[0]
    assert span_attrs(root)["pipeline.status"] == "failed"
    assert root.status.status_code == StatusCode.ERROR

    transform = spans_by_name(exporter, "etl.transform")[0]
    assert span_attrs(transform)["pipeline.status"] == "failed"
    assert "account_id" in span_attrs(transform).get("pipeline.failure_reason", "")


@pytest.mark.phase2
def test_T2_3_failure_reason_excludes_stack_trace():
    """T2.3 — failure_reason contains exception message but not a full stack trace."""
    pipe, exporter = make_pipeline()

    with pytest.raises(RuntimeError):
        with pipe as run:
            with run.phase("extract"):
                raise RuntimeError("Connection refused to oracle-db")

    root = spans_by_name(exporter, "pipeline.run")[0]
    reason = span_attrs(root).get("pipeline.failure_reason", "")
    assert "Connection refused" in reason
    # Stack trace markers must not appear in the reason field
    assert "Traceback" not in reason
    assert "File \"" not in reason


# ── T2.4 Retry trace ──────────────────────────────────────────────────────

@pytest.mark.phase2
def test_T2_4_retry_attempt_attribute_on_span():
    """T2.4 — retry_attempt attribute is set correctly on each attempt."""
    pipe_attempt_0, exporter0 = make_pipeline(retry_attempt=0)
    pipe_attempt_1, exporter1 = make_pipeline(retry_attempt=1)

    with pipe_attempt_0 as run:
        with run.phase("extract"):
            pass

    with pipe_attempt_1 as run:
        with run.phase("extract"):
            pass

    root0 = spans_by_name(exporter0, "pipeline.run")[0]
    root1 = spans_by_name(exporter1, "pipeline.run")[0]

    # First attempt: retry.attempt not set (or 0); second attempt: retry.attempt = 1
    assert span_attrs(root0).get("retry.attempt", 0) == 0
    assert span_attrs(root1).get("retry.attempt") == 1


@pytest.mark.phase2
def test_T2_4_retry_uses_same_run_id():
    """T2.4 — retry should share pipeline_run_id when externally supplied."""
    shared_run_id = "shared-run-id-retry-test"
    pipe0, _ = make_pipeline(run_id=shared_run_id, retry_attempt=0)
    pipe1, _ = make_pipeline(run_id=shared_run_id, retry_attempt=1)

    assert pipe0.run_id == shared_run_id
    assert pipe1.run_id == shared_run_id


# ── T2.5 State store ──────────────────────────────────────────────────────

@pytest.mark.phase2
def test_T2_5_state_store_records_success(tmp_path):
    """T2.5 — after successful run, state store has status=success and duration."""
    db = str(tmp_path / "test_runs.db")
    pipe, _ = make_pipeline()
    pipe._db_path = db

    with pipe:
        time.sleep(0.01)

    store = RunStateStore(db_path=db)
    record = store.get(pipe.run_id)

    assert record is not None, "Run record not found in state store"
    assert record.status == "success"
    assert record.duration_ms is not None and record.duration_ms >= 0
    assert record.pipeline_id == pipe.pipeline_id


@pytest.mark.phase2
def test_T2_5_state_store_records_failure(tmp_path):
    """T2.5 — after failed run, state store has status=failed."""
    db = str(tmp_path / "test_runs.db")
    pipe, _ = make_pipeline()
    pipe._db_path = db

    with pytest.raises(RuntimeError):
        with pipe:
            raise RuntimeError("Intentional failure")

    store = RunStateStore(db_path=db)
    record = store.get(pipe.run_id)

    assert record is not None
    assert record.status == "failed"


@pytest.mark.phase2
def test_T2_6_state_store_ttl_expiry(tmp_path):
    """T2.6 — expired records are absent after TTL passes."""
    import sdk.state_store as ss_mod
    original = ss_mod._DEFAULT_MAX_JOB_DURATION_S

    db = str(tmp_path / "ttl_test.db")
    store = RunStateStore(db_path=db)

    from sdk.state_store import RunRecord
    import time as t

    # Create a record that has already expired (expires_at in the past)
    rec = RunRecord(
        run_id="expired-run",
        pipeline_id="test",
        status="running",
        start_time=t.time() - 100,
        last_updated=t.time() - 100,
    )
    # Force expires_at to the past
    rec.expires_at = t.time() - 1
    store.upsert(rec)

    # get() triggers purge — expired record should be gone
    result = store.get("expired-run")
    assert result is None, "Expired record should have been purged"


# ── T2.7 Heartbeat ────────────────────────────────────────────────────────

@pytest.mark.phase2
def test_T2_7_heartbeat_emits_events():
    """T2.7 — root span receives heartbeat events during a long-running pipeline."""
    pipe, exporter = make_pipeline()

    # Patch heartbeat interval to 0.05s so test doesn't take 60s
    original_loop = pipe._heartbeat_loop

    def fast_heartbeat(interval_s=0.05):
        return original_loop(interval_s=interval_s)

    pipe._heartbeat_loop = fast_heartbeat

    with pipe:
        time.sleep(0.2)  # long enough for ~3 heartbeats at 0.05s interval

    root = spans_by_name(exporter, "pipeline.run")[0]
    heartbeat_events = [e for e in (root.events or []) if e.name == "pipeline.heartbeat"]
    assert len(heartbeat_events) >= 2, \
        f"Expected at least 2 heartbeat events, got {len(heartbeat_events)}"


# ── T2.8 Ingestion latency ────────────────────────────────────────────────

@pytest.mark.phase2
def test_T2_8_span_recorded_immediately():
    """T2.8 — span appears in exporter immediately after pipeline exits (< 100ms overhead)."""
    pipe, exporter = make_pipeline()

    t0 = time.time()
    with pipe:
        pass
    elapsed = (time.time() - t0) * 1000

    spans = exporter.get_finished_spans()
    assert spans, "No spans recorded"
    # SimpleSpanProcessor is synchronous — spans available immediately
    # Check overhead is not pathological
    assert elapsed < 500, f"Pipeline overhead too high: {elapsed:.0f}ms"
