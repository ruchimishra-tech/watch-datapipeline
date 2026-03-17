"""
Phase 5 Unit Tests — T5.8, T5.9 (Airflow listener, offline)

Tests for the Airflow listener synthesizer (synthesizer/airflow_listener.py).
Run without Airflow installed — uses mock DAG run / task instance objects.

T5.8 — DAG run success emits root span with correct pipeline_run_id
T5.9 — Task instance failure emits task span with pipeline.status=failed
"""
from unittest.mock import MagicMock

import pytest

from synthesizer.airflow_listener import WatchAirflowListener, _derive_run_id


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_dag_run(dag_id: str = "orders-daily", run_id: str = "scheduled__2026-03-17"):
    dr = MagicMock()
    dr.dag_id = dag_id
    dr.run_id = run_id
    return dr


def _mock_task_instance(dag_id: str = "orders-daily", run_id: str = "scheduled__2026-03-17", task_id: str = "extract"):
    ti = MagicMock()
    ti.dag_id = dag_id
    ti.run_id = run_id
    ti.task_id = task_id
    return ti


def _make_listener():
    tracer = MagicMock()
    span = MagicMock()
    tracer.start_span.return_value = span
    listener = WatchAirflowListener(tracer=tracer)
    return listener, tracer, span


# ── _derive_run_id ────────────────────────────────────────────────────────────

@pytest.mark.phase5
class TestDeriveRunId:
    def test_same_inputs_produce_same_id(self):
        id1 = _derive_run_id("orders-daily", "run-123")
        id2 = _derive_run_id("orders-daily", "run-123")
        assert id1 == id2

    def test_different_run_ids_produce_different_watch_ids(self):
        id1 = _derive_run_id("orders-daily", "run-123")
        id2 = _derive_run_id("orders-daily", "run-456")
        assert id1 != id2

    def test_different_dag_ids_produce_different_watch_ids(self):
        id1 = _derive_run_id("orders-daily", "run-123")
        id2 = _derive_run_id("invoices-daily", "run-123")
        assert id1 != id2

    def test_id_is_32_chars(self):
        run_id = _derive_run_id("my-dag", "my-run")
        assert len(run_id) == 32


# ── T5.8: DAG run success ─────────────────────────────────────────────────────

@pytest.mark.phase5
class TestT58DagRunSuccess:
    """T5.8 — Successful DAG run emits pipeline.run span with correct attributes."""

    def test_dag_run_running_starts_span(self):
        listener, tracer, span = _make_listener()
        dr = _mock_dag_run()
        listener.on_dag_run_running(dag_run=dr, msg="")
        tracer.start_span.assert_called_once_with("pipeline.run")

    def test_dag_run_sets_pipeline_run_id(self):
        listener, tracer, span = _make_listener()
        dr = _mock_dag_run(dag_id="orders-daily", run_id="run-abc")
        listener.on_dag_run_running(dag_run=dr, msg="")
        attrs = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        expected = _derive_run_id("orders-daily", "run-abc")
        assert attrs.get("pipeline.run_id") == expected

    def test_dag_run_sets_dag_id(self):
        listener, tracer, span = _make_listener()
        dr = _mock_dag_run(dag_id="invoices-daily")
        listener.on_dag_run_running(dag_run=dr, msg="")
        attrs = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert attrs.get("airflow.dag_id") == "invoices-daily"

    def test_dag_run_success_ends_span_with_success_status(self):
        listener, tracer, span = _make_listener()
        dr = _mock_dag_run()
        listener.on_dag_run_running(dag_run=dr, msg="")
        listener.on_dag_run_success(dag_run=dr, msg="")
        attrs = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert attrs.get("pipeline.status") == "success"
        span.end.assert_called_once()

    def test_dag_run_success_without_prior_running_is_no_op(self):
        listener, tracer, span = _make_listener()
        dr = _mock_dag_run()
        listener.on_dag_run_success(dag_run=dr, msg="")
        span.end.assert_not_called()

    def test_retry_same_run_id_same_watch_id(self):
        """Two runs of the same Airflow run_id must map to the same watch run_id."""
        id1 = _derive_run_id("orders-daily", "scheduled__2026-03-17")
        id2 = _derive_run_id("orders-daily", "scheduled__2026-03-17")
        assert id1 == id2, "Retries must produce the same pipeline_run_id"


# ── T5.9: Task instance failure ───────────────────────────────────────────────

@pytest.mark.phase5
class TestT59TaskInstanceFailed:
    """T5.9 — Failed task instance emits span with pipeline.status=failed and task name."""

    def test_task_running_starts_span(self):
        listener, tracer, span = _make_listener()
        ti = _mock_task_instance()
        listener.on_task_instance_running(previous_state=None, task_instance=ti, session=None)
        tracer.start_span.assert_called_once()

    def test_task_span_name_includes_task_id(self):
        listener, tracer, span = _make_listener()
        ti = _mock_task_instance(task_id="extract_orders")
        listener.on_task_instance_running(previous_state=None, task_instance=ti, session=None)
        span_name = tracer.start_span.call_args.args[0]
        assert "extract_orders" in span_name

    def test_task_failed_sets_status_failed(self):
        listener, tracer, span = _make_listener()
        ti = _mock_task_instance()
        listener.on_task_instance_running(previous_state=None, task_instance=ti, session=None)
        listener.on_task_instance_failed(
            previous_state=None, task_instance=ti,
            error=Exception("Connection refused"), session=None
        )
        attrs = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert attrs.get("pipeline.status") == "failed"

    def test_task_failed_sets_failure_reason(self):
        listener, tracer, span = _make_listener()
        ti = _mock_task_instance()
        listener.on_task_instance_running(previous_state=None, task_instance=ti, session=None)
        listener.on_task_instance_failed(
            previous_state=None, task_instance=ti,
            error=Exception("Connection refused"), session=None
        )
        attrs = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert "Connection refused" in attrs.get("pipeline.failure_reason", "")

    def test_task_failed_ends_span(self):
        listener, tracer, span = _make_listener()
        ti = _mock_task_instance()
        listener.on_task_instance_running(previous_state=None, task_instance=ti, session=None)
        listener.on_task_instance_failed(
            previous_state=None, task_instance=ti, error=None, session=None
        )
        span.end.assert_called_once()

    def test_task_failed_carries_pipeline_run_id(self):
        listener, tracer, span = _make_listener()
        ti = _mock_task_instance(dag_id="orders-daily", run_id="run-abc")
        listener.on_task_instance_running(previous_state=None, task_instance=ti, session=None)
        attrs = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        expected_run_id = _derive_run_id("orders-daily", "run-abc")
        assert attrs.get("pipeline.run_id") == expected_run_id

    def test_listener_does_not_raise_on_error(self):
        """Listener failure must not crash the Airflow scheduler."""
        listener, tracer, span = _make_listener()
        tracer.start_span.side_effect = RuntimeError("OTel init failed")
        ti = _mock_task_instance()
        # Must not raise
        listener.on_task_instance_running(previous_state=None, task_instance=ti, session=None)
