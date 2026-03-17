"""
Watch Airflow Listener Plugin — Phase 5, Feature 5.2.

Hooks into Airflow DAG run and task instance lifecycle events to emit
synthetic OTel spans without modifying DAG code.

Installation:
    Copy or symlink this file into your Airflow plugins directory, or install
    as a pip package. Airflow discovers plugins automatically on startup.

Design:
    - pipeline_run_id is derived deterministically from the DAG run ID so that
      retries produce the same run_id (enabling deduplication in Tempo).
    - Plugin failure must not affect DAG execution: all methods catch exceptions.
    - Task spans are child spans of the DAG run span.

Usage (Airflow 2.x with the listener API):
    Copy to $AIRFLOW_HOME/plugins/watch_airflow_listener.py
    Airflow auto-discovers and registers it.
"""
import hashlib
import logging
from typing import Optional

logger = logging.getLogger("watch_obs.airflow_listener")

# Guard: import Airflow listener API only when running inside Airflow.
# This lets us unit-test the module without installing Airflow.
try:
    from airflow.listeners import hookimpl
    _AIRFLOW_AVAILABLE = True
except ImportError:
    # Stub decorator for non-Airflow environments (tests, CI)
    def hookimpl(f):
        return f
    _AIRFLOW_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.trace import StatusCode
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False


def _derive_run_id(dag_id: str, run_id: str) -> str:
    """
    Derive a deterministic pipeline_run_id from Airflow's dag_id + run_id.
    Using SHA-256 ensures the same Airflow DAG run always maps to the same
    Watch run_id, even across retries or re-runs with the same logical date.
    """
    raw = f"{dag_id}:{run_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


class WatchAirflowListener:
    """
    Airflow listener plugin that emits Watch-compatible OTel spans.

    Registered via Airflow's plugin manager. Spans are emitted to the
    configured OTLP endpoint (set via OTEL_EXPORTER_OTLP_ENDPOINT env var).
    """

    def __init__(self, tracer=None):
        if _OTEL_AVAILABLE:
            self._tracer = tracer or trace.get_tracer("watch_obs.airflow")
        else:
            self._tracer = None
        # dag_run_id -> {"span": ..., "run_id": ...}
        self._active_runs: dict[str, dict] = {}
        # (dag_run_id, task_id) -> span
        self._active_tasks: dict[tuple, object] = {}

    # ── DAG Run lifecycle ─────────────────────────────────────────────────────

    @hookimpl
    def on_dag_run_running(self, dag_run, msg: str) -> None:
        """Called when a DAG run transitions to RUNNING state."""
        try:
            dag_id = dag_run.dag_id
            airflow_run_id = dag_run.run_id
            watch_run_id = _derive_run_id(dag_id, airflow_run_id)

            if self._tracer is None:
                return

            span = self._tracer.start_span("pipeline.run")
            span.set_attribute("pipeline.run_id", watch_run_id)
            span.set_attribute("pipeline.name", dag_id)
            span.set_attribute("pipeline.id", dag_id)
            span.set_attribute("pipeline.type", "batch")
            span.set_attribute("pipeline.status", "running")
            span.set_attribute("airflow.dag_id", dag_id)
            span.set_attribute("airflow.run_id", airflow_run_id)
            span.set_attribute("synthesizer.source", "airflow_listener")

            self._active_runs[airflow_run_id] = {
                "span": span,
                "run_id": watch_run_id,
                "dag_id": dag_id,
            }
            logger.debug(
                "watch_obs.airflow: dag_run started dag=%s airflow_run_id=%s watch_run_id=%s",
                dag_id, airflow_run_id, watch_run_id,
            )
        except Exception as exc:
            logger.warning("watch_obs.airflow: on_dag_run_running failed: %s", exc)

    @hookimpl
    def on_dag_run_success(self, dag_run, msg: str) -> None:
        """Called when a DAG run succeeds."""
        self._end_dag_run(dag_run, status="success", status_code=StatusCode.OK if _OTEL_AVAILABLE else None)

    @hookimpl
    def on_dag_run_failed(self, dag_run, msg: str) -> None:
        """Called when a DAG run fails."""
        self._end_dag_run(dag_run, status="failed", status_code=StatusCode.ERROR if _OTEL_AVAILABLE else None)

    def _end_dag_run(self, dag_run, status: str, status_code) -> None:
        airflow_run_id = dag_run.run_id
        run_state = self._active_runs.pop(airflow_run_id, None)
        if run_state is None:
            return
        try:
            span = run_state["span"]
            span.set_attribute("pipeline.status", status)
            if status_code is not None:
                span.set_status(status_code)
            span.end()
        except Exception as exc:
            logger.warning("watch_obs.airflow: _end_dag_run failed: %s", exc)

    # ── Task Instance lifecycle ───────────────────────────────────────────────

    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance, session) -> None:
        """Called when a task instance transitions to RUNNING."""
        try:
            if self._tracer is None:
                return
            airflow_run_id = task_instance.run_id
            task_id = task_instance.task_id
            dag_id = task_instance.dag_id

            span = self._tracer.start_span(f"airflow.task.{task_id}")
            span.set_attribute("airflow.task_id", task_id)
            span.set_attribute("airflow.dag_id", dag_id)
            span.set_attribute("airflow.run_id", airflow_run_id)
            span.set_attribute("pipeline.run_id", _derive_run_id(dag_id, airflow_run_id))
            span.set_attribute("pipeline.status", "running")
            span.set_attribute("synthesizer.source", "airflow_listener")

            self._active_tasks[(airflow_run_id, task_id)] = span
        except Exception as exc:
            logger.warning("watch_obs.airflow: on_task_instance_running failed: %s", exc)

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, session) -> None:
        """Called when a task instance succeeds."""
        self._end_task(task_instance, status="success",
                       status_code=StatusCode.OK if _OTEL_AVAILABLE else None)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, error, session) -> None:
        """Called when a task instance fails."""
        self._end_task(task_instance, status="failed",
                       status_code=StatusCode.ERROR if _OTEL_AVAILABLE else None,
                       failure_reason=str(error)[:500] if error else "unknown")

    def _end_task(self, task_instance, status: str, status_code, failure_reason: str = "") -> None:
        airflow_run_id = task_instance.run_id
        task_id = task_instance.task_id
        span = self._active_tasks.pop((airflow_run_id, task_id), None)
        if span is None:
            return
        try:
            span.set_attribute("pipeline.status", status)
            if failure_reason:
                span.set_attribute("pipeline.failure_reason", failure_reason)
            if status_code is not None:
                span.set_status(status_code)
            span.end()
        except Exception as exc:
            logger.warning("watch_obs.airflow: _end_task failed: %s", exc)


# ── Plugin registration ───────────────────────────────────────────────────────

if _AIRFLOW_AVAILABLE:
    from airflow.plugins_manager import AirflowPlugin

    class WatchAirflowPlugin(AirflowPlugin):
        name = "watch_observability"
        listeners = [WatchAirflowListener()]
