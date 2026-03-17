"""
Pipeline context manager — top-level entry point for SDK users.

Design:
- Generates or accepts pipeline_run_id
- Emits root span (pipeline.run)
- Manages phase context managers
- Never raises exceptions to the calling pipeline
"""
import logging
import os
import threading
import time
import uuid
from typing import Any, Optional

from opentelemetry import trace
from opentelemetry.trace import Span, StatusCode

from sdk.emitter import get_tracer, flush
from sdk.phase import Phase
from sdk.propagation import RunContext
from sdk.state_store import RunRecord, get_store

logger = logging.getLogger("watch_obs.pipeline")


class Pipeline:
    """
    Context manager representing a complete pipeline run.

    Usage:
        # Basic usage — SDK generates run_id
        with Pipeline(name="customer-360", pipeline_id="customer-360") as run:
            with run.phase("extract") as phase:
                phase.record_metric("rows_read", 50000)

        # Child process — accepts parent's run_id from env
        with Pipeline.from_env(name="customer-360-child", pipeline_id="customer-360") as run:
            ...

        # External run_id (e.g. from Airflow DAG run)
        with Pipeline(name="my-pipe", pipeline_id="my-pipe", run_id="airflow-run-123") as run:
            ...
    """

    def __init__(
        self,
        name: str,
        pipeline_id: str,
        run_id: Optional[str] = None,
        environment: Optional[str] = None,
        retry_attempt: int = 0,
        _tracer_provider=None,  # injectable for testing — do not use in production
    ):
        self.name = name
        self.pipeline_id = pipeline_id
        self.run_id = run_id or str(uuid.uuid4())
        self.environment = environment or os.getenv("WATCH_DEPLOYMENT_ENVIRONMENT", "dev")
        self.retry_attempt = retry_attempt
        self._tracer_provider = _tracer_provider  # None = use global

        self._tracer: Optional[trace.Tracer] = None
        self._root_span: Optional[Span] = None
        self._start_time: Optional[float] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._db_path: Optional[str] = None   # override for tests
        self._context = RunContext(
            run_id=self.run_id,
            pipeline_id=self.pipeline_id,
            pipeline_name=self.name,
            environment=self.environment,
        )

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "Pipeline":
        self._start_time = time.time()
        try:
            if self._tracer_provider is not None:
                self._tracer = self._tracer_provider.get_tracer(f"watch_obs.{self.pipeline_id}")
            else:
                self._tracer = get_tracer(self.pipeline_id)
            self._root_span = self._tracer.start_span("pipeline.run")
            self._root_span.set_attribute("pipeline.run_id", self.run_id)
            self._root_span.set_attribute("pipeline.name", self.name)
            self._root_span.set_attribute("pipeline.id", self.pipeline_id)
            self._root_span.set_attribute("pipeline.type", "batch")
            self._root_span.set_attribute("deployment.environment", self.environment)
            self._root_span.set_attribute("pipeline.status", "running")
            if self.retry_attempt > 0:
                self._root_span.set_attribute("retry.attempt", self.retry_attempt)
            logger.debug("watch_obs: pipeline started run_id=%s", self.run_id)
        except Exception as exc:
            logger.warning("watch_obs: failed to start root span (%s)", exc)

        # Persist initial run state
        self._state_upsert("running")

        # Start heartbeat thread for long-running jobs
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name=f"watch-heartbeat-{self.run_id[:8]}"
        )
        self._heartbeat_thread.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration_ms = int((time.time() - self._start_time) * 1000) if self._start_time else 0

        if self._root_span is not None:
            try:
                self._root_span.set_attribute("pipeline.duration_ms", duration_ms)

                if exc_type is not None:
                    # Record failure — message only, no traceback (may contain PII)
                    self._root_span.set_attribute("pipeline.status", "failed")
                    self._root_span.set_attribute(
                        "pipeline.failure_reason",
                        f"{exc_type.__name__}: {str(exc_val)[:500]}"
                    )
                    self._root_span.set_status(StatusCode.ERROR)
                else:
                    self._root_span.set_attribute("pipeline.status", "success")
                    self._root_span.set_status(StatusCode.OK)

                self._root_span.end()
            except Exception as emit_exc:
                logger.warning("watch_obs: failed to end root span (%s)", emit_exc)

        # Flush spans asynchronously — non-blocking with timeout
        try:
            flush(timeout_ms=5000)
        except Exception as flush_exc:
            logger.warning("watch_obs: flush failed (%s)", flush_exc)

        # Stop heartbeat thread
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2)

        # Persist final run state
        final_status = "failed" if exc_type is not None else "success"
        self._state_upsert(final_status, duration_ms=duration_ms)

        # Do not suppress the original exception
        return False

    # ── Heartbeat ─────────────────────────────────────────────────────────

    def _heartbeat_loop(self, interval_s: int = 60) -> None:
        """
        Emits a heartbeat span event every `interval_s` seconds for long-running jobs.
        Runs in a daemon thread — stops when _heartbeat_stop is set.
        """
        while not self._heartbeat_stop.wait(timeout=interval_s):
            if self._root_span is not None:
                try:
                    elapsed_ms = int((time.time() - self._start_time) * 1000)
                    self._root_span.add_event(
                        "pipeline.heartbeat",
                        attributes={"elapsed_ms": elapsed_ms, "pipeline.run_id": self.run_id}
                    )
                    self._state_upsert("running")
                    logger.debug("watch_obs: heartbeat run_id=%s elapsed_ms=%d", self.run_id, elapsed_ms)
                except Exception as exc:
                    logger.warning("watch_obs: heartbeat failed (%s)", exc)

    # ── State store helpers ───────────────────────────────────────────────

    def _state_upsert(self, status: str, duration_ms: Optional[int] = None) -> None:
        try:
            from sdk.state_store import RunStateStore
            store = RunStateStore(db_path=self._db_path) if self._db_path else get_store()
            record = RunRecord(
                run_id=self.run_id,
                pipeline_id=self.pipeline_id,
                status=status,
                start_time=self._start_time or time.time(),
                last_updated=time.time(),
                retry_count=self.retry_attempt,
                duration_ms=duration_ms,
            )
            store.upsert(record)
        except Exception as exc:
            logger.warning("watch_obs: state upsert failed (%s)", exc)

    # ── Phase factory ─────────────────────────────────────────────────────

    def phase(self, phase_name: str, **kwargs: Any) -> Phase:
        """
        Returns a Phase context manager for the given ETL phase.

        Args:
            phase_name: one of 'extract', 'transform', 'load'
            **kwargs:   shorthand attributes (source_system, target_system, etc.)
        """
        if self._tracer is None:
            # SDK failed to initialise — return a no-op Phase
            logger.warning("watch_obs: tracer not initialised, phase '%s' will be a no-op", phase_name)
            self._tracer = trace.get_tracer("watch_obs.noop")

        return Phase(
            tracer=self._tracer,
            phase_name=phase_name,
            run_id=self.run_id,
            pipeline_id=self.pipeline_id,
            pipeline_name=self.name,
            environment=self.environment,
            **kwargs,
        )

    # ── Propagation helpers ───────────────────────────────────────────────

    def to_env_vars(self) -> dict[str, str]:
        """Env vars for injecting run identity into subprocesses."""
        return self._context.to_env_vars()

    def to_http_headers(self) -> dict[str, str]:
        """HTTP headers for propagating run identity across REST calls."""
        return self._context.to_http_headers()

    def to_kafka_headers(self) -> list[tuple[str, bytes]]:
        """Kafka message headers for event-driven propagation."""
        return self._context.to_kafka_headers()

    # ── Alternate constructors ────────────────────────────────────────────

    @classmethod
    def from_env(cls, name: str, pipeline_id: str, **kwargs) -> "Pipeline":
        """
        Constructs a Pipeline using run_id from environment variables.
        Used in child processes to continue the parent's trace.
        Falls back to generating a new run_id if env vars are not set.
        """
        ctx = RunContext.from_env()
        run_id = ctx.run_id if ctx else None
        environment = ctx.environment if ctx else None
        return cls(
            name=name,
            pipeline_id=pipeline_id,
            run_id=run_id,
            environment=environment,
            **kwargs,
        )
