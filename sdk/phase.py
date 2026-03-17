"""
Phase context manager — represents one ETL phase (extract / transform / load)
within a pipeline run.
"""
import logging
import time
from typing import Any, Optional

from opentelemetry import trace
from opentelemetry.trace import Span, StatusCode

logger = logging.getLogger("watch_obs.phase")

# Valid ETL phase names
VALID_PHASES = {"extract", "transform", "load"}


class Phase:
    """
    Context manager for a single ETL phase span.

    Usage:
        with run.phase("extract", source_system="oracle") as phase:
            phase.set_attribute("extract.rows_read", 50000)
            phase.record_metric("bytes_read", 1048576)
    """

    def __init__(
        self,
        tracer: trace.Tracer,
        phase_name: str,
        run_id: str,
        pipeline_id: str,
        pipeline_name: str,
        environment: str,
        **kwargs: Any,
    ):
        if phase_name not in VALID_PHASES:
            raise ValueError(f"phase_name must be one of {VALID_PHASES}, got '{phase_name}'")

        self._tracer = tracer
        self._phase_name = phase_name
        self._run_id = run_id
        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._environment = environment
        self._extra_attrs = kwargs
        self._span: Optional[Span] = None
        self._start_time: Optional[float] = None

    def __enter__(self) -> "Phase":
        self._start_time = time.time()
        span_name = f"etl.{self._phase_name}"
        self._span = self._tracer.start_span(span_name)

        # Required root attributes on every span
        self._span.set_attribute("pipeline.run_id", self._run_id)
        self._span.set_attribute("pipeline.name", self._pipeline_name)
        self._span.set_attribute("pipeline.id", self._pipeline_id)
        self._span.set_attribute("pipeline.type", "batch")
        self._span.set_attribute("deployment.environment", self._environment)
        self._span.set_attribute("pipeline.status", "running")
        self._span.set_attribute("etl.phase", self._phase_name)

        # Any extra keyword args passed at phase creation (e.g. source_system)
        for key, value in self._extra_attrs.items():
            # Map shorthand names to full attribute paths
            mapped_key = _map_shorthand(self._phase_name, key)
            try:
                self._span.set_attribute(mapped_key, value)
            except Exception as exc:
                logger.warning("watch_obs: could not set attribute %s: %s", mapped_key, exc)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._span is None:
            return False

        duration_ms = int((time.time() - self._start_time) * 1000)
        self._span.set_attribute(f"{self._phase_name}.duration_ms", duration_ms)

        if exc_type is not None:
            # Failure — record exception type and message but NOT the full traceback (may contain PII)
            self._span.set_attribute("pipeline.status", "failed")
            self._span.set_attribute("pipeline.failure_phase", self._phase_name)
            self._span.set_attribute(
                "pipeline.failure_reason",
                f"{exc_type.__name__}: {str(exc_val)[:500]}"
            )
            self._span.set_status(StatusCode.ERROR)
            logger.debug("watch_obs: phase %s failed — %s", self._phase_name, exc_val)
        else:
            self._span.set_attribute("pipeline.status", "success")
            self._span.set_status(StatusCode.OK)

        try:
            self._span.end()
        except Exception as exc:
            logger.warning("watch_obs: span.end() failed (%s)", exc)

        # Do not suppress the exception — let Pipeline.__exit__ handle it
        return False

    def set_attribute(self, key: str, value: Any) -> None:
        """Set an arbitrary span attribute. Silently no-ops if span is not active."""
        if self._span is None:
            return
        try:
            self._span.set_attribute(key, value)
        except Exception as exc:
            logger.warning("watch_obs: could not set attribute %s: %s", key, exc)

    def record_metric(self, name: str, value: Any) -> None:
        """
        Convenience alias for set_attribute — prefixes with the phase name.
        e.g. phase.record_metric("rows_read", 50000)
             → sets extract.rows_read = 50000 on the phase span.
        """
        full_key = f"{self._phase_name}.{name}"
        self.set_attribute(full_key, value)

    def emit_event(self, name: str, **attributes: Any) -> None:
        """Add a named span event (e.g. schema_drift_detected, dq_failure)."""
        if self._span is None:
            return
        try:
            self._span.add_event(name, attributes=attributes)
        except Exception as exc:
            logger.warning("watch_obs: could not add event %s: %s", name, exc)


def _map_shorthand(phase: str, key: str) -> str:
    """
    Maps shorthand kwargs to full attribute names.
    e.g. source_system → extract.source.system
         target_system → load.target.system
    """
    shorthands = {
        "source_system": f"{phase}.source.system",
        "target_system": f"{phase}.target.system",
        "source_table":  f"{phase}.source.table",
        "target_table":  f"{phase}.target.table",
    }
    return shorthands.get(key, key)
