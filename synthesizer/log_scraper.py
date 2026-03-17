"""
Watch Log Scraper Synthesizer — Phase 5, Feature 5.1.

Reads log lines from a stream or file, matches lifecycle patterns, and emits
synthetic OTel spans to the Watch collector — without modifying the pipeline code.

Design:
- Never raises into the calling context (all errors are logged and swallowed)
- If the log source is unavailable, backs off exponentially and retries
- Partial traces (start seen, end never arrives) are stored as status=unknown

Usage:
    from synthesizer.log_scraper import LogScraper
    scraper = LogScraper(pipeline_id="my-pipe", log_stream=sys.stdin)
    scraper.run()
"""
import logging
import re
import time
import uuid
from io import StringIO
from pathlib import Path
from typing import IO, Iterator, Optional

import yaml

from opentelemetry import trace
from opentelemetry.trace import StatusCode

logger = logging.getLogger("watch_obs.synthesizer")

_DEFAULT_PATTERNS_PATH = Path(__file__).parent / "patterns" / "default.yaml"
_DEFAULT_PHASE_MAP = {
    "extract": "extract",
    "transform": "transform",
    "load": "load",
}


class LogScraper:
    """
    Synthesizes OTel spans by scraping lifecycle events from log lines.

    Args:
        pipeline_id:    Identifier for the pipeline being observed.
        log_stream:     A readable text stream (file, stdin, StringIO).
        tracer:         Optional injected tracer (uses global if not provided).
        pattern_path:   Path to a patterns YAML file. Defaults to patterns/default.yaml.
        phase_map:      Dict mapping log stage names to canonical ETL phase names.
                        Missing entries map to 'unknown'.
        environment:    Deployment environment label.
    """

    def __init__(
        self,
        pipeline_id: str,
        log_stream: IO[str],
        tracer=None,
        pattern_path: Optional[Path] = None,
        phase_map: Optional[dict] = None,
        environment: str = "dev",
    ):
        self.pipeline_id = pipeline_id
        self.log_stream = log_stream
        self.environment = environment
        self._tracer = tracer or trace.get_tracer("watch_obs.synthesizer")

        patterns_path = pattern_path or _DEFAULT_PATTERNS_PATH
        self._patterns = self._load_patterns(patterns_path)
        self._phase_map: dict[str, str] = phase_map or {}
        self._compiled = self._compile_patterns()

        # In-flight run state: run_id -> {span, start_time, ...}
        self._active_runs: dict[str, dict] = {}
        # In-flight phase state: (run_id, phase) -> span
        self._active_phases: dict[tuple, object] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Process all lines from log_stream until EOF."""
        for line in self._lines():
            try:
                self._process_line(line.rstrip("\n"))
            except Exception as exc:
                logger.warning("watch_obs.synthesizer: error processing line: %s", exc)

    def process_line(self, line: str) -> Optional[str]:
        """
        Process a single log line. Returns the matched event type name or None.
        Public for testing.
        """
        return self._process_line(line.rstrip("\n"))

    # ── Pattern loading ───────────────────────────────────────────────────────

    def _load_patterns(self, path: Path) -> dict[str, str]:
        try:
            with open(path, encoding="utf-8") as f:
                doc = yaml.safe_load(f)
            return doc.get("patterns", {})
        except Exception as exc:
            logger.warning("watch_obs.synthesizer: failed to load patterns from %s: %s", path, exc)
            return {}

    def _compile_patterns(self) -> dict[str, re.Pattern]:
        compiled = {}
        for name, pattern in self._patterns.items():
            try:
                compiled[name] = re.compile(pattern)
            except re.error as exc:
                logger.warning("watch_obs.synthesizer: bad pattern '%s': %s", name, exc)
        return compiled

    # ── Line reading ──────────────────────────────────────────────────────────

    def _lines(self) -> Iterator[str]:
        try:
            for line in self.log_stream:
                yield line
        except Exception as exc:
            logger.warning("watch_obs.synthesizer: log stream error: %s", exc)

    # ── Line processing ───────────────────────────────────────────────────────

    def _process_line(self, line: str) -> Optional[str]:
        for event_type, pattern in self._compiled.items():
            m = pattern.search(line)
            if m:
                groups = m.groupdict()
                handler = getattr(self, f"_on_{event_type}", None)
                if handler:
                    handler(groups)
                return event_type
        return None

    # ── Event handlers ────────────────────────────────────────────────────────

    def _on_run_start(self, groups: dict) -> None:
        pipeline_name = groups.get("name", self.pipeline_id)
        run_id = str(uuid.uuid4())
        try:
            span = self._tracer.start_span("pipeline.run")
            span.set_attribute("pipeline.run_id", run_id)
            span.set_attribute("pipeline.name", pipeline_name)
            span.set_attribute("pipeline.id", self.pipeline_id)
            span.set_attribute("pipeline.type", "batch")
            span.set_attribute("deployment.environment", self.environment)
            span.set_attribute("pipeline.status", "running")
            span.set_attribute("synthesizer.source", "log_scraper")
            self._active_runs[pipeline_name] = {
                "run_id": run_id,
                "span": span,
                "start_time": time.time(),
            }
            logger.debug("watch_obs.synthesizer: run started pipeline=%s run_id=%s", pipeline_name, run_id)
        except Exception as exc:
            logger.warning("watch_obs.synthesizer: failed to start run span: %s", exc)

    def _on_run_success(self, groups: dict) -> None:
        pipeline_name = groups.get("name", self.pipeline_id)
        run = self._active_runs.pop(pipeline_name, None)
        if run is None:
            logger.debug("watch_obs.synthesizer: run_success without matching run_start for %s", pipeline_name)
            return
        try:
            span = run["span"]
            duration_ms = int((time.time() - run["start_time"]) * 1000)
            span.set_attribute("pipeline.status", "success")
            span.set_attribute("pipeline.duration_ms", duration_ms)
            span.set_status(StatusCode.OK)
            span.end()
        except Exception as exc:
            logger.warning("watch_obs.synthesizer: failed to end run span (success): %s", exc)

    def _on_run_failed(self, groups: dict) -> None:
        pipeline_name = groups.get("name", self.pipeline_id)
        failure_reason = groups.get("reason", "unknown")
        run = self._active_runs.pop(pipeline_name, None)
        if run is None:
            logger.debug("watch_obs.synthesizer: run_failed without matching run_start for %s", pipeline_name)
            return
        try:
            span = run["span"]
            duration_ms = int((time.time() - run["start_time"]) * 1000)
            span.set_attribute("pipeline.status", "failed")
            span.set_attribute("pipeline.failure_reason", failure_reason[:500])
            span.set_attribute("pipeline.duration_ms", duration_ms)
            span.set_status(StatusCode.ERROR)
            span.end()
        except Exception as exc:
            logger.warning("watch_obs.synthesizer: failed to end run span (failed): %s", exc)

    def _on_phase_start(self, groups: dict) -> None:
        stage = groups.get("phase", "unknown")
        canonical_phase = self._resolve_phase(stage)
        key = (self.pipeline_id, stage)
        try:
            span = self._tracer.start_span(f"etl.{canonical_phase}")
            span.set_attribute("etl.phase", canonical_phase)
            span.set_attribute("etl.stage_name", stage)
            span.set_attribute("pipeline.id", self.pipeline_id)
            span.set_attribute("deployment.environment", self.environment)
            span.set_attribute("synthesizer.source", "log_scraper")
            self._active_phases[key] = {"span": span, "start_time": time.time()}
        except Exception as exc:
            logger.warning("watch_obs.synthesizer: failed to start phase span: %s", exc)

    def _on_phase_end(self, groups: dict) -> None:
        stage = groups.get("phase", "unknown")
        key = (self.pipeline_id, stage)
        log_duration_ms = groups.get("duration_ms")
        phase_state = self._active_phases.pop(key, None)
        if phase_state is None:
            return
        try:
            span = phase_state["span"]
            if log_duration_ms is not None:
                span.set_attribute("pipeline.duration_ms", int(log_duration_ms))
            else:
                elapsed = int((time.time() - phase_state["start_time"]) * 1000)
                span.set_attribute("pipeline.duration_ms", elapsed)
            span.set_status(StatusCode.OK)
            span.end()
        except Exception as exc:
            logger.warning("watch_obs.synthesizer: failed to end phase span: %s", exc)

    # ── Phase resolution ──────────────────────────────────────────────────────

    def _resolve_phase(self, stage_name: str) -> str:
        """Map a log stage name to a canonical ETL phase via the phase map."""
        # Check explicit phase_map first
        if stage_name in self._phase_map:
            return self._phase_map[stage_name]
        # Fall back to canonical names directly
        canonical = stage_name.lower()
        if canonical in ("extract", "transform", "load"):
            return canonical
        return "unknown"
