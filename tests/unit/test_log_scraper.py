"""
Phase 5 Unit Tests — T5.1–T5.6

Tests for the log scraper synthesizer (synthesizer/log_scraper.py).
All run offline — no OTel collector required. Uses injected mock tracers.

T5.1 — run_start pattern emits root span with correct attributes
T5.2 — run_success pattern ends span with status=success
T5.3 — run_failed pattern ends span with failure_reason
T5.4 — phase_start/phase_end timing matches log duration
T5.5 — phase map lookup maps stage name to canonical ETL phase
T5.6 — phase map fallback: unknown stage -> etl.phase = "unknown"
"""
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from synthesizer.log_scraper import LogScraper


# ── Test tracer / span mocks ──────────────────────────────────────────────────

def _make_mock_tracer():
    """Returns a mock tracer that records started spans."""
    tracer = MagicMock()
    span = MagicMock()
    tracer.start_span.return_value = span
    return tracer, span


# ── T5.1: run_start ───────────────────────────────────────────────────────────

@pytest.mark.phase5
class TestT51RunStart:
    """T5.1 — run_start pattern emits root span with pipeline.name and running status."""

    def test_run_start_creates_span(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        event = scraper.process_line("Pipeline 'test-pipe' started")
        assert event == "run_start"
        tracer.start_span.assert_called_once_with("pipeline.run")

    def test_run_start_sets_pipeline_name(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'customer-360' started")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("pipeline.name") == "customer-360"

    def test_run_start_sets_status_running(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'test-pipe' started")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("pipeline.status") == "running"

    def test_run_start_sets_synthesizer_source(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'test-pipe' started")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("synthesizer.source") == "log_scraper"

    def test_unrelated_line_returns_none(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        event = scraper.process_line("INFO 2026-03-17 some random log line")
        assert event is None


# ── T5.2: run_success ─────────────────────────────────────────────────────────

@pytest.mark.phase5
class TestT52RunSuccess:
    """T5.2 — run_success ends span with pipeline.status=success."""

    def test_run_success_sets_status(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'alpha' started")
        scraper.process_line("Pipeline 'alpha' completed successfully")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("pipeline.status") == "success"

    def test_run_success_ends_span(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'alpha' started")
        scraper.process_line("Pipeline 'alpha' completed successfully")
        span.end.assert_called_once()

    def test_run_success_without_prior_start_is_no_op(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        # No run_start — should not raise
        event = scraper.process_line("Pipeline 'orphan' completed successfully")
        assert event == "run_success"
        span.end.assert_not_called()


# ── T5.3: run_failed ─────────────────────────────────────────────────────────

@pytest.mark.phase5
class TestT53RunFailed:
    """T5.3 — run_failed ends span with pipeline.failure_reason from log."""

    def test_run_failed_sets_failure_reason(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'beta' started")
        scraper.process_line("Pipeline 'beta' failed: NullPointerException")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("pipeline.failure_reason") == "NullPointerException"

    def test_run_failed_sets_status_failed(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'beta' started")
        scraper.process_line("Pipeline 'beta' failed: timeout after 30s")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("pipeline.status") == "failed"

    def test_run_failed_ends_span(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'gamma' started")
        scraper.process_line("Pipeline 'gamma' failed: OOM")
        span.end.assert_called_once()

    def test_failure_reason_with_colon_in_message(self):
        """Failure reasons may contain colons — only the first one is the delimiter."""
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Pipeline 'delta' started")
        scraper.process_line("Pipeline 'delta' failed: java.lang.RuntimeException: bad data at row 42")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        reason = calls.get("pipeline.failure_reason", "")
        assert "java.lang.RuntimeException" in reason


# ── T5.4: phase timing ────────────────────────────────────────────────────────

@pytest.mark.phase5
class TestT54PhaseTiming:
    """T5.4 — phase duration from log line is stored on the phase span."""

    def test_phase_end_uses_log_duration(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Stage 'extract' starting")
        scraper.process_line("Stage 'extract' completed in 4500ms")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("pipeline.duration_ms") == 4500

    def test_phase_end_without_start_is_no_op(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        event = scraper.process_line("Stage 'extract' completed in 1000ms")
        assert event == "phase_end"
        span.end.assert_not_called()

    def test_phase_span_is_started_on_phase_start(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Stage 'transform' starting")
        tracer.start_span.assert_called_once()

    def test_phase_span_ended_on_phase_end(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Stage 'load' starting")
        scraper.process_line("Stage 'load' completed in 200ms")
        span.end.assert_called_once()


# ── T5.5: phase map lookup ────────────────────────────────────────────────────

@pytest.mark.phase5
class TestT55PhaseMapLookup:
    """T5.5 — stage name in phase_map maps to canonical etl.phase."""

    def test_phase_map_maps_custom_stage_to_extract(self):
        tracer, span = _make_mock_tracer()
        phase_map = {"process_accounts": "transform", "fetch_data": "extract"}
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer, phase_map=phase_map)
        scraper.process_line("Stage 'fetch_data' starting")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("etl.phase") == "extract"

    def test_phase_map_maps_custom_stage_to_transform(self):
        tracer, span = _make_mock_tracer()
        phase_map = {"process_accounts": "transform"}
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer, phase_map=phase_map)
        scraper.process_line("Stage 'process_accounts' starting")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("etl.phase") == "transform"

    def test_canonical_names_resolve_without_phase_map(self):
        """'extract', 'transform', 'load' resolve correctly without a phase_map."""
        for canonical in ("extract", "transform", "load"):
            tracer, span = _make_mock_tracer()
            scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
            scraper.process_line(f"Stage '{canonical}' starting")
            calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
            assert calls.get("etl.phase") == canonical, (
                f"Stage '{canonical}' should resolve to etl.phase='{canonical}'"
            )


# ── T5.6: phase map fallback ──────────────────────────────────────────────────

@pytest.mark.phase5
class TestT56PhaseMapFallback:
    """T5.6 — stage not in phase_map -> etl.phase = 'unknown', no error raised."""

    def test_unknown_stage_maps_to_unknown(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer, phase_map={})
        scraper.process_line("Stage 'mystery_stage_xyz' starting")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("etl.phase") == "unknown"

    def test_unknown_stage_does_not_raise(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        # Should not raise
        scraper.process_line("Stage 'some_weird_stage' starting")
        scraper.process_line("Stage 'some_weird_stage' completed in 300ms")

    def test_stage_name_preserved_in_attribute(self):
        tracer, span = _make_mock_tracer()
        scraper = LogScraper("test-pipe", StringIO(""), tracer=tracer)
        scraper.process_line("Stage 'legacy_batch_v2' starting")
        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert calls.get("etl.stage_name") == "legacy_batch_v2"


# ── Full run: multiple log lines through StringIO ─────────────────────────────

@pytest.mark.phase5
class TestFullLogRun:
    """Integration of all patterns: process a realistic log sequence."""

    def test_full_lifecycle(self):
        tracer, span = _make_mock_tracer()
        log = StringIO(
            "Pipeline 'orders-daily' started\n"
            "Stage 'extract' starting\n"
            "Stage 'extract' completed in 1200ms\n"
            "Stage 'transform' starting\n"
            "Stage 'transform' completed in 800ms\n"
            "Stage 'load' starting\n"
            "Stage 'load' completed in 400ms\n"
            "Pipeline 'orders-daily' completed successfully\n"
        )
        scraper = LogScraper("orders-daily", log, tracer=tracer)
        scraper.run()
        # span.end() called 4 times: 1 root + 3 phase spans
        # (tracer.start_span called 4 times, each returns same mock span)
        assert span.end.call_count == 4
