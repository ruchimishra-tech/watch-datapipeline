"""
Phase 12 Unit Tests — T12.3–T12.6

Offline tests for cardinality guard and span sampling.
T12.1 and T12.2 (namespace isolation) require a live Tempo instance.
T12.3 and T12.4 — cardinality guard strips pipeline_run_id from metrics
T12.5 and T12.6 — span sampling: success spans sampled, error spans always kept
"""
import pytest

from sdk.cardinality_guard import CardinalityGuard, MetricDataPoint
from sdk.span_sampling import SpanSampler, SpanDescriptor, STATUS_OK, STATUS_ERROR


# ── T12.3: Cardinality guard strips pipeline_run_id ──────────────────────────

@pytest.mark.phase12
class TestT123CardinalityGuardRunId:
    def test_run_id_stripped_from_metrics(self):
        guard = CardinalityGuard()
        data_points = [
            MetricDataPoint(
                metric_name="pipeline_runs_total",
                labels={"pipeline_run_id": "run-abc123", "status": "success"},
                value=1.0,
            )
        ]
        result = guard.process(data_points)
        assert "pipeline_run_id" not in result.processed[0].labels

    def test_other_labels_preserved(self):
        guard = CardinalityGuard()
        data_points = [
            MetricDataPoint(
                "pipeline_runs_total",
                {"pipeline_run_id": "run-abc", "pipeline_id": "my-pipe", "status": "success"},
                1.0,
            )
        ]
        result = guard.process(data_points)
        assert result.processed[0].labels["pipeline_id"] == "my-pipe"
        assert result.processed[0].labels["status"] == "success"

    def test_strip_count_recorded(self):
        guard = CardinalityGuard()
        data_points = [
            MetricDataPoint("m", {"pipeline_run_id": f"run-{i}"}, 1.0)
            for i in range(5)
        ]
        result = guard.process(data_points)
        assert result.stripped_labels.get("pipeline_run_id", 0) == 5

    def test_no_run_id_means_no_strip(self):
        guard = CardinalityGuard()
        data_points = [
            MetricDataPoint("m", {"pipeline_id": "p1", "status": "ok"}, 1.0)
        ]
        result = guard.process(data_points)
        assert result.total_stripped_count == 0


# ── T12.4: Cardinality guard counter ─────────────────────────────────────────

@pytest.mark.phase12
class TestT124CardinalityGuardCounter:
    def test_prometheus_lines_contain_stripped_counter(self):
        guard = CardinalityGuard()
        guard.process([MetricDataPoint("m", {"pipeline_run_id": "run-1"}, 1.0)])
        lines = guard.to_prometheus_lines()
        assert any("metrics_labels_stripped_total" in l for l in lines)
        assert any("pipeline_run_id" in l for l in lines)

    def test_counter_accumulates_across_batches(self):
        guard = CardinalityGuard()
        guard.process([MetricDataPoint("m", {"pipeline_run_id": "run-1"}, 1.0)])
        guard.process([MetricDataPoint("m", {"pipeline_run_id": "run-2"}, 1.0)])
        assert guard.total_stripped() == 2

    def test_high_cardinality_label_stripped(self):
        guard = CardinalityGuard(max_cardinality=5)
        # Create 10 unique values for label "request_id"
        data_points = [
            MetricDataPoint("m", {"request_id": f"req-{i}", "status": "ok"}, 1.0)
            for i in range(10)
        ]
        result = guard.process(data_points)
        # All request_id labels should be stripped
        for dp in result.processed:
            assert "request_id" not in dp.labels

    def test_low_cardinality_label_kept(self):
        guard = CardinalityGuard(max_cardinality=100)
        data_points = [
            MetricDataPoint("m", {"status": "ok"}, 1.0),
            MetricDataPoint("m", {"status": "fail"}, 1.0),
        ]
        result = guard.process(data_points)
        for dp in result.processed:
            assert "status" in dp.labels


# ── T12.5: Span sampling — success spans sampled at rate ─────────────────────

@pytest.mark.phase12
class TestT125SpanSampling:
    def test_10pct_rate_samples_roughly_10pct(self):
        sampler = SpanSampler(config={"high-vol-pipe": 0.1}, seed=42)
        spans = [
            SpanDescriptor(
                span_id=f"span-{i:04d}", trace_id="trace-1",
                pipeline_id="high-vol-pipe", status=STATUS_OK
            )
            for i in range(200)
        ]
        retained, dropped = sampler.sample_batch(spans)
        rate = len(retained) / len(spans)
        # With deterministic hashing, should be close to 10% (allow 5-20% range)
        assert 0.05 <= rate <= 0.20, f"Expected ~10% sample rate, got {rate:.1%}"

    def test_100pct_rate_retains_all(self):
        sampler = SpanSampler(config={"my-pipe": 1.0})
        spans = [
            SpanDescriptor(f"s{i}", "t1", "my-pipe", STATUS_OK)
            for i in range(50)
        ]
        retained, dropped = sampler.sample_batch(spans)
        assert len(retained) == 50
        assert len(dropped) == 0

    def test_unknown_pipeline_uses_full_rate(self):
        sampler = SpanSampler(config={})
        span = SpanDescriptor("s1", "t1", "unknown-pipe", STATUS_OK)
        decision = sampler.should_sample(span)
        assert decision.sampled is True
        assert decision.sample_rate == 1.0

    def test_zero_rate_drops_all_success(self):
        sampler = SpanSampler(config={"silent-pipe": 0.0})
        spans = [
            SpanDescriptor(f"s{i}", "t1", "silent-pipe", STATUS_OK)
            for i in range(20)
        ]
        retained, dropped = sampler.sample_batch(spans)
        assert len(retained) == 0
        assert len(dropped) == 20


# ── T12.6: Error spans not sampled (always retained) ─────────────────────────

@pytest.mark.phase12
class TestT126ErrorSpansNotSampled:
    def test_error_spans_always_retained_at_low_rate(self):
        sampler = SpanSampler(config={"high-vol-pipe": 0.1}, seed=42)
        error_spans = [
            SpanDescriptor(f"err-{i}", "t1", "high-vol-pipe", STATUS_ERROR)
            for i in range(20)
        ]
        retained, dropped = sampler.sample_batch(error_spans)
        assert len(retained) == 20
        assert len(dropped) == 0

    def test_error_span_decision_reason(self):
        sampler = SpanSampler(config={"p": 0.1})
        span = SpanDescriptor("s1", "t1", "p", STATUS_ERROR)
        decision = sampler.should_sample(span)
        assert decision.sampled is True
        assert "error" in decision.reason.lower()

    def test_mixed_batch_errors_retained(self):
        sampler = SpanSampler(config={"p": 0.0})  # drop all success
        spans = [
            SpanDescriptor("ok-1", "t1", "p", STATUS_OK),
            SpanDescriptor("err-1", "t1", "p", STATUS_ERROR),
            SpanDescriptor("ok-2", "t1", "p", STATUS_OK),
            SpanDescriptor("err-2", "t1", "p", STATUS_ERROR),
        ]
        retained, dropped = sampler.sample_batch(spans)
        retained_ids = {s.span_id for s in retained}
        assert "err-1" in retained_ids
        assert "err-2" in retained_ids
        assert "ok-1" not in retained_ids
        assert "ok-2" not in retained_ids

    def test_10_error_spans_all_in_tempo(self):
        """T12.6 logic: 10 error spans, all 10 should be retained."""
        sampler = SpanSampler(config={"high-vol-pipe": 0.1}, seed=99)
        error_spans = [
            SpanDescriptor(f"err-{i:02d}", "trace-xyz", "high-vol-pipe", STATUS_ERROR)
            for i in range(10)
        ]
        retained, dropped = sampler.sample_batch(error_spans)
        assert len(retained) == 10, f"All 10 error spans must be retained, got {len(retained)}"
