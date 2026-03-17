"""
Phase 6 Unit Tests — T6.3, T6.4 + streaming metrics unit tests

T6.3 — StreamingConsumerLagHigh alert rule fires when lag exceeds threshold (structure)
T6.4 — StreamingConsumerLagHigh has non-zero 'for' so it does not fire immediately
T6.5 — StreamingConsumerDown rule references throughput and lag metrics
T6.6 — StreamingErrorRateHigh rule fires at >1% error rate

Also covers:
- StreamingMetricsSnapshot label correctness
- StreamingMetricsSnapshot Prometheus exposition format
- StreamingMetricsCollector collect_once with mock Kafka client
- Environment label isolation (T6.8 structure)
"""
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from sdk.streaming import (
    StreamingMetricsCollector,
    StreamingMetricsSnapshot,
    METRIC_LAG_SUM,
    METRIC_THROUGHPUT,
    METRIC_ERROR_RATE,
    METRIC_E2E_LATENCY_MS,
    REQUIRED_LABELS,
)

ALERTS_DIR = Path(__file__).parents[2] / "alerts"


def _load_streaming_alerts() -> dict:
    path = ALERTS_DIR / "streaming-alerts.yaml"
    if not path.exists():
        pytest.skip("streaming-alerts.yaml not found")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _find_alert(doc: dict, name: str) -> dict | None:
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            if rule.get("alert") == name:
                return rule
    return None


# ── StreamingMetricsSnapshot ──────────────────────────────────────────────────

@pytest.mark.phase6
class TestStreamingMetricsSnapshot:
    def test_labels_carry_all_required_keys(self):
        snap = StreamingMetricsSnapshot(
            consumer_group="orders-cg",
            topic="orders.raw",
            pipeline_id="orders-streaming",
            environment="production",
        )
        labels = snap.labels()
        for key in ("consumer_group", "topic", "pipeline_id", "deployment.environment"):
            assert key in labels, f"Required label '{key}' missing from snapshot labels"

    def test_environment_label_value(self):
        snap = StreamingMetricsSnapshot(
            consumer_group="cg1", topic="t1", pipeline_id="p1", environment="staging"
        )
        assert snap.labels()["deployment.environment"] == "staging"

    def test_prometheus_lines_contain_all_metrics(self):
        snap = StreamingMetricsSnapshot(
            consumer_group="cg1", topic="t1", pipeline_id="p1", environment="dev",
            lag_sum=5000, throughput_msgs_per_sec=100.5, error_rate=0.002,
            e2e_latency_ms_p50=12.0, e2e_latency_ms_p95=45.0, e2e_latency_ms_p99=120.0,
        )
        lines = snap.to_prometheus_lines()
        metric_names = [l.split("{")[0] for l in lines]
        assert METRIC_LAG_SUM in metric_names
        assert METRIC_THROUGHPUT in metric_names
        assert METRIC_ERROR_RATE in metric_names
        assert METRIC_E2E_LATENCY_MS in metric_names

    def test_prometheus_lines_contain_lag_value(self):
        snap = StreamingMetricsSnapshot(
            consumer_group="cg", topic="t", pipeline_id="p", environment="dev",
            lag_sum=42000,
        )
        lines = "\n".join(snap.to_prometheus_lines())
        assert "42000" in lines

    def test_prometheus_lines_contain_all_quantiles(self):
        snap = StreamingMetricsSnapshot(
            consumer_group="cg", topic="t", pipeline_id="p", environment="dev",
            e2e_latency_ms_p50=10.0, e2e_latency_ms_p95=50.0, e2e_latency_ms_p99=100.0,
        )
        lines = "\n".join(snap.to_prometheus_lines())
        assert 'quantile="0.5"' in lines
        assert 'quantile="0.95"' in lines
        assert 'quantile="0.99"' in lines


# ── StreamingMetricsCollector with mock Kafka client ─────────────────────────

@pytest.mark.phase6
class TestStreamingMetricsCollector:
    def _mock_kafka_client(self, lag: int = 1000, error_rate: float = 0.0):
        client = MagicMock()
        client.get_consumer_lag.return_value = (lag, lag + 5000)
        client.get_error_rate.return_value = error_rate
        client.get_e2e_latency_percentiles.return_value = {"p50": 10.0, "p95": 50.0, "p99": 100.0}
        return client

    def test_collect_once_returns_snapshots(self):
        client = self._mock_kafka_client(lag=500)
        collector = StreamingMetricsCollector(
            bootstrap_servers="kafka:9092",
            consumer_group="test-cg",
            topics=["test.topic"],
            pipeline_id="test-pipe",
            environment="dev",
            _kafka_client=client,
        )
        snapshots = collector.collect_once()
        assert len(snapshots) == 1
        assert snapshots[0].lag_sum == 500

    def test_collect_once_carries_correct_labels(self):
        client = self._mock_kafka_client()
        collector = StreamingMetricsCollector(
            bootstrap_servers="kafka:9092",
            consumer_group="orders-cg",
            topics=["orders.raw"],
            pipeline_id="orders-streaming",
            environment="production",
            _kafka_client=client,
        )
        snapshots = collector.collect_once()
        labels = snapshots[0].labels()
        assert labels["consumer_group"] == "orders-cg"
        assert labels["topic"] == "orders.raw"
        assert labels["pipeline_id"] == "orders-streaming"
        assert labels["deployment.environment"] == "production"

    def test_collect_once_multiple_topics(self):
        client = self._mock_kafka_client()
        collector = StreamingMetricsCollector(
            bootstrap_servers="kafka:9092",
            consumer_group="multi-cg",
            topics=["topic.a", "topic.b", "topic.c"],
            pipeline_id="multi-pipe",
            environment="dev",
            _kafka_client=client,
        )
        snapshots = collector.collect_once()
        assert len(snapshots) == 3
        topics = {s.topic for s in snapshots}
        assert topics == {"topic.a", "topic.b", "topic.c"}

    def test_snapshot_accessible_after_collect(self):
        client = self._mock_kafka_client(lag=9999)
        collector = StreamingMetricsCollector(
            bootstrap_servers="kafka:9092",
            consumer_group="cg", topics=["t1"],
            pipeline_id="p", environment="dev",
            _kafka_client=client,
        )
        collector.collect_once()
        snap = collector.snapshot("t1")
        assert snap is not None
        assert snap.lag_sum == 9999

    def test_snapshot_none_before_first_collect(self):
        client = self._mock_kafka_client()
        collector = StreamingMetricsCollector(
            bootstrap_servers="kafka:9092",
            consumer_group="cg", topics=["t1"],
            pipeline_id="p", environment="dev",
            _kafka_client=client,
        )
        assert collector.snapshot("t1") is None

    def test_kafka_error_does_not_raise(self):
        client = MagicMock()
        client.get_consumer_lag.side_effect = Exception("Kafka unavailable")
        collector = StreamingMetricsCollector(
            bootstrap_servers="kafka:9092",
            consumer_group="cg", topics=["t1"],
            pipeline_id="p", environment="dev",
            _kafka_client=client,
        )
        # Must not raise
        snapshots = collector.collect_once()
        assert snapshots == []

    def test_T68_environment_label_isolation(self):
        """T6.8 — staging metrics carry deployment.environment=staging."""
        client = self._mock_kafka_client()
        collector = StreamingMetricsCollector(
            bootstrap_servers="kafka:9092",
            consumer_group="cg", topics=["t1"],
            pipeline_id="p", environment="staging",
            _kafka_client=client,
        )
        snapshots = collector.collect_once()
        assert snapshots[0].environment == "staging"
        assert snapshots[0].labels()["deployment.environment"] == "staging"


# ── T6.3 / T6.4: Alert rule structure ────────────────────────────────────────

@pytest.mark.phase6
class TestT63T64StreamingLagAlert:
    """T6.3 — Rule fires when lag > threshold. T6.4 — 'for: 5m' prevents early firing."""

    def test_alert_exists(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerLagHigh")
        assert rule is not None, "StreamingConsumerLagHigh alert rule not found"

    def test_expr_references_lag_metric(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerLagHigh")
        assert METRIC_LAG_SUM in rule["expr"], (
            f"StreamingConsumerLagHigh expr must reference {METRIC_LAG_SUM}"
        )

    def test_T64_fires_after_5m_not_immediately(self):
        """T6.4 — 'for' duration is 5m so transient spikes don't page."""
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerLagHigh")
        for_val = rule.get("for", "0m")
        assert for_val not in ("0m", "0s", "", None), (
            f"StreamingConsumerLagHigh should have 'for: 5m' to avoid false positives; got: {for_val}"
        )

    def test_severity_is_warning(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerLagHigh")
        assert rule["labels"]["severity"] == "warning"

    def test_has_runbook_url(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerLagHigh")
        runbook = rule["labels"].get("runbook_url", "")
        assert runbook.startswith("http")


@pytest.mark.phase6
class TestT65ConsumerDownAlert:
    """T6.5 — StreamingConsumerDown rule uses throughput and lag metrics."""

    def test_alert_exists(self):
        doc = _load_streaming_alerts()
        assert _find_alert(doc, "StreamingConsumerDown") is not None

    def test_expr_references_throughput(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerDown")
        assert METRIC_THROUGHPUT in rule["expr"]

    def test_expr_references_lag(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerDown")
        assert METRIC_LAG_SUM in rule["expr"]

    def test_severity_is_critical(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingConsumerDown")
        assert rule["labels"]["severity"] == "critical"


@pytest.mark.phase6
class TestT66ErrorRateAlert:
    """T6.6 — StreamingErrorRateHigh fires when error_rate > 1%."""

    def test_alert_exists(self):
        doc = _load_streaming_alerts()
        assert _find_alert(doc, "StreamingErrorRateHigh") is not None

    def test_expr_references_error_metric(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingErrorRateHigh")
        assert METRIC_ERROR_RATE in rule["expr"]

    def test_threshold_is_1_percent(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingErrorRateHigh")
        assert "0.01" in rule["expr"], (
            "StreamingErrorRateHigh should fire when error_rate > 0.01 (1%)"
        )

    def test_fires_after_5m(self):
        doc = _load_streaming_alerts()
        rule = _find_alert(doc, "StreamingErrorRateHigh")
        for_val = rule.get("for", "0m")
        assert for_val not in ("0m", "0s", "", None)


# ── Streaming dashboard validation ────────────────────────────────────────────

@pytest.mark.phase6
class TestStreamingDashboardValidation:
    def test_streaming_dashboard_passes_validator(self):
        from cli.validate_dashboards import validate_dashboard
        path = Path(__file__).parents[2] / "dashboards" / "l2-streaming.json"
        if not path.exists():
            pytest.skip("l2-streaming.json not found")
        errors = validate_dashboard(path)
        assert errors == [], f"l2-streaming.json validation errors: {errors}"
