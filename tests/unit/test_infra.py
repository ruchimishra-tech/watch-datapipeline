"""
Phase 11 Unit Tests — T11.6, T11.7

Offline tests for infrastructure alert rules and Spark metrics.

T11.6 — InfrastructureCPUHigh alert is correctly structured (for: 10m, threshold 85%)
T11.7 — InfrastructureCPUHigh does NOT fire below threshold (structural check)
"""
import json
from pathlib import Path

import pytest
import yaml

ALERTS_DIR = Path(__file__).parents[2] / "alerts"
DASHBOARDS_DIR = Path(__file__).parents[2] / "dashboards"


def _load_infra_alerts() -> dict:
    path = ALERTS_DIR / "infra-alerts.yaml"
    if not path.exists():
        pytest.skip("infra-alerts.yaml not found")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _find_alert(doc: dict, name: str) -> dict | None:
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            if rule.get("alert") == name:
                return rule
    return None


# ── T11.6: CPU alert fires at threshold ──────────────────────────────────────

@pytest.mark.phase11
class TestT116CPUAlertFiresAtThreshold:
    def test_cpu_alert_exists(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        assert rule is not None

    def test_cpu_alert_has_10m_for_duration(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        assert rule.get("for") == "10m"

    def test_cpu_alert_threshold_is_85(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        assert "85" in rule["expr"], "CPU threshold should be 85%"

    def test_cpu_alert_severity_is_warning(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        assert rule["labels"]["severity"] == "warning"

    def test_cpu_alert_references_node_cpu_metric(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        assert "node_cpu_seconds_total" in rule["expr"]

    def test_cpu_alert_uses_idle_mode(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        assert "idle" in rule["expr"]


# ── T11.7: CPU alert does NOT fire below threshold (structural) ───────────────

@pytest.mark.phase11
class TestT117CPUAlertNotFiresBelow:
    def test_expr_uses_greater_than_operator(self):
        """Structural check: expr uses '>' not '>=' at threshold, so 85% exact doesn't fire."""
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        # expr should use "> 85" not ">= 85"
        assert "> 85" in rule["expr"] or ">85" in rule["expr"]

    def test_for_duration_prevents_flapping(self):
        """10m for: means transient spikes don't trigger the alert."""
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "InfrastructureCPUHigh")
        assert rule.get("for", "0m") != "0m", "CPU alert should have a non-zero 'for' duration"


# ── Infra alert structure ──────────────────────────────────────────────────────

@pytest.mark.phase11
class TestInfraAlertStructure:
    def test_oom_alert_fires_immediately(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "PodOOMKilled")
        assert rule is not None
        assert rule.get("for", "0m") in ("0m", "0s", "")

    def test_oom_alert_is_critical(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "PodOOMKilled")
        assert rule["labels"]["severity"] == "critical"

    def test_spark_gc_alert_exists(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "SparkExecutorHighGC")
        assert rule is not None

    def test_spark_gc_threshold_is_20pct(self):
        doc = _load_infra_alerts()
        rule = _find_alert(doc, "SparkExecutorHighGC")
        assert "0.20" in rule["expr"] or "20" in rule["expr"]

    def test_all_infra_alerts_have_runbook(self):
        doc = _load_infra_alerts()
        for group in doc["groups"]:
            for rule in group.get("rules", []):
                url = rule.get("labels", {}).get("runbook_url", "")
                assert url.startswith("http"), f"{rule.get('alert')} missing runbook_url"


# ── Spark metrics unit tests ───────────────────────────────────────────────────

@pytest.mark.phase11
class TestSparkMetrics:
    def test_prometheus_lines_contain_heap_metric(self):
        from sdk.spark_metrics import SparkMetricsSnapshot, ExecutorMetrics
        snapshot = SparkMetricsSnapshot(app_id="test-app-001")
        snapshot.executors.append(ExecutorMetrics(
            executor_id="1",
            jvm_heap_used_bytes=512 * 1024 * 1024,
            jvm_gc_time_ms=1200,
            jvm_task_time_ms=60000,
        ))
        lines = snapshot.to_prometheus_lines()
        assert any("spark_executor_jvm_heap_used_bytes" in l for l in lines)
        assert any("test-app-001" in l for l in lines)

    def test_prometheus_lines_contain_gc_metric(self):
        from sdk.spark_metrics import SparkMetricsSnapshot, ExecutorMetrics
        snapshot = SparkMetricsSnapshot(app_id="test-app-002")
        snapshot.executors.append(ExecutorMetrics("1", jvm_gc_time_ms=500))
        lines = snapshot.to_prometheus_lines()
        assert any("spark_executor_jvm_gc_time_ms" in l for l in lines)

    def test_stage_quantile_p50(self):
        from sdk.spark_metrics import StageMetrics
        stage = StageMetrics(stage_id=1, task_durations_ms=[100.0, 200.0, 300.0, 400.0])
        p50 = stage.quantile(0.5)
        assert p50 >= 200.0

    def test_stage_empty_returns_zero(self):
        from sdk.spark_metrics import StageMetrics
        stage = StageMetrics(stage_id=1, task_durations_ms=[])
        assert stage.quantile(0.5) == 0.0

    def test_injectable_mock_client(self):
        from sdk.spark_metrics import SparkMetricsCollector, SparkMetricsSnapshot
        from unittest.mock import MagicMock
        mock_client = MagicMock()
        mock_client.collect.return_value = SparkMetricsSnapshot(app_id="mock-app")
        collector = SparkMetricsCollector("mock-app", _client=mock_client)
        result = collector.collect()
        assert result.app_id == "mock-app"
        mock_client.collect.assert_called_once_with("mock-app")


# ── L3 dashboard structure ────────────────────────────────────────────────────

@pytest.mark.phase11
class TestL3Dashboard:
    def _load(self) -> dict:
        path = DASHBOARDS_DIR / "l3-infra.json"
        if not path.exists():
            pytest.skip("l3-infra.json not found")
        return json.loads(path.read_text(encoding="utf-8"))

    def test_dashboard_has_uid(self):
        d = self._load()
        assert d.get("uid") == "watch-l3-infra"

    def test_dashboard_has_environment_variable(self):
        d = self._load()
        names = [v["name"] for v in d.get("templating", {}).get("list", [])]
        assert "environment" in names

    def test_dashboard_has_cpu_heatmap_panel(self):
        d = self._load()
        titles = [p["title"] for p in d.get("panels", [])]
        assert any("cpu" in t.lower() or "heatmap" in t.lower() for t in titles)

    def test_dashboard_has_pod_health_panel(self):
        d = self._load()
        titles = [p["title"] for p in d.get("panels", [])]
        assert any("pod" in t.lower() for t in titles)

    def test_dashboard_has_pipeline_timeline_panel(self):
        d = self._load()
        titles = [p["title"] for p in d.get("panels", [])]
        assert any("pipeline" in t.lower() or "timeline" in t.lower() for t in titles)
