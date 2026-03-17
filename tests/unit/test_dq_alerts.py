"""
Phase 7 Unit Tests — T7.10, T7.11

Offline tests for the DQ alert rules (alerts/dq-alerts.yaml).
No Prometheus required — validates YAML structure and rule correctness.

T7.10 — DQ alert YAML is valid and loads correctly
T7.11 — DataQualityCriticalViolation alert fires immediately (for: 0m)
"""
from pathlib import Path

import pytest
import yaml

ALERTS_DIR = Path(__file__).parents[2] / "alerts"


def _load_dq_alerts() -> dict:
    path = ALERTS_DIR / "dq-alerts.yaml"
    if not path.exists():
        pytest.skip("dq-alerts.yaml not found")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _find_alert(doc: dict, name: str) -> dict | None:
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            if rule.get("alert") == name:
                return rule
    return None


# ── T7.10: DQ alert YAML structure ───────────────────────────────────────────

@pytest.mark.phase7
class TestT710DQAlertsStructure:
    def test_yaml_loads_without_error(self):
        doc = _load_dq_alerts()
        assert "groups" in doc

    def test_has_at_least_one_group(self):
        doc = _load_dq_alerts()
        assert len(doc["groups"]) >= 1

    def test_critical_violation_alert_exists(self):
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "DataQualityCriticalViolation")
        assert rule is not None, "DataQualityCriticalViolation alert not found"

    def test_schema_change_alert_exists(self):
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "SchemaChangeDetected")
        assert rule is not None, "SchemaChangeDetected alert not found"

    def test_warning_degraded_alert_exists(self):
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "DataQualityWarningDegraded")
        assert rule is not None, "DataQualityWarningDegraded alert not found"

    def test_all_alerts_have_required_labels(self):
        doc = _load_dq_alerts()
        for group in doc["groups"]:
            for rule in group.get("rules", []):
                labels = rule.get("labels", {})
                assert "severity" in labels, f"{rule.get('alert')} missing severity label"
                assert "runbook_url" in labels, f"{rule.get('alert')} missing runbook_url label"

    def test_all_runbook_urls_are_http(self):
        doc = _load_dq_alerts()
        for group in doc["groups"]:
            for rule in group.get("rules", []):
                url = rule.get("labels", {}).get("runbook_url", "")
                assert url.startswith("http"), (
                    f"{rule.get('alert')} runbook_url must start with http, got: {url!r}"
                )

    def test_all_severity_values_are_valid(self):
        valid = {"critical", "warning", "info"}
        doc = _load_dq_alerts()
        for group in doc["groups"]:
            for rule in group.get("rules", []):
                sev = rule.get("labels", {}).get("severity", "")
                assert sev in valid, (
                    f"{rule.get('alert')} has invalid severity: {sev!r}"
                )

    def test_all_alerts_have_expr(self):
        doc = _load_dq_alerts()
        for group in doc["groups"]:
            for rule in group.get("rules", []):
                assert "expr" in rule, f"{rule.get('alert')} missing expr"


# ── T7.11: DataQualityCriticalViolation fires immediately ────────────────────

@pytest.mark.phase7
class TestT711CriticalViolationAlert:
    def test_alert_fires_immediately(self):
        """T7.11 — for: 0m means it fires without waiting."""
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "DataQualityCriticalViolation")
        for_val = rule.get("for", "0m")
        assert for_val in ("0m", "0s", ""), (
            f"DataQualityCriticalViolation should fire immediately (for: 0m), got: {for_val}"
        )

    def test_severity_is_critical(self):
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "DataQualityCriticalViolation")
        assert rule["labels"]["severity"] == "critical"

    def test_expr_references_critical_severity(self):
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "DataQualityCriticalViolation")
        assert "critical" in rule["expr"]

    def test_expr_references_dq_violations_metric(self):
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "DataQualityCriticalViolation")
        assert "pipeline_dq_violations_total" in rule["expr"]

    def test_schema_alert_expr_references_schema_rule(self):
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "SchemaChangeDetected")
        assert "schema_check" in rule["expr"]

    def test_warning_degraded_has_sustained_for_duration(self):
        """DataQualityWarningDegraded should have a meaningful 'for' to avoid noise."""
        doc = _load_dq_alerts()
        rule = _find_alert(doc, "DataQualityWarningDegraded")
        for_val = rule.get("for", "0m")
        assert for_val not in ("0m", "0s", ""), (
            f"DataQualityWarningDegraded should have a non-zero 'for' duration, got: {for_val}"
        )
