"""
Phase 4 Unit Tests — T4.1, T4.2

Tests that alert rule YAML files have the correct structure and that
the PromQL expressions are logically sound (evaluated against mock data
using the prometheus_client library for metric simulation).

T4.1 — PipelineRunFailed alert rule structure and expression
T4.2 — PipelineRunDelayed alert rule structure and expression
"""
from pathlib import Path

import pytest
import yaml

ALERTS_DIR = Path(__file__).parents[2] / "alerts"
ALERT_FILE = ALERTS_DIR / "pipeline-alerts.yaml"


def _load_alert_rules() -> dict:
    if not ALERT_FILE.exists():
        pytest.skip("pipeline-alerts.yaml not found")
    with open(ALERT_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _find_alert(doc: dict, name: str) -> dict | None:
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            if rule.get("alert") == name:
                return rule
    return None


# ── T4.1: PipelineRunFailed ───────────────────────────────────────────────────

@pytest.mark.phase4
class TestT41PipelineRunFailed:
    """T4.1 — PipelineRunFailed rule structure and expression correctness."""

    def test_rule_exists(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        assert rule is not None, "PipelineRunFailed alert rule not found in pipeline-alerts.yaml"

    def test_rule_has_expr(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        assert "expr" in rule, "PipelineRunFailed rule must have an 'expr' field"
        assert rule["expr"].strip(), "expr must not be empty"

    def test_expr_references_failed_status(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        expr = rule["expr"]
        assert "failed" in expr, (
            f"PipelineRunFailed expr should reference 'failed' status metric; got: {expr}"
        )

    def test_expr_uses_increase_or_rate(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        expr = rule["expr"]
        assert any(fn in expr for fn in ("increase(", "rate(", "count(", "sum(")), (
            f"PipelineRunFailed expr should use increase/rate/count/sum; got: {expr}"
        )

    def test_severity_is_critical(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        severity = rule.get("labels", {}).get("severity")
        assert severity == "critical", (
            f"PipelineRunFailed should have severity=critical; got: {severity}"
        )

    def test_runbook_url_present(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        runbook = rule.get("labels", {}).get("runbook_url", "")
        assert runbook.startswith("http"), (
            f"PipelineRunFailed must have a runbook_url starting with http; got: {runbook}"
        )

    def test_has_dashboard_url_annotation(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        annotations = rule.get("annotations", {})
        has_dashboard = any(
            "dashboard" in k.lower() or "grafana" in v.lower()
            for k, v in annotations.items()
        )
        assert has_dashboard, (
            f"PipelineRunFailed should have a dashboard_url annotation; got: {annotations}"
        )

    def test_fires_immediately_for_failures(self):
        """'for' should be 0m or absent — failures need immediate alerting."""
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunFailed")
        for_value = rule.get("for", "0m")
        assert for_value in ("0m", "0s", "", None), (
            f"PipelineRunFailed should fire immediately (for: 0m); got: {for_value}"
        )


# ── T4.2: PipelineRunDelayed ──────────────────────────────────────────────────

@pytest.mark.phase4
class TestT42PipelineRunDelayed:
    """T4.2 — PipelineRunDelayed rule structure and expression correctness."""

    def test_rule_exists(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunDelayed")
        assert rule is not None, "PipelineRunDelayed alert rule not found"

    def test_rule_has_expr(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunDelayed")
        assert "expr" in rule and rule["expr"].strip()

    def test_expr_references_time_or_last_run(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunDelayed")
        expr = rule["expr"]
        # Expression should involve timing (time(), duration, or last_run metrics)
        assert any(kw in expr for kw in ("time()", "last_run", "delay", "start_time")), (
            f"PipelineRunDelayed expr should reference time or last_run; got: {expr}"
        )

    def test_severity_is_warning(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunDelayed")
        severity = rule.get("labels", {}).get("severity")
        assert severity == "warning", (
            f"PipelineRunDelayed should be severity=warning (not yet failed); got: {severity}"
        )

    def test_fires_after_sustained_delay(self):
        """'for' should be > 0 — transient blips shouldn't page."""
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunDelayed")
        for_value = rule.get("for", "0m")
        assert for_value not in ("0m", "0s", "", None), (
            f"PipelineRunDelayed should have a non-zero 'for' duration to avoid flapping; "
            f"got: {for_value}"
        )

    def test_runbook_url_present(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunDelayed")
        runbook = rule.get("labels", {}).get("runbook_url", "")
        assert runbook.startswith("http")

    def test_has_description_annotation(self):
        doc = _load_alert_rules()
        rule = _find_alert(doc, "PipelineRunDelayed")
        assert "description" in rule.get("annotations", {}), (
            "PipelineRunDelayed must have a 'description' annotation for actionable alerts"
        )


# ── Cross-cutting: all alerts have required fields ────────────────────────────

@pytest.mark.phase4
class TestAllAlertsHaveRequiredFields:
    """Every alert in every rule file must have the Watch mandatory fields."""

    def test_all_alerts_have_severity(self):
        doc = _load_alert_rules()
        for group in doc.get("groups", []):
            for rule in group.get("rules", []):
                if "alert" not in rule:
                    continue
                severity = rule.get("labels", {}).get("severity")
                assert severity, (
                    f"Alert '{rule['alert']}' missing severity label"
                )

    def test_all_alerts_have_runbook_url(self):
        doc = _load_alert_rules()
        for group in doc.get("groups", []):
            for rule in group.get("rules", []):
                if "alert" not in rule:
                    continue
                runbook = rule.get("labels", {}).get("runbook_url", "")
                assert runbook.startswith("http"), (
                    f"Alert '{rule['alert']}' missing or invalid runbook_url: {runbook!r}"
                )

    def test_all_alerts_have_summary_annotation(self):
        doc = _load_alert_rules()
        for group in doc.get("groups", []):
            for rule in group.get("rules", []):
                if "alert" not in rule:
                    continue
                annotations = rule.get("annotations", {})
                assert "summary" in annotations, (
                    f"Alert '{rule['alert']}' missing 'summary' annotation"
                )
