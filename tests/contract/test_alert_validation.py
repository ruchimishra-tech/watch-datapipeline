"""
Phase 4 Contract Tests — T4.3, T4.4

Tests for the alert rule CI validator (cli/validate_alerts.py).
All run offline — no Prometheus or promtool required.

T4.3 — invalid PromQL structure (missing expr) detected
T4.4 — missing required labels detected
"""
import tempfile
from pathlib import Path

import pytest
import yaml

from cli.validate_alerts import validate_alert_file, AlertError, REQUIRED_LABELS

# ── Helpers ───────────────────────────────────────────────────────────────────

def _write_yaml(tmp_path: Path, data: dict, filename: str = "alert.yaml") -> Path:
    p = tmp_path / filename
    p.write_text(yaml.dump(data), encoding="utf-8")
    return p


def _make_valid_rule(
    alert="WatchTestAlert",
    expr='increase(pipeline_runs_total{status="failed"}[5m]) > 0',
    severity="critical",
    runbook_url="https://wiki.internal/watch/runbooks/test",
) -> dict:
    return {
        "groups": [{
            "name": "watch.test",
            "rules": [{
                "alert": alert,
                "expr": expr,
                "for": "0m",
                "labels": {
                    "severity": severity,
                    "runbook_url": runbook_url,
                },
                "annotations": {
                    "summary": "Test alert {{ $labels.pipeline_id }}",
                    "description": "Test description.",
                },
            }]
        }]
    }


# ── T4.3: invalid PromQL (missing expr) ───────────────────────────────────────

@pytest.mark.phase4
class TestT43InvalidPromQL:
    """T4.3 — CI exits non-zero when a rule has no 'expr' field."""

    def test_missing_expr_raises_error(self, tmp_path):
        doc = _make_valid_rule()
        del doc["groups"][0]["rules"][0]["expr"]
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        rules = [e.rule for e in errors]
        assert "missing-expr" in rules, f"Expected missing-expr; got: {rules}"

    def test_error_message_identifies_alert(self, tmp_path):
        doc = _make_valid_rule(alert="BrokenAlert")
        del doc["groups"][0]["rules"][0]["expr"]
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        missing = [e for e in errors if e.rule == "missing-expr"]
        assert any("BrokenAlert" in e.detail for e in missing), (
            "Error should name the alert with missing expr"
        )

    def test_missing_alert_name_raises_error(self, tmp_path):
        doc = _make_valid_rule()
        del doc["groups"][0]["rules"][0]["alert"]
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        rules = [e.rule for e in errors]
        assert "missing-alert-name" in rules

    def test_valid_rule_no_expr_error(self, tmp_path):
        doc = _make_valid_rule()
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        expr_errors = [e for e in errors if e.rule == "missing-expr"]
        assert expr_errors == [], f"Valid rule should have no expr errors; got {expr_errors}"

    def test_invalid_yaml_returns_error(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("{not: valid: yaml: ::", encoding="utf-8")

        errors = validate_alert_file(p)
        rules = [e.rule for e in errors]
        assert "invalid-yaml" in rules

    def test_missing_groups_key_returns_error(self, tmp_path):
        p = _write_yaml(tmp_path, {"rules": []})
        errors = validate_alert_file(p)
        rules = [e.rule for e in errors]
        assert "invalid-structure" in rules


# ── T4.4: missing required labels ─────────────────────────────────────────────

@pytest.mark.phase4
class TestT44MissingRequiredLabels:
    """T4.4 — CI exits non-zero when alert rule is missing required labels."""

    @pytest.mark.parametrize("missing_label", list(REQUIRED_LABELS))
    def test_missing_label_raises_error(self, tmp_path, missing_label):
        doc = _make_valid_rule()
        del doc["groups"][0]["rules"][0]["labels"][missing_label]
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        rules = [e.rule for e in errors]
        assert "missing-required-label" in rules, (
            f"Missing '{missing_label}' should raise missing-required-label; got: {rules}"
        )

    @pytest.mark.parametrize("missing_label", list(REQUIRED_LABELS))
    def test_error_message_names_missing_label(self, tmp_path, missing_label):
        doc = _make_valid_rule()
        del doc["groups"][0]["rules"][0]["labels"][missing_label]
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        label_errors = [e for e in errors if e.rule == "missing-required-label"]
        assert any(missing_label in e.detail for e in label_errors), (
            f"Error detail should name the missing label '{missing_label}'"
        )

    def test_no_labels_section_raises_all_required_errors(self, tmp_path):
        doc = _make_valid_rule()
        doc["groups"][0]["rules"][0]["labels"] = {}
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        label_errors = [e for e in errors if e.rule == "missing-required-label"]
        missing_names = {e.detail for e in label_errors}
        for label in REQUIRED_LABELS:
            assert any(label in d for d in missing_names), (
                f"Should report missing '{label}'; got: {missing_names}"
            )

    def test_invalid_severity_raises_error(self, tmp_path):
        doc = _make_valid_rule(severity="ultra-critical")
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        rules = [e.rule for e in errors]
        assert "invalid-severity" in rules

    def test_runbook_url_without_http_raises_error(self, tmp_path):
        doc = _make_valid_rule(runbook_url="wiki/runbooks/test")
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        rules = [e.rule for e in errors]
        assert "invalid-runbook-url" in rules

    def test_valid_rule_passes_all_label_checks(self, tmp_path):
        doc = _make_valid_rule()
        path = _write_yaml(tmp_path, doc)

        errors = validate_alert_file(path)
        assert errors == [], f"Valid rule should have no errors; got: {errors}"


# ── Smoke: the real alert file we ship passes validation ─────────────────────

@pytest.mark.phase4
class TestRealAlertFilesPassValidation:
    def _repo_root(self) -> Path:
        return Path(__file__).parents[2]

    def test_pipeline_alerts_passes(self):
        path = self._repo_root() / "alerts" / "pipeline-alerts.yaml"
        if not path.exists():
            pytest.skip("pipeline-alerts.yaml not found")
        errors = validate_alert_file(path)
        assert errors == [], f"pipeline-alerts.yaml has errors: {[str(e) for e in errors]}"
