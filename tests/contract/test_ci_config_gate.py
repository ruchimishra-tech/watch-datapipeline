"""
Phase 9 Contract Tests — T9.5, T9.6

Offline tests for the CI observability config validation gate
(ci/validate_observability_config.py).

T9.5 — Invalid SLO YAML (missing target) causes CI exit non-zero
T9.6 — Alert YAML with unknown/invalid structure causes CI exit non-zero
"""
import json
from pathlib import Path

import pytest
import yaml


# ── Helpers ────────────────────────────────────────────────────────────────────

def _write(tmp_path: Path, filename: str, content) -> Path:
    p = tmp_path / filename
    if filename.endswith(".json"):
        p.write_text(json.dumps(content), encoding="utf-8")
    else:
        p.write_text(yaml.dump(content), encoding="utf-8")
    return p


def _valid_slo() -> dict:
    return {
        "pipeline_id": "test-pipeline",
        "owner": "team-test",
        "slos": {
            "freshness": {
                "sli": "pipeline:freshness_sli:good",
                "target": 99.5,
                "window": "28d",
            }
        },
        "error_budget": {"freeze_threshold_pct": 10},
    }


def _valid_alert() -> dict:
    return {
        "groups": [{
            "name": "watch.test",
            "rules": [{
                "alert": "TestAlert",
                "expr": 'increase(pipeline_runs_total{status="failed"}[5m]) > 0',
                "for": "0m",
                "labels": {
                    "severity": "critical",
                    "runbook_url": "https://wiki.internal/watch/runbooks/test",
                },
                "annotations": {
                    "summary": "Test {{ $labels.pipeline_id }}",
                    "description": "Test description.",
                },
            }]
        }]
    }


# ── T9.5: Invalid SLO exits non-zero ──────────────────────────────────────────

@pytest.mark.phase9
class TestT95InvalidSLOExitsNonZero:
    def test_missing_target_causes_failure(self, tmp_path):
        from ci.validate_observability_config import validate_slo_files
        doc = _valid_slo()
        del doc["slos"]["freshness"]["target"]
        _write(tmp_path, "bad-slo.yaml", doc)
        errors = validate_slo_files(tmp_path)
        assert len(errors) > 0

    def test_error_message_mentions_target(self, tmp_path):
        from ci.validate_observability_config import validate_slo_files
        doc = _valid_slo()
        del doc["slos"]["freshness"]["target"]
        _write(tmp_path, "bad-slo.yaml", doc)
        errors = validate_slo_files(tmp_path)
        assert any("target" in e.message for e in errors)

    def test_valid_slo_no_errors(self, tmp_path):
        from ci.validate_observability_config import validate_slo_files
        _write(tmp_path, "good-slo.yaml", _valid_slo())
        errors = validate_slo_files(tmp_path)
        assert errors == []

    def test_target_out_of_range_causes_failure(self, tmp_path):
        from ci.validate_observability_config import validate_slo_files
        doc = _valid_slo()
        doc["slos"]["freshness"]["target"] = 100.5
        _write(tmp_path, "bad-slo.yaml", doc)
        errors = validate_slo_files(tmp_path)
        assert len(errors) > 0

    def test_missing_pipeline_id_causes_failure(self, tmp_path):
        from ci.validate_observability_config import validate_slo_files
        doc = _valid_slo()
        del doc["pipeline_id"]
        _write(tmp_path, "bad-slo.yaml", doc)
        errors = validate_slo_files(tmp_path)
        assert len(errors) > 0

    def test_run_validation_orchestrator_returns_errors(self, tmp_path):
        from ci.validate_observability_config import run_validation
        doc = _valid_slo()
        del doc["slos"]["freshness"]["target"]
        _write(tmp_path, "bad-slo.yaml", doc)
        errors = run_validation(slo_dir=tmp_path)
        assert len(errors) > 0

    def test_run_validation_exit_code_is_1_on_errors(self, tmp_path):
        """Simulate CI exit code by checking errors list is non-empty."""
        from ci.validate_observability_config import run_validation
        doc = _valid_slo()
        del doc["slos"]["freshness"]["target"]
        _write(tmp_path, "bad-slo.yaml", doc)
        errors = run_validation(slo_dir=tmp_path)
        exit_code = 1 if errors else 0
        assert exit_code == 1


# ── T9.6: Invalid alert exits non-zero ────────────────────────────────────────

@pytest.mark.phase9
class TestT96InvalidAlertExitsNonZero:
    def test_missing_expr_causes_failure(self, tmp_path):
        from ci.validate_observability_config import validate_alert_files
        doc = _valid_alert()
        del doc["groups"][0]["rules"][0]["expr"]
        _write(tmp_path, "bad-alert.yaml", doc)
        errors = validate_alert_files(tmp_path)
        assert len(errors) > 0

    def test_missing_severity_label_causes_failure(self, tmp_path):
        from ci.validate_observability_config import validate_alert_files
        doc = _valid_alert()
        del doc["groups"][0]["rules"][0]["labels"]["severity"]
        _write(tmp_path, "bad-alert.yaml", doc)
        errors = validate_alert_files(tmp_path)
        assert len(errors) > 0

    def test_missing_runbook_url_causes_failure(self, tmp_path):
        from ci.validate_observability_config import validate_alert_files
        doc = _valid_alert()
        del doc["groups"][0]["rules"][0]["labels"]["runbook_url"]
        _write(tmp_path, "bad-alert.yaml", doc)
        errors = validate_alert_files(tmp_path)
        assert len(errors) > 0

    def test_valid_alert_no_errors(self, tmp_path):
        from ci.validate_observability_config import validate_alert_files
        _write(tmp_path, "good-alert.yaml", _valid_alert())
        errors = validate_alert_files(tmp_path)
        assert errors == []

    def test_invalid_severity_causes_failure(self, tmp_path):
        from ci.validate_observability_config import validate_alert_files
        doc = _valid_alert()
        doc["groups"][0]["rules"][0]["labels"]["severity"] = "super-critical"
        _write(tmp_path, "bad-alert.yaml", doc)
        errors = validate_alert_files(tmp_path)
        assert len(errors) > 0

    def test_mixed_valid_invalid_files(self, tmp_path):
        from ci.validate_observability_config import validate_alert_files
        _write(tmp_path, "good-alert.yaml", _valid_alert())
        bad = _valid_alert()
        del bad["groups"][0]["rules"][0]["expr"]
        _write(tmp_path, "bad-alert.yaml", bad)
        errors = validate_alert_files(tmp_path)
        assert len(errors) == 1
        assert "bad-alert.yaml" in errors[0].file_path
