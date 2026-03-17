"""
Phase 3 Contract Tests — T3.5, T3.6, T3.7

Tests for the dashboard CI validator (cli/validate_dashboards.py).
All tests run offline — no Grafana instance required.

T3.5 — missing mandatory panel detected
T3.6 — unknown datasource UID detected
T3.7 — missing $environment template variable detected
"""
import json
import tempfile
from pathlib import Path

import pytest

from cli.validate_dashboards import validate_dashboard, ValidationError, KNOWN_DATASOURCE_UIDS

# ── Minimal valid dashboard fixture ───────────────────────────────────────────

def _make_valid_dashboard(extra_panels: list | None = None) -> dict:
    """Returns a minimal dashboard that passes all three rules."""
    panels = [
        {"uid": "panel-alpha", "type": "stat", "datasource": {"uid": "prometheus-dev"}},
        {"uid": "panel-beta",  "type": "table", "datasource": {"uid": "prometheus-dev"}},
    ]
    if extra_panels:
        panels.extend(extra_panels)
    return {
        "__watch_meta": {
            "tier": "L1",
            "mandatory_panels": ["panel-alpha", "panel-beta"],
        },
        "uid": "test-dashboard",
        "title": "Test Dashboard",
        "schemaVersion": 38,
        "templating": {
            "list": [
                {
                    "name": "environment",
                    "type": "custom",
                    "options": [{"text": "dev", "value": "dev", "selected": True}],
                }
            ]
        },
        "panels": panels,
    }


def _write_json(tmp_path: Path, data: dict, filename: str = "dash.json") -> Path:
    p = tmp_path / filename
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


# ── T3.5: missing mandatory panel ─────────────────────────────────────────────

@pytest.mark.phase3
class TestT35MissingMandatoryPanel:
    """T3.5 — CI exits non-zero and names the missing panel."""

    def test_missing_one_panel_raises_error(self, tmp_path):
        dash = _make_valid_dashboard()
        # Remove panel-beta from the panels list but keep it in mandatory_panels
        dash["panels"] = [p for p in dash["panels"] if p["uid"] != "panel-beta"]

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        rules = [e.rule for e in errors]
        assert "missing-mandatory-panel" in rules, (
            f"Expected missing-mandatory-panel error; got: {rules}"
        )

    def test_error_message_names_missing_uid(self, tmp_path):
        dash = _make_valid_dashboard()
        dash["panels"] = [p for p in dash["panels"] if p["uid"] != "panel-beta"]

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        missing_errors = [e for e in errors if e.rule == "missing-mandatory-panel"]
        assert any("panel-beta" in e.detail for e in missing_errors), (
            "Error detail should mention the missing panel uid 'panel-beta'"
        )

    def test_multiple_missing_panels_each_reported(self, tmp_path):
        dash = _make_valid_dashboard()
        dash["panels"] = []  # remove all panels

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        missing = [e for e in errors if e.rule == "missing-mandatory-panel"]
        assert len(missing) == 2, (
            f"Expected 2 missing-mandatory-panel errors, got {len(missing)}"
        )

    def test_valid_dashboard_no_panel_errors(self, tmp_path):
        dash = _make_valid_dashboard()
        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        panel_errors = [e for e in errors if e.rule == "missing-mandatory-panel"]
        assert panel_errors == [], f"Valid dashboard should have no panel errors; got {panel_errors}"

    def test_no_mandatory_panels_in_meta_is_valid(self, tmp_path):
        dash = _make_valid_dashboard()
        dash["__watch_meta"]["mandatory_panels"] = []
        dash["panels"] = []  # no panels, nothing mandatory

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        panel_errors = [e for e in errors if e.rule == "missing-mandatory-panel"]
        assert panel_errors == []


# ── T3.6: unknown datasource UID ──────────────────────────────────────────────

@pytest.mark.phase3
class TestT36UnknownDatasource:
    """T3.6 — CI exits non-zero when a panel references an unregistered datasource."""

    def test_unknown_datasource_raises_error(self, tmp_path):
        dash = _make_valid_dashboard()
        dash["panels"].append({
            "uid": "rogue-panel",
            "type": "stat",
            "datasource": {"uid": "influxdb-secret"},  # not in KNOWN_DATASOURCE_UIDS
        })

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        rules = [e.rule for e in errors]
        assert "unknown-datasource" in rules, (
            f"Expected unknown-datasource error; got: {rules}"
        )

    def test_error_message_names_bad_uid(self, tmp_path):
        bad_uid = "influxdb-secret"
        dash = _make_valid_dashboard()
        dash["panels"].append({
            "uid": "rogue-panel",
            "type": "stat",
            "datasource": {"uid": bad_uid},
        })

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        ds_errors = [e for e in errors if e.rule == "unknown-datasource"]
        assert any(bad_uid in e.detail for e in ds_errors), (
            f"Error detail should mention the bad uid '{bad_uid}'"
        )

    def test_known_datasource_uids_all_pass(self, tmp_path):
        for uid in KNOWN_DATASOURCE_UIDS:
            dash = _make_valid_dashboard()
            dash["panels"][0]["datasource"] = {"uid": uid}
            path = _write_json(tmp_path, dash, filename=f"{uid}.json")
            errors = validate_dashboard(path)
            ds_errors = [e for e in errors if e.rule == "unknown-datasource"]
            assert ds_errors == [], (
                f"Known uid '{uid}' should not raise unknown-datasource error"
            )

    def test_datasource_in_nested_target_is_checked(self, tmp_path):
        """Datasource refs inside panel.targets[] must also be validated."""
        bad_uid = "clickhouse-prod"
        dash = _make_valid_dashboard()
        dash["panels"][0]["targets"] = [
            {"datasource": {"uid": bad_uid}, "refId": "A"}
        ]

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        ds_errors = [e for e in errors if e.rule == "unknown-datasource"]
        assert any(bad_uid in e.detail for e in ds_errors), (
            "Datasource refs inside targets[] should be validated"
        )

    def test_string_datasource_ref_checked(self, tmp_path):
        """Old-style string datasource refs (not dicts) should also be validated."""
        bad_uid = "old-style-unknown"
        dash = _make_valid_dashboard()
        dash["panels"][0]["datasource"] = bad_uid  # string ref

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        ds_errors = [e for e in errors if e.rule == "unknown-datasource"]
        assert any(bad_uid in e.detail for e in ds_errors), (
            "String-style datasource refs should be validated"
        )


# ── T3.7: missing $environment template variable ───────────────────────────────

@pytest.mark.phase3
class TestT37MissingEnvironmentVariable:
    """T3.7 — CI exits non-zero when $environment variable is absent."""

    def test_missing_environment_variable_raises_error(self, tmp_path):
        dash = _make_valid_dashboard()
        dash["templating"]["list"] = []  # remove all variables

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        rules = [e.rule for e in errors]
        assert "missing-required-variable" in rules, (
            f"Expected missing-required-variable error; got: {rules}"
        )

    def test_error_message_names_environment(self, tmp_path):
        dash = _make_valid_dashboard()
        dash["templating"]["list"] = []

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        var_errors = [e for e in errors if e.rule == "missing-required-variable"]
        assert any("environment" in e.detail for e in var_errors), (
            "Error detail should mention 'environment'"
        )

    def test_wrong_variable_name_fails(self, tmp_path):
        dash = _make_valid_dashboard()
        dash["templating"]["list"] = [{"name": "env", "type": "custom"}]  # typo

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        rules = [e.rule for e in errors]
        assert "missing-required-variable" in rules, (
            "Variable named 'env' should not satisfy the 'environment' requirement"
        )

    def test_correct_environment_variable_passes(self, tmp_path):
        dash = _make_valid_dashboard()  # already has $environment
        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        var_errors = [e for e in errors if e.rule == "missing-required-variable"]
        assert var_errors == [], (
            f"Dashboard with $environment should have no variable errors; got {var_errors}"
        )

    def test_no_templating_section_fails(self, tmp_path):
        dash = _make_valid_dashboard()
        del dash["templating"]

        path = _write_json(tmp_path, dash)
        errors = validate_dashboard(path)

        rules = [e.rule for e in errors]
        assert "missing-required-variable" in rules

    def test_invalid_json_returns_json_error(self, tmp_path):
        path = tmp_path / "broken.json"
        path.write_text("{not valid json", encoding="utf-8")

        errors = validate_dashboard(path)
        rules = [e.rule for e in errors]
        assert "invalid-json" in rules, (
            "Broken JSON file should return invalid-json error"
        )


# ── Integration: run against the real dashboards ──────────────────────────────

@pytest.mark.phase3
class TestRealDashboardsPassValidation:
    """Smoke test — the dashboards we ship must pass the validator."""

    def _repo_root(self) -> Path:
        return Path(__file__).parents[2]

    def test_l1_estate_passes(self):
        path = self._repo_root() / "dashboards" / "l1-estate.json"
        if not path.exists():
            pytest.skip("l1-estate.json not found")
        errors = validate_dashboard(path)
        assert errors == [], f"l1-estate.json has validation errors: {errors}"

    def test_l2_pipeline_passes(self):
        path = self._repo_root() / "dashboards" / "l2-pipeline.json"
        if not path.exists():
            pytest.skip("l2-pipeline.json not found")
        errors = validate_dashboard(path)
        assert errors == [], f"l2-pipeline.json has validation errors: {errors}"
