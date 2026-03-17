"""
Phase 8 Contract Tests — T8.1, T8.2, T8.3

Offline validation of SLO YAML files against schema/slo-schema.json.

T8.1 — Valid SLO YAML passes slo-schema.json validation
T8.2 — SLO YAML missing 'target' field fails validation
T8.3 — SLO with target > 99.99 fails validation
"""
from pathlib import Path

import jsonschema
import pytest
import yaml

SCHEMA_DIR = Path(__file__).parents[2] / "schema"
SLO_DIR = Path(__file__).parents[2] / "slos"


def _load_schema() -> dict:
    path = SCHEMA_DIR / "slo-schema.json"
    import json
    return json.loads(path.read_text(encoding="utf-8"))


def _valid_slo() -> dict:
    return {
        "pipeline_id": "test-pipeline",
        "owner": "team-test",
        "slos": {
            "freshness": {
                "sli": "pipeline:freshness_sli:good",
                "target": 99.5,
                "window": "28d",
                "sla_seconds": 7200,
            }
        },
        "error_budget": {
            "freeze_threshold_pct": 10,
        },
    }


def _validate(doc: dict, schema: dict):
    """Validate doc against schema. Raises jsonschema.ValidationError on failure."""
    jsonschema.validate(instance=doc, schema=schema)


# ── T8.1: Valid SLO passes ─────────────────────────────────────────────────

@pytest.mark.phase8
class TestT81ValidSLO:
    def test_valid_slo_passes(self):
        schema = _load_schema()
        _validate(_valid_slo(), schema)  # must not raise

    def test_real_customer360_slo_passes(self):
        path = SLO_DIR / "customer-360.yaml"
        if not path.exists():
            pytest.skip("customer-360.yaml not found")
        schema = _load_schema()
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        _validate(doc, schema)

    def test_valid_slo_with_optional_fields(self):
        schema = _load_schema()
        doc = _valid_slo()
        doc["slos"]["freshness"]["description"] = "Data freshness SLO"
        doc["error_budget"]["alert_fast_burn_factor"] = 14.4
        doc["error_budget"]["alert_slow_burn_factor"] = 3.0
        _validate(doc, schema)

    def test_multiple_slos_valid(self):
        schema = _load_schema()
        doc = _valid_slo()
        doc["slos"]["success_rate"] = {
            "sli": "pipeline:success_rate_sli:good",
            "target": 99.0,
            "window": "28d",
        }
        _validate(doc, schema)


# ── T8.2: Missing target fails ─────────────────────────────────────────────

@pytest.mark.phase8
class TestT82MissingTarget:
    def test_missing_target_raises(self):
        schema = _load_schema()
        doc = _valid_slo()
        del doc["slos"]["freshness"]["target"]
        with pytest.raises(jsonschema.ValidationError) as exc_info:
            _validate(doc, schema)
        assert "target" in str(exc_info.value)

    def test_missing_sli_raises(self):
        schema = _load_schema()
        doc = _valid_slo()
        del doc["slos"]["freshness"]["sli"]
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_missing_pipeline_id_raises(self):
        schema = _load_schema()
        doc = _valid_slo()
        del doc["pipeline_id"]
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_missing_error_budget_raises(self):
        schema = _load_schema()
        doc = _valid_slo()
        del doc["error_budget"]
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)


# ── T8.3: Target out of range fails ────────────────────────────────────────

@pytest.mark.phase8
class TestT83TargetOutOfRange:
    def test_target_100_raises(self):
        schema = _load_schema()
        doc = _valid_slo()
        doc["slos"]["freshness"]["target"] = 100.0
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_target_100_1_raises(self):
        schema = _load_schema()
        doc = _valid_slo()
        doc["slos"]["freshness"]["target"] = 100.1
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_target_99_99_passes(self):
        schema = _load_schema()
        doc = _valid_slo()
        doc["slos"]["freshness"]["target"] = 99.99
        _validate(doc, schema)

    def test_target_negative_raises(self):
        schema = _load_schema()
        doc = _valid_slo()
        doc["slos"]["freshness"]["target"] = -1.0
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)
