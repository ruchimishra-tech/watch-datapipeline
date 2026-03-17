"""
Phase 8 Contract Tests — T8.4, T8.5

Offline validation of data contract YAML files against schema/contract-schema.json.

T8.4 — Valid data contract YAML passes contract-schema.json validation
T8.5 — Contract with consumer entry missing 'criticality' field fails validation
"""
import json
from pathlib import Path

import jsonschema
import pytest
import yaml

SCHEMA_DIR = Path(__file__).parents[2] / "schema"
CONTRACTS_DIR = Path(__file__).parents[2] / "contracts"


def _load_schema() -> dict:
    path = SCHEMA_DIR / "contract-schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _valid_contract() -> dict:
    return {
        "contract_id": "contract-test-v1",
        "pipeline_id": "test-pipeline",
        "owner": "team-test",
        "output_schema": {
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "value", "type": "float", "nullable": True},
            ]
        },
        "consumers": [
            {
                "team": "team-consumer-a",
                "criticality": "high",
            }
        ],
    }


def _validate(doc: dict, schema: dict):
    jsonschema.validate(instance=doc, schema=schema)


# ── T8.4: Valid contract passes ─────────────────────────────────────────────

@pytest.mark.phase8
class TestT84ValidContract:
    def test_valid_contract_passes(self):
        schema = _load_schema()
        _validate(_valid_contract(), schema)

    def test_real_customer360_contract_passes(self):
        path = CONTRACTS_DIR / "customer-360-v1.yaml"
        if not path.exists():
            pytest.skip("customer-360-v1.yaml not found")
        schema = _load_schema()
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        _validate(doc, schema)

    def test_contract_with_all_optional_fields_passes(self):
        schema = _load_schema()
        doc = _valid_contract()
        doc["version"] = "1.0.0"
        doc["volume"] = {"min_rows": 100, "max_rows": 50000}
        doc["freshness"] = {"max_age_hours": 4}
        doc["dq_rules"] = [
            {"rule": "null_rate", "column": "id", "max_failure_rate_pct": 0.0}
        ]
        doc["consumers"][0]["notification_channel"] = "#team-a-alerts"
        doc["consumers"][0]["use_cases"] = ["Risk scoring"]
        _validate(doc, schema)

    def test_multiple_consumers_pass(self):
        schema = _load_schema()
        doc = _valid_contract()
        doc["consumers"].append({
            "team": "team-consumer-b",
            "criticality": "critical",
        })
        _validate(doc, schema)

    def test_all_column_types_valid(self):
        schema = _load_schema()
        for col_type in ("string", "integer", "float", "boolean", "timestamp", "date", "json"):
            doc = _valid_contract()
            doc["output_schema"]["columns"][0]["type"] = col_type
            _validate(doc, schema)

    def test_all_criticality_values_valid(self):
        schema = _load_schema()
        for criticality in ("critical", "high", "medium", "low"):
            doc = _valid_contract()
            doc["consumers"][0]["criticality"] = criticality
            _validate(doc, schema)


# ── T8.5: Missing criticality fails ─────────────────────────────────────────

@pytest.mark.phase8
class TestT85MissingConsumerCriticality:
    def test_missing_criticality_raises(self):
        schema = _load_schema()
        doc = _valid_contract()
        del doc["consumers"][0]["criticality"]
        with pytest.raises(jsonschema.ValidationError) as exc_info:
            _validate(doc, schema)
        assert "criticality" in str(exc_info.value)

    def test_missing_consumer_team_raises(self):
        schema = _load_schema()
        doc = _valid_contract()
        del doc["consumers"][0]["team"]
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_invalid_criticality_raises(self):
        schema = _load_schema()
        doc = _valid_contract()
        doc["consumers"][0]["criticality"] = "super-critical"
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_empty_consumers_list_raises(self):
        schema = _load_schema()
        doc = _valid_contract()
        doc["consumers"] = []
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_missing_contract_id_raises(self):
        schema = _load_schema()
        doc = _valid_contract()
        del doc["contract_id"]
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)

    def test_invalid_column_type_raises(self):
        schema = _load_schema()
        doc = _valid_contract()
        doc["output_schema"]["columns"][0]["type"] = "bigint"
        with pytest.raises(jsonschema.ValidationError):
            _validate(doc, schema)
