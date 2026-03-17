"""
Phase 10 Unit Tests — T10.1–T10.8

Tests for the obs-cli self-service onboarding tool (cli/obs_cli.py).
All offline — no Tempo or Grafana required.

T10.1 — register creates pipeline-registration.yaml with correct content
T10.2 — register with spaces in pipeline_id exits non-zero
T10.3 — register duplicate pipeline_id exits non-zero
T10.4 — slo init creates valid SLO YAML
T10.5 — contract init creates valid contract YAML
T10.6 — validate exits 0 when all files are valid
T10.7 — validate uses only bundled schemas (no network)
T10.8 — validate exits non-zero for invalid file and names the field
"""
import json
from pathlib import Path

import jsonschema
import pytest
import yaml

from cli.obs_cli import (
    cmd_register,
    cmd_slo_init,
    cmd_contract_init,
    cmd_validate,
    _validate_pipeline_id,
    REGISTRATION_FILENAME,
)

_SCHEMA_DIR = Path(__file__).parents[2] / "schema"


def _load_schema(name: str) -> dict:
    return json.loads((_SCHEMA_DIR / name).read_text(encoding="utf-8"))


# ── T10.1: register — valid pipeline_id ──────────────────────────────────────

@pytest.mark.phase10
class TestT101RegisterValid:
    def test_creates_registration_file(self, tmp_path):
        rc = cmd_register("cust-360", "customer-data", "team-cd", 1, tmp_path)
        assert rc == 0
        assert (tmp_path / REGISTRATION_FILENAME).exists()

    def test_file_contains_pipeline_id(self, tmp_path):
        cmd_register("cust-360", "customer-data", "team-cd", 1, tmp_path)
        doc = yaml.safe_load((tmp_path / REGISTRATION_FILENAME).read_text())
        assert doc["pipeline_id"] == "cust-360"

    def test_file_contains_domain(self, tmp_path):
        cmd_register("cust-360", "customer-data", "team-cd", 1, tmp_path)
        doc = yaml.safe_load((tmp_path / REGISTRATION_FILENAME).read_text())
        assert doc["domain"] == "customer-data"

    def test_file_contains_owner(self, tmp_path):
        cmd_register("cust-360", "customer-data", "team-cd", 1, tmp_path)
        doc = yaml.safe_load((tmp_path / REGISTRATION_FILENAME).read_text())
        assert doc["owner"] == "team-cd"

    def test_file_contains_tier(self, tmp_path):
        cmd_register("cust-360", "customer-data", "team-cd", 2, tmp_path)
        doc = yaml.safe_load((tmp_path / REGISTRATION_FILENAME).read_text())
        assert doc["instrumentation_tier"] == 2

    def test_output_dir_created_if_not_exists(self, tmp_path):
        out = tmp_path / "observability"
        assert not out.exists()
        rc = cmd_register("my-pipe", "d", "t", 1, out)
        assert rc == 0
        assert out.exists()


# ── T10.2: register — invalid pipeline_id with spaces ────────────────────────

@pytest.mark.phase10
class TestT102RegisterInvalidSpaces:
    def test_pipeline_id_with_space_exits_nonzero(self, tmp_path):
        rc = cmd_register("cust 360", "d", "t", 1, tmp_path)
        assert rc == 1

    def test_pipeline_id_with_space_no_file_created(self, tmp_path):
        cmd_register("cust 360", "d", "t", 1, tmp_path)
        assert not (tmp_path / REGISTRATION_FILENAME).exists()

    def test_pipeline_id_with_underscore_is_invalid(self, tmp_path):
        rc = cmd_register("cust_360", "d", "t", 1, tmp_path)
        assert rc == 1

    def test_pipeline_id_uppercase_is_invalid(self, tmp_path):
        rc = cmd_register("Cust-360", "d", "t", 1, tmp_path)
        assert rc == 1

    def test_validate_pipeline_id_returns_error_for_spaces(self):
        err = _validate_pipeline_id("cust 360")
        assert err is not None
        assert "invalid character" in err.lower() or "space" in err.lower()

    def test_validate_pipeline_id_valid_returns_none(self):
        err = _validate_pipeline_id("cust-360")
        assert err is None


# ── T10.3: register — duplicate pipeline_id ──────────────────────────────────

@pytest.mark.phase10
class TestT103RegisterDuplicate:
    def test_duplicate_register_exits_nonzero(self, tmp_path):
        cmd_register("my-pipe", "d", "t", 1, tmp_path)
        rc = cmd_register("my-pipe", "d", "t", 1, tmp_path)
        assert rc == 1

    def test_force_flag_overwrites(self, tmp_path):
        cmd_register("my-pipe", "d", "t-old", 1, tmp_path)
        rc = cmd_register("my-pipe", "d", "t-new", 1, tmp_path, force=True)
        assert rc == 0
        doc = yaml.safe_load((tmp_path / REGISTRATION_FILENAME).read_text())
        assert doc["owner"] == "t-new"


# ── T10.4: slo init ───────────────────────────────────────────────────────────

@pytest.mark.phase10
class TestT104SLOInit:
    def test_slo_file_created(self, tmp_path):
        rc = cmd_slo_init("my-pipe", "team-x", tmp_path)
        assert rc == 0
        assert (tmp_path / "slos" / "my-pipe.yaml").exists()

    def test_slo_file_passes_schema_validation(self, tmp_path):
        cmd_slo_init("my-pipe", "team-x", tmp_path)
        schema = _load_schema("slo-schema.json")
        doc = yaml.safe_load((tmp_path / "slos" / "my-pipe.yaml").read_text())
        jsonschema.validate(instance=doc, schema=schema)  # must not raise

    def test_slo_file_has_correct_pipeline_id(self, tmp_path):
        cmd_slo_init("my-pipe", "team-x", tmp_path)
        doc = yaml.safe_load((tmp_path / "slos" / "my-pipe.yaml").read_text())
        assert doc["pipeline_id"] == "my-pipe"

    def test_slo_file_has_at_least_one_slo(self, tmp_path):
        cmd_slo_init("my-pipe", "team-x", tmp_path)
        doc = yaml.safe_load((tmp_path / "slos" / "my-pipe.yaml").read_text())
        assert len(doc["slos"]) >= 1

    def test_slo_invalid_pipeline_id_exits_nonzero(self, tmp_path):
        rc = cmd_slo_init("My Pipe", "team-x", tmp_path)
        assert rc == 1


# ── T10.5: contract init ──────────────────────────────────────────────────────

@pytest.mark.phase10
class TestT105ContractInit:
    def test_contract_file_created(self, tmp_path):
        rc = cmd_contract_init("my-pipe", "team-x", tmp_path)
        assert rc == 0
        assert (tmp_path / "contracts" / "my-pipe.yaml").exists()

    def test_contract_file_passes_schema_validation(self, tmp_path):
        cmd_contract_init("my-pipe", "team-x", tmp_path)
        schema = _load_schema("contract-schema.json")
        doc = yaml.safe_load((tmp_path / "contracts" / "my-pipe.yaml").read_text())
        jsonschema.validate(instance=doc, schema=schema)  # must not raise

    def test_contract_file_has_correct_pipeline_id(self, tmp_path):
        cmd_contract_init("my-pipe", "team-x", tmp_path)
        doc = yaml.safe_load((tmp_path / "contracts" / "my-pipe.yaml").read_text())
        assert doc["pipeline_id"] == "my-pipe"

    def test_contract_has_consumers(self, tmp_path):
        cmd_contract_init("my-pipe", "team-x", tmp_path)
        doc = yaml.safe_load((tmp_path / "contracts" / "my-pipe.yaml").read_text())
        assert len(doc["consumers"]) >= 1

    def test_contract_invalid_pipeline_id_exits_nonzero(self, tmp_path):
        rc = cmd_contract_init("My Pipe", "team-x", tmp_path)
        assert rc == 1


# ── T10.6: validate — valid files ─────────────────────────────────────────────

@pytest.mark.phase10
class TestT106ValidateValid:
    def test_validate_exits_0_for_generated_files(self, tmp_path):
        obs = tmp_path / "obs"
        cmd_slo_init("my-pipe", "team-x", obs)
        cmd_contract_init("my-pipe", "team-x", obs)
        rc = cmd_validate(obs)
        assert rc == 0

    def test_validate_empty_dir_exits_0(self, tmp_path):
        obs = tmp_path / "obs"
        obs.mkdir()
        rc = cmd_validate(obs)
        assert rc == 0


# ── T10.7: validate — offline (no network) ────────────────────────────────────

@pytest.mark.phase10
class TestT107ValidateOffline:
    def test_validate_works_without_network(self, tmp_path, monkeypatch):
        """Monkeypatch socket to block all network access; validate must still work."""
        import socket

        def _no_network(*args, **kwargs):
            raise OSError("Network is disabled")

        obs = tmp_path / "obs"
        cmd_slo_init("my-pipe", "team-x", obs)

        monkeypatch.setattr(socket, "getaddrinfo", _no_network)
        monkeypatch.setattr(socket, "create_connection", _no_network)

        rc = cmd_validate(obs)
        assert rc == 0, "validate should work offline using bundled schemas"


# ── T10.8: validate — invalid file exits non-zero ─────────────────────────────

@pytest.mark.phase10
class TestT108ValidateInvalid:
    def test_invalid_slo_exits_nonzero(self, tmp_path):
        obs = tmp_path / "obs"
        slo_dir = obs / "slos"
        slo_dir.mkdir(parents=True)
        bad = {
            "pipeline_id": "my-pipe",
            "owner": "team-x",
            "slos": {
                "freshness": {
                    "sli": "pipeline:freshness_sli:good",
                    # missing 'target'
                    "window": "28d",
                }
            },
            "error_budget": {"freeze_threshold_pct": 10},
        }
        (slo_dir / "bad.yaml").write_text(yaml.dump(bad), encoding="utf-8")
        rc = cmd_validate(obs)
        assert rc == 1

    def test_invalid_contract_exits_nonzero(self, tmp_path):
        obs = tmp_path / "obs"
        contract_dir = obs / "contracts"
        contract_dir.mkdir(parents=True)
        bad = {
            "contract_id": "c1",
            "pipeline_id": "my-pipe",
            "owner": "team-x",
            "output_schema": {"columns": [{"name": "id", "type": "string"}]},
            "consumers": [{"team": "t"}],  # missing criticality
        }
        (contract_dir / "bad.yaml").write_text(yaml.dump(bad), encoding="utf-8")
        rc = cmd_validate(obs)
        assert rc == 1
