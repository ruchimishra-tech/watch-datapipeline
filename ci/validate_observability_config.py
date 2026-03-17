"""
Watch CI Observability Config Validation Gate — Phase 9.2.

Validates team-owned observability config files against central schemas.
Called as a CI step — exits non-zero if any file fails validation.

Validates:
  - SLO YAML files (schema/slo-schema.json)
  - Data contract YAML files (schema/contract-schema.json)
  - Alert rule YAML files (cli/validate_alerts.py logic)
  - Dashboard JSON files (cli/validate_dashboards.py logic)

Exit codes:
  0 — all files valid
  1 — one or more files failed validation

Usage:
    python ci/validate_observability_config.py \\
        --slo-dir slos/ \\
        --contract-dir contracts/ \\
        --alert-dir alerts/ \\
        --dashboard-dir dashboards/
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

import jsonschema
import yaml

logger = logging.getLogger("watch.ci.validate_obs_config")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# Locate schemas relative to this file (ci/ -> project root -> schema/)
_ROOT = Path(__file__).parents[1]
_SCHEMA_DIR = _ROOT / "schema"


# ── Schema loaders ─────────────────────────────────────────────────────────────

def _load_json_schema(name: str) -> dict:
    path = _SCHEMA_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


# ── Validators ─────────────────────────────────────────────────────────────────

class ValidationError:
    def __init__(self, file_path: str, message: str):
        self.file_path = file_path
        self.message = message

    def __str__(self) -> str:
        return f"{self.file_path}: {self.message}"


def validate_slo_files(slo_dir: Optional[Path]) -> list[ValidationError]:
    if not slo_dir or not slo_dir.exists():
        return []
    schema = _load_json_schema("slo-schema.json")
    errors = []
    for p in sorted(slo_dir.glob("*.yaml")) + sorted(slo_dir.glob("*.yml")):
        try:
            doc = yaml.safe_load(p.read_text(encoding="utf-8"))
            jsonschema.validate(instance=doc, schema=schema)
            logger.info("  [PASS] %s", p.name)
        except jsonschema.ValidationError as e:
            errors.append(ValidationError(str(p), e.message))
            logger.error("  [FAIL] %s: %s", p.name, e.message)
        except Exception as e:
            errors.append(ValidationError(str(p), str(e)))
            logger.error("  [FAIL] %s: %s", p.name, e)
    return errors


def validate_contract_files(contract_dir: Optional[Path]) -> list[ValidationError]:
    if not contract_dir or not contract_dir.exists():
        return []
    schema = _load_json_schema("contract-schema.json")
    errors = []
    for p in sorted(contract_dir.glob("*.yaml")) + sorted(contract_dir.glob("*.yml")):
        try:
            doc = yaml.safe_load(p.read_text(encoding="utf-8"))
            jsonschema.validate(instance=doc, schema=schema)
            logger.info("  [PASS] %s", p.name)
        except jsonschema.ValidationError as e:
            errors.append(ValidationError(str(p), e.message))
            logger.error("  [FAIL] %s: %s", p.name, e.message)
        except Exception as e:
            errors.append(ValidationError(str(p), str(e)))
            logger.error("  [FAIL] %s: %s", p.name, e)
    return errors


def validate_alert_files(alert_dir: Optional[Path]) -> list[ValidationError]:
    if not alert_dir or not alert_dir.exists():
        return []
    # Import the existing alert validator
    sys.path.insert(0, str(_ROOT))
    from cli.validate_alerts import validate_alert_file

    errors = []
    for p in sorted(alert_dir.glob("*.yaml")) + sorted(alert_dir.glob("*.yml")):
        try:
            alert_errors = validate_alert_file(p)
            if alert_errors:
                for ae in alert_errors:
                    errors.append(ValidationError(str(p), str(ae)))
                    logger.error("  [FAIL] %s: %s", p.name, ae)
            else:
                logger.info("  [PASS] %s", p.name)
        except Exception as e:
            errors.append(ValidationError(str(p), str(e)))
            logger.error("  [FAIL] %s: %s", p.name, e)
    return errors


def validate_dashboard_files(dashboard_dir: Optional[Path]) -> list[ValidationError]:
    if not dashboard_dir or not dashboard_dir.exists():
        return []
    sys.path.insert(0, str(_ROOT))
    from cli.validate_dashboards import validate_dashboard_file, DashboardError

    errors = []
    for p in sorted(dashboard_dir.glob("*.json")):
        try:
            validate_dashboard_file(str(p))
            logger.info("  [PASS] %s", p.name)
        except DashboardError as e:
            errors.append(ValidationError(str(p), str(e)))
            logger.error("  [FAIL] %s: %s", p.name, e)
        except Exception as e:
            errors.append(ValidationError(str(p), str(e)))
            logger.error("  [FAIL] %s: %s", p.name, e)
    return errors


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run_validation(
    slo_dir: Optional[Path] = None,
    contract_dir: Optional[Path] = None,
    alert_dir: Optional[Path] = None,
    dashboard_dir: Optional[Path] = None,
) -> list[ValidationError]:
    """
    Run all configured validators and return combined errors.
    """
    all_errors: list[ValidationError] = []

    if slo_dir:
        logger.info("Validating SLO files in %s ...", slo_dir)
        all_errors.extend(validate_slo_files(slo_dir))

    if contract_dir:
        logger.info("Validating contract files in %s ...", contract_dir)
        all_errors.extend(validate_contract_files(contract_dir))

    if alert_dir:
        logger.info("Validating alert files in %s ...", alert_dir)
        all_errors.extend(validate_alert_files(alert_dir))

    if dashboard_dir:
        logger.info("Validating dashboard files in %s ...", dashboard_dir)
        all_errors.extend(validate_dashboard_files(dashboard_dir))

    return all_errors


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Watch CI Observability Config Validation Gate"
    )
    parser.add_argument("--slo-dir", type=Path, default=None)
    parser.add_argument("--contract-dir", type=Path, default=None)
    parser.add_argument("--alert-dir", type=Path, default=None)
    parser.add_argument("--dashboard-dir", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    errors = run_validation(
        slo_dir=args.slo_dir,
        contract_dir=args.contract_dir,
        alert_dir=args.alert_dir,
        dashboard_dir=args.dashboard_dir,
    )
    if errors:
        logger.error("\n[watch-gate] VALIDATION FAILED (%d error(s)):", len(errors))
        for e in errors:
            logger.error("  %s", e)
        return 1
    logger.info("\n[watch-gate] All observability config files are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
