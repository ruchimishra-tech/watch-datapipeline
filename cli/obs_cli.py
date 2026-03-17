"""
Watch Self-Service Onboarding CLI — Phase 10.

obs-cli: helps pipeline teams register, generate skeleton config,
validate it offline, and verify spans are flowing.

Commands:
  pipeline register  — create pipeline-registration.yaml
  slo init           — generate skeleton SLO YAML
  contract init      — generate skeleton data contract YAML
  validate           — validate all observability/ files offline
  verify             — query Tempo to assert spans are flowing

All I/O is injectable for testing (output_dir, stdout sink).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

import yaml
import jsonschema

logger = logging.getLogger("watch.obs_cli")

# Central schemas are bundled next to this file's repo root
_ROOT = Path(__file__).parents[1]
_SCHEMA_DIR = _ROOT / "schema"

PIPELINE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9\-]{0,62}[a-z0-9]$")
REGISTRATION_FILENAME = "pipeline-registration.yaml"


# ── Validation helpers ─────────────────────────────────────────────────────────

def _validate_pipeline_id(pipeline_id: str) -> Optional[str]:
    """Returns error message if invalid, else None."""
    if not pipeline_id:
        return "pipeline_id must not be empty"
    if " " in pipeline_id or "\t" in pipeline_id:
        return f"pipeline_id '{pipeline_id}' contains invalid character (spaces are not allowed)"
    if not PIPELINE_ID_PATTERN.match(pipeline_id):
        return (
            f"pipeline_id '{pipeline_id}' is invalid: must be lowercase alphanumeric "
            f"and hyphens, start and end with alphanumeric, max 64 chars"
        )
    return None


def _load_json_schema(name: str) -> dict:
    p = _SCHEMA_DIR / name
    return json.loads(p.read_text(encoding="utf-8"))


# ── register ──────────────────────────────────────────────────────────────────

def cmd_register(
    pipeline_id: str,
    domain: str,
    owner: str,
    tier: int,
    output_dir: Path,
    force: bool = False,
) -> int:
    """
    Create pipeline-registration.yaml in output_dir.
    Returns exit code (0=ok, 1=error).
    """
    err = _validate_pipeline_id(pipeline_id)
    if err:
        logger.error("[obs-cli] register failed: %s", err)
        return 1

    out_path = output_dir / REGISTRATION_FILENAME
    if out_path.exists() and not force:
        existing = yaml.safe_load(out_path.read_text(encoding="utf-8"))
        if existing.get("pipeline_id") == pipeline_id:
            logger.error(
                "[obs-cli] pipeline '%s' is already registered at %s",
                pipeline_id, out_path
            )
            return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    doc = {
        "pipeline_id": pipeline_id,
        "domain": domain,
        "owner": owner,
        "instrumentation_tier": tier,
    }
    out_path.write_text(yaml.dump(doc, default_flow_style=False), encoding="utf-8")
    logger.info("[obs-cli] Registered pipeline '%s' -> %s", pipeline_id, out_path)
    return 0


# ── slo init ──────────────────────────────────────────────────────────────────

def cmd_slo_init(
    pipeline_id: str,
    owner: str,
    output_dir: Path,
) -> int:
    """Generate a skeleton SLO YAML that passes slo-schema.json validation."""
    err = _validate_pipeline_id(pipeline_id)
    if err:
        logger.error("[obs-cli] slo init failed: %s", err)
        return 1

    slo_dir = output_dir / "slos"
    slo_dir.mkdir(parents=True, exist_ok=True)
    out_path = slo_dir / f"{pipeline_id}.yaml"

    doc = {
        "pipeline_id": pipeline_id,
        "owner": owner,
        "slos": {
            "freshness": {
                "sli": "pipeline:freshness_sli:good",
                "target": 99.5,
                "window": "28d",
                "sla_seconds": 7200,
                "description": "Data must arrive within 2 hours of scheduled trigger.",
            },
            "success_rate": {
                "sli": "pipeline:success_rate_sli:good",
                "target": 99.0,
                "window": "28d",
                "description": "At least 99% of runs must succeed.",
            },
        },
        "error_budget": {
            "freeze_threshold_pct": 10,
        },
    }
    out_path.write_text(yaml.dump(doc, default_flow_style=False), encoding="utf-8")
    logger.info("[obs-cli] Generated SLO skeleton -> %s", out_path)
    return 0


# ── contract init ─────────────────────────────────────────────────────────────

def cmd_contract_init(
    pipeline_id: str,
    owner: str,
    output_dir: Path,
) -> int:
    """Generate a skeleton data contract YAML that passes contract-schema.json validation."""
    err = _validate_pipeline_id(pipeline_id)
    if err:
        logger.error("[obs-cli] contract init failed: %s", err)
        return 1

    contract_dir = output_dir / "contracts"
    contract_dir.mkdir(parents=True, exist_ok=True)
    out_path = contract_dir / f"{pipeline_id}.yaml"

    doc = {
        "contract_id": f"contract-{pipeline_id}-v1",
        "pipeline_id": pipeline_id,
        "owner": owner,
        "version": "1.0.0",
        "output_schema": {
            "columns": [
                {
                    "name": "id",
                    "type": "string",
                    "nullable": False,
                    "evolution_policy": "locked",
                    "description": "Primary key — never remove or change type.",
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "nullable": False,
                    "evolution_policy": "additive_only",
                },
            ]
        },
        "volume": {
            "min_rows": 1,
            "max_rows": 10000000,
        },
        "freshness": {
            "max_age_hours": 4,
        },
        "consumers": [
            {
                "team": "team-placeholder",
                "criticality": "medium",
                "notification_channel": "#team-placeholder-alerts",
            }
        ],
    }
    out_path.write_text(yaml.dump(doc, default_flow_style=False), encoding="utf-8")
    logger.info("[obs-cli] Generated contract skeleton -> %s", out_path)
    return 0


# ── validate ──────────────────────────────────────────────────────────────────

def cmd_validate(obs_dir: Path) -> int:
    """
    Validate all files in obs_dir against central schemas.
    Entirely offline — uses bundled schemas in schema/.
    Returns exit code.
    """
    slo_schema = _load_json_schema("slo-schema.json")
    contract_schema = _load_json_schema("contract-schema.json")

    errors: list[str] = []
    files_checked = 0

    def _check(path: Path, schema: dict) -> None:
        nonlocal files_checked
        files_checked += 1
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
            jsonschema.validate(instance=doc, schema=schema)
            logger.info("  [PASS] %s", path.relative_to(obs_dir))
        except jsonschema.ValidationError as e:
            errors.append(f"{path.name}: {e.message}")
            logger.error("  [FAIL] %s: %s", path.relative_to(obs_dir), e.message)
        except Exception as e:
            errors.append(f"{path.name}: {e}")
            logger.error("  [FAIL] %s: %s", path.relative_to(obs_dir), e)

    for p in sorted((obs_dir / "slos").glob("*.yaml")) if (obs_dir / "slos").exists() else []:
        _check(p, slo_schema)
    for p in sorted((obs_dir / "contracts").glob("*.yaml")) if (obs_dir / "contracts").exists() else []:
        _check(p, contract_schema)

    if errors:
        logger.error("\n[obs-cli] validate FAILED (%d error(s)) — %d file(s) checked:", len(errors), files_checked)
        for e in errors:
            logger.error("  %s", e)
        return 1

    logger.info("\n[obs-cli] All %d file(s) valid.", files_checked)
    return 0


# ── verify ────────────────────────────────────────────────────────────────────

def cmd_verify(
    pipeline_id: str,
    since_minutes: int = 30,
    tempo_url: str = "http://localhost:3200",
) -> int:
    """
    Query Tempo to assert spans are flowing for a pipeline.
    Returns 0 if spans found, 1 if not.
    """
    import time
    import urllib.request
    import urllib.error

    since_ns = int((time.time() - since_minutes * 60) * 1e9)
    query = (
        f'{tempo_url}/api/search?tags=pipeline.name%3D{pipeline_id}'
        f'&minDuration=0ms&start={since_ns}'
    )
    try:
        with urllib.request.urlopen(query, timeout=10) as resp:
            body = json.loads(resp.read().decode())
        traces = body.get("traces", [])
        if not traces:
            logger.error(
                "[obs-cli] verify FAILED: no spans found in Tempo for pipeline '%s' "
                "in the last %d minutes",
                pipeline_id, since_minutes
            )
            return 1
        logger.info(
            "[obs-cli] verify OK: %d trace(s) found for pipeline '%s' in last %dm",
            len(traces), pipeline_id, since_minutes
        )
        return 0
    except urllib.error.URLError as e:
        logger.error("[obs-cli] verify failed: Tempo not reachable at %s: %s", tempo_url, e)
        return 1


# ── CLI wiring ────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="obs-cli",
        description="Watch self-service onboarding CLI for pipeline teams.",
    )
    sub = parser.add_subparsers(dest="command")

    # pipeline sub-group
    p_pipe = sub.add_parser("pipeline", help="Pipeline management commands")
    p_pipe_sub = p_pipe.add_subparsers(dest="subcommand")
    p_reg = p_pipe_sub.add_parser("register", help="Register a new pipeline")
    p_reg.add_argument("--pipeline-id", required=True)
    p_reg.add_argument("--domain", default="unknown")
    p_reg.add_argument("--owner", default="unknown")
    p_reg.add_argument("--tier", type=int, choices=[1, 2, 3], default=2)
    p_reg.add_argument("--output-dir", type=Path, default=Path("observability"))
    p_reg.add_argument("--force", action="store_true")

    # slo init
    p_slo = sub.add_parser("slo", help="SLO commands")
    p_slo_sub = p_slo.add_subparsers(dest="subcommand")
    p_slo_init = p_slo_sub.add_parser("init", help="Generate skeleton SLO YAML")
    p_slo_init.add_argument("--pipeline-id", required=True)
    p_slo_init.add_argument("--owner", default="unknown")
    p_slo_init.add_argument("--output-dir", type=Path, default=Path("observability"))

    # contract init
    p_contract = sub.add_parser("contract", help="Data contract commands")
    p_contract_sub = p_contract.add_subparsers(dest="subcommand")
    p_contract_init = p_contract_sub.add_parser("init", help="Generate skeleton contract YAML")
    p_contract_init.add_argument("--pipeline-id", required=True)
    p_contract_init.add_argument("--owner", default="unknown")
    p_contract_init.add_argument("--output-dir", type=Path, default=Path("observability"))

    # validate
    p_validate = sub.add_parser("validate", help="Validate all observability/ files offline")
    p_validate.add_argument("--obs-dir", type=Path, default=Path("observability"))

    # verify
    p_verify = sub.add_parser("verify", help="Verify spans are flowing in Tempo")
    p_verify.add_argument("--pipeline-id", required=True)
    p_verify.add_argument("--since", type=int, default=30, dest="since_minutes")
    p_verify.add_argument("--tempo-url", default=os.getenv("TEMPO_URL", "http://localhost:3200"))

    return parser


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "pipeline" and args.subcommand == "register":
        return cmd_register(
            pipeline_id=args.pipeline_id,
            domain=args.domain,
            owner=args.owner,
            tier=args.tier,
            output_dir=args.output_dir,
            force=args.force,
        )
    elif args.command == "slo" and args.subcommand == "init":
        return cmd_slo_init(args.pipeline_id, args.owner, args.output_dir)
    elif args.command == "contract" and args.subcommand == "init":
        return cmd_contract_init(args.pipeline_id, args.owner, args.output_dir)
    elif args.command == "validate":
        return cmd_validate(args.obs_dir)
    elif args.command == "verify":
        return cmd_verify(args.pipeline_id, args.since_minutes, args.tempo_url)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
