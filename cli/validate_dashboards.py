"""
Dashboard CI Validation Script — Phase 3, Tests T3.5–T3.7.

Validates all Grafana dashboard JSON files in dashboards/ against the
Watch platform rules:
  1. Mandatory panels present (by uid, defined in __watch_meta)
  2. All datasource references are known deployed datasource names
  3. $environment template variable is declared

Usage:
    python cli/validate_dashboards.py                      # validate all dashboards
    python cli/validate_dashboards.py dashboards/l1-estate.json  # validate one file

Exit code 0 = all valid. Exit code 1 = one or more violations.
"""
import json
import sys
from pathlib import Path

# Datasource UIDs that are deployed in this platform.
# Any dashboard referencing a uid not in this set fails validation.
KNOWN_DATASOURCE_UIDS = {
    "tempo-dev",
    "tempo-staging",
    "tempo-production",
    "prometheus-dev",
    "prometheus-staging",
    "prometheus-production",
    "loki-dev",
    "loki-staging",
    "loki-production",
}

REQUIRED_VARIABLE = "environment"


class ValidationError:
    def __init__(self, file: str, rule: str, detail: str):
        self.file = file
        self.rule = rule
        self.detail = detail

    def __str__(self):
        return f"[{self.file}] {self.rule}: {self.detail}"


def validate_dashboard(path: Path) -> list[ValidationError]:
    errors = []
    name = path.name

    try:
        with open(path) as f:
            dashboard = json.load(f)
    except json.JSONDecodeError as e:
        return [ValidationError(name, "invalid-json", str(e))]

    meta = dashboard.get("__watch_meta", {})
    panels = dashboard.get("panels", [])
    panel_uids = {p.get("uid") for p in panels}

    # ── Rule 1: mandatory panels ─────────────────────────────────────────
    mandatory = meta.get("mandatory_panels", [])
    for uid in mandatory:
        if uid not in panel_uids:
            errors.append(ValidationError(
                name, "missing-mandatory-panel",
                f"Panel uid '{uid}' is required but not found in this dashboard. "
                f"Present panel uids: {sorted(panel_uids)}"
            ))

    # ── Rule 2: datasource references ───────────────────────────────────
    datasource_uids = _collect_datasource_uids(dashboard)
    for uid in datasource_uids:
        if uid and uid not in KNOWN_DATASOURCE_UIDS:
            errors.append(ValidationError(
                name, "unknown-datasource",
                f"Datasource uid '{uid}' is not in the list of deployed datasources. "
                f"Known uids: {sorted(KNOWN_DATASOURCE_UIDS)}"
            ))

    # ── Rule 3: $environment template variable ───────────────────────────
    variables = dashboard.get("templating", {}).get("list", [])
    var_names = [v.get("name") for v in variables]
    if REQUIRED_VARIABLE not in var_names:
        errors.append(ValidationError(
            name, "missing-required-variable",
            f"Template variable '{REQUIRED_VARIABLE}' is required on every dashboard "
            f"for environment isolation. Found variables: {var_names}"
        ))

    return errors


def _collect_datasource_uids(obj) -> set[str]:
    """Recursively collect all datasource uid values from a dashboard dict."""
    uids = set()
    if isinstance(obj, dict):
        if "datasource" in obj:
            ds = obj["datasource"]
            if isinstance(ds, dict):
                uid = ds.get("uid")
                if uid:
                    uids.add(uid)
            elif isinstance(ds, str) and ds:
                uids.add(ds)
        for v in obj.values():
            uids |= _collect_datasource_uids(v)
    elif isinstance(obj, list):
        for item in obj:
            uids |= _collect_datasource_uids(item)
    return uids


def main(paths: list[Path]) -> int:
    all_errors: list[ValidationError] = []

    for path in paths:
        errors = validate_dashboard(path)
        all_errors.extend(errors)
        status = "OK" if not errors else f"FAIL ({len(errors)} error(s))"
        print(f"  {status}  {path.name}")
        for e in errors:
            print(f"         → {e.rule}: {e.detail}")

    print()
    if all_errors:
        print(f"Dashboard validation FAILED — {len(all_errors)} error(s) across {len(paths)} file(s).")
        return 1
    else:
        print(f"Dashboard validation PASSED — {len(paths)} file(s) checked.")
        return 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        targets = [Path(p) for p in sys.argv[1:]]
    else:
        repo_root = Path(__file__).parents[1]
        targets = sorted((repo_root / "dashboards").glob("*.json"))

    if not targets:
        print("No dashboard files found.")
        sys.exit(0)

    print(f"Validating {len(targets)} dashboard(s)...")
    sys.exit(main(targets))
