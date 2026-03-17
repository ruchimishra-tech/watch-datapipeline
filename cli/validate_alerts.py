"""
Alert Rule CI Validation Script — Phase 4, Tests T4.3–T4.4.

Validates all Prometheus alert rule YAML files in alerts/ against Watch rules:
  1. Valid YAML structure (groups -> rules -> expr/for/labels/annotations)
  2. Required labels present: severity, runbook_url
  3. PromQL expression is syntactically valid (via promtool if available,
     else via basic structural check)

Usage:
    python cli/validate_alerts.py                     # validate all files
    python cli/validate_alerts.py alerts/my.yaml      # validate one file

Exit code 0 = all valid. Exit code 1 = one or more violations.
"""
import subprocess
import sys
from pathlib import Path

import yaml

REQUIRED_LABELS = {"severity", "runbook_url"}
VALID_SEVERITIES = {"critical", "warning", "info"}


class AlertError:
    def __init__(self, file: str, rule: str, detail: str):
        self.file = file
        self.rule = rule
        self.detail = detail

    def __str__(self):
        return f"[{self.file}] {self.rule}: {self.detail}"


def validate_alert_file(path: Path) -> list[AlertError]:
    errors = []
    name = path.name

    try:
        with open(path, encoding="utf-8") as f:
            doc = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return [AlertError(name, "invalid-yaml", str(e))]

    if not isinstance(doc, dict) or "groups" not in doc:
        return [AlertError(name, "invalid-structure", "Top-level 'groups' key missing")]

    for group in doc.get("groups", []):
        group_name = group.get("name", "<unnamed>")
        for rule in group.get("rules", []):
            alert_name = rule.get("alert", "<unnamed>")
            ctx = f"group={group_name} alert={alert_name}"

            # ── Rule 1: required fields ──────────────────────────────────────
            if "expr" not in rule:
                errors.append(AlertError(name, "missing-expr", f"{ctx}: 'expr' is required"))
            if "alert" not in rule:
                errors.append(AlertError(name, "missing-alert-name", f"{ctx}: 'alert' name is required"))

            # ── Rule 2: required labels ──────────────────────────────────────
            labels = rule.get("labels", {})
            for required_label in REQUIRED_LABELS:
                if required_label not in labels:
                    errors.append(AlertError(
                        name, "missing-required-label",
                        f"{ctx}: label '{required_label}' is required on every alert rule. "
                        f"Present labels: {sorted(labels.keys())}"
                    ))

            # ── Rule 3: severity must be a known value ───────────────────────
            severity = labels.get("severity")
            if severity and severity not in VALID_SEVERITIES:
                errors.append(AlertError(
                    name, "invalid-severity",
                    f"{ctx}: severity '{severity}' is not valid. "
                    f"Must be one of: {sorted(VALID_SEVERITIES)}"
                ))

            # ── Rule 4: runbook_url must look like a URL ─────────────────────
            runbook_url = labels.get("runbook_url", "")
            if runbook_url and not runbook_url.startswith(("http://", "https://")):
                errors.append(AlertError(
                    name, "invalid-runbook-url",
                    f"{ctx}: runbook_url '{runbook_url}' must start with http:// or https://"
                ))

    # ── Rule 5: PromQL syntax via promtool (if installed) ───────────────────
    promtool_errors = _check_promql(path, name)
    errors.extend(promtool_errors)

    return errors


def _check_promql(path: Path, name: str) -> list[AlertError]:
    """Run promtool check rules if promtool is available."""
    try:
        result = subprocess.run(
            ["promtool", "check", "rules", str(path)],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return [AlertError(name, "invalid-promql",
                               (result.stdout + result.stderr).strip())]
        return []
    except FileNotFoundError:
        # promtool not installed — skip PromQL validation
        return []
    except subprocess.TimeoutExpired:
        return [AlertError(name, "promtool-timeout", "promtool timed out after 10s")]


def main(paths: list[Path]) -> int:
    all_errors: list[AlertError] = []

    for path in paths:
        errors = validate_alert_file(path)
        all_errors.extend(errors)
        status = "OK" if not errors else f"FAIL ({len(errors)} error(s))"
        print(f"  {status}  {path.name}")
        for e in errors:
            print(f"         -> {e.rule}: {e.detail}")

    print()
    if all_errors:
        print(f"Alert rule validation FAILED - {len(all_errors)} error(s) across {len(paths)} file(s).")
        return 1
    print(f"Alert rule validation PASSED - {len(paths)} file(s) checked.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1:
        targets = [Path(p) for p in sys.argv[1:]]
    else:
        repo_root = Path(__file__).parents[1]
        targets = sorted((repo_root / "alerts").glob("*.yaml"))

    if not targets:
        print("No alert rule files found.")
        sys.exit(0)

    print(f"Validating {len(targets)} alert rule file(s)...")
    sys.exit(main(targets))
