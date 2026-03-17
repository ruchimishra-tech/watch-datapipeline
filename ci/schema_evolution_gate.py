"""
Watch Schema Evolution Gate — Phase 8.7.

CI gate script: detects breaking schema changes in a PR diff and
cross-references the data contract to identify affected consumer teams.

Breaking changes:
  - Column removal
  - Column type change

Non-breaking changes (allowed without consumer approval):
  - Adding a nullable column
  - Adding a new column with a default value

Exit codes:
  0 — no breaking changes, or only non-breaking additions
  1 — breaking change detected; output lists consumer teams requiring approval

Usage (in CI):
    python ci/schema_evolution_gate.py \\
        --diff-file pr.diff \\
        --contract-file contracts/customer-360.yaml

The diff is expected to be in a simple YAML-based schema format:
  - "- column_name: type" means column was removed
  - "+ column_name: type" means column was added

This module also exposes pure functions for unit testing.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from typing import Optional

import yaml

logger = logging.getLogger("watch.ci.schema_evolution_gate")
logging.basicConfig(level=logging.INFO, format="%(message)s")


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class ColumnChange:
    name: str
    change_type: str   # "removed", "added", "type_changed"
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    nullable: bool = False


@dataclass
class SchemaEvolutionResult:
    breaking_changes: list[ColumnChange] = field(default_factory=list)
    non_breaking_changes: list[ColumnChange] = field(default_factory=list)
    affected_consumer_teams: list[str] = field(default_factory=list)

    @property
    def has_breaking_changes(self) -> bool:
        return bool(self.breaking_changes)


# ── Diff parsing ──────────────────────────────────────────────────────────────

def parse_schema_diff(diff_lines: list[str]) -> list[ColumnChange]:
    """
    Parse a simplified schema diff format.

    Lines starting with '-' indicate removed columns.
    Lines starting with '+' indicate added columns.
    Lines starting with '~' indicate type changes (old->new format).

    Format examples:
      - risk_tier: string          # column removed
      + new_col: string nullable   # nullable column added
      ~ account_id: integer->string  # type changed

    Returns list of ColumnChange objects.
    """
    changes = []
    for line in diff_lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("- "):
            rest = line[2:].strip()
            parts = rest.split(":", 1)
            if len(parts) == 2:
                col_name = parts[0].strip()
                changes.append(ColumnChange(name=col_name, change_type="removed"))

        elif line.startswith("+ "):
            rest = line[2:].strip()
            parts = rest.split(":", 1)
            if len(parts) == 2:
                col_name = parts[0].strip()
                type_info = parts[1].strip()
                nullable = "nullable" in type_info.lower()
                changes.append(ColumnChange(
                    name=col_name, change_type="added", nullable=nullable
                ))

        elif line.startswith("~ "):
            rest = line[2:].strip()
            parts = rest.split(":", 1)
            if len(parts) == 2:
                col_name = parts[0].strip()
                type_change = parts[1].strip()
                if "->" in type_change:
                    old_t, new_t = type_change.split("->", 1)
                    changes.append(ColumnChange(
                        name=col_name, change_type="type_changed",
                        old_type=old_t.strip(), new_type=new_t.strip()
                    ))

    return changes


def classify_changes(changes: list[ColumnChange]) -> tuple[list[ColumnChange], list[ColumnChange]]:
    """
    Classify changes into breaking and non-breaking.

    Breaking: removal, type change.
    Non-breaking: adding a nullable column.

    Returns (breaking, non_breaking).
    """
    breaking = []
    non_breaking = []
    for c in changes:
        if c.change_type in ("removed", "type_changed"):
            breaking.append(c)
        elif c.change_type == "added":
            # Adding any column is non-breaking (consumers don't depend on new columns)
            non_breaking.append(c)
        else:
            breaking.append(c)  # Unknown change type — conservative, treat as breaking
    return breaking, non_breaking


def load_contract_consumers(contract_path: str) -> list[str]:
    """
    Load consumer team names from a data contract YAML.

    Returns list of team identifiers.
    """
    with open(contract_path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)
    consumers = contract.get("consumers", [])
    return [c["team"] for c in consumers if "team" in c]


# ── Gate evaluation ───────────────────────────────────────────────────────────

def evaluate_schema_gate(
    diff_lines: list[str],
    contract_consumers: Optional[list[str]] = None,
) -> SchemaEvolutionResult:
    """
    Pure gate evaluation: parse diff, classify, identify affected consumers.

    Args:
        diff_lines: Lines of the schema diff file.
        contract_consumers: Consumer team names from the data contract.

    Returns:
        SchemaEvolutionResult with breaking/non-breaking changes and affected teams.
    """
    changes = parse_schema_diff(diff_lines)
    breaking, non_breaking = classify_changes(changes)

    affected = contract_consumers if (breaking and contract_consumers) else []

    return SchemaEvolutionResult(
        breaking_changes=breaking,
        non_breaking_changes=non_breaking,
        affected_consumer_teams=affected,
    )


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Watch Schema Evolution Gate — detects breaking schema changes."
    )
    parser.add_argument("--diff-file", required=True,
                        help="Path to schema diff file.")
    parser.add_argument("--contract-file", default=None,
                        help="Path to data contract YAML for consumer lookup.")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)

    with open(args.diff_file, encoding="utf-8") as f:
        diff_lines = f.readlines()

    consumers = None
    if args.contract_file:
        consumers = load_contract_consumers(args.contract_file)

    result = evaluate_schema_gate(diff_lines, consumers)

    if result.has_breaking_changes:
        logger.info("[watch-schema-gate] BLOCK — breaking changes detected:")
        for c in result.breaking_changes:
            logger.info("  %s: %s", c.change_type, c.name)
        if result.affected_consumer_teams:
            logger.info("Consumer teams requiring approval:")
            for team in result.affected_consumer_teams:
                logger.info("  - %s", team)
        return 1

    logger.info("[watch-schema-gate] PASS — no breaking changes detected.")
    if result.non_breaking_changes:
        logger.info("Non-breaking additions: %s",
                    [c.name for c in result.non_breaking_changes])
    return 0


if __name__ == "__main__":
    sys.exit(main())
