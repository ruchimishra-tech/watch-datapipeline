"""
Watch Error Budget Gate — Phase 8.4.

CI gate script: blocks a deployment if the pipeline's error budget
is below `freeze_threshold_pct`. Called as a CI step in team PR pipelines.

Exit codes:
  0 — budget is healthy, or override is active => PR may proceed
  1 — budget is depleted below freeze threshold => PR is blocked

Usage (in CI):
    python ci/error_budget_gate.py \\
        --pipeline-id customer-360-enrichment \\
        --budget-remaining-pct 8.0 \\
        --freeze-threshold 10 \\
        [--override-label override-error-budget] \\
        [--approver-teams team-lead-alice,team-lead-bob]

The script is designed to receive pre-fetched budget values from a
wrapper that calls the Prometheus API. This keeps the gate testable
without live infrastructure.
"""
import argparse
import logging
import sys
from typing import Optional

logger = logging.getLogger("watch.ci.error_budget_gate")
logging.basicConfig(level=logging.INFO, format="%(message)s")


# ── Gate logic (pure, testable) ───────────────────────────────────────────────

class GateResult:
    def __init__(self, allowed: bool, reason: str):
        self.allowed = allowed
        self.reason = reason
        self.exit_code = 0 if allowed else 1

    def __repr__(self) -> str:
        status = "PASS" if self.allowed else "BLOCK"
        return f"GateResult({status}: {self.reason})"


def evaluate_gate(
    pipeline_id: str,
    budget_remaining_pct: float,
    freeze_threshold_pct: float,
    has_override_label: bool = False,
    approver_teams: Optional[list[str]] = None,
) -> GateResult:
    """
    Pure gate evaluation — no I/O, fully testable.

    Args:
        pipeline_id: Pipeline identifier (for logging).
        budget_remaining_pct: Current error budget remaining (0–100%).
        freeze_threshold_pct: Block below this threshold.
        has_override_label: True if PR has the override label.
        approver_teams: List of team leads who have approved (for override).

    Returns:
        GateResult with allowed flag and reason.
    """
    if budget_remaining_pct >= freeze_threshold_pct:
        return GateResult(
            allowed=True,
            reason=(
                f"Error budget healthy: {budget_remaining_pct:.1f}% remaining "
                f"(threshold: {freeze_threshold_pct:.1f}%)"
            ),
        )

    # Budget below threshold — check for override
    if has_override_label and approver_teams:
        return GateResult(
            allowed=True,
            reason=(
                f"Error budget low ({budget_remaining_pct:.1f}%) but override active "
                f"with approvals from: {', '.join(approver_teams)}"
            ),
        )

    if has_override_label and not approver_teams:
        return GateResult(
            allowed=False,
            reason=(
                f"Error budget depleted: {budget_remaining_pct:.1f}% remaining "
                f"(threshold: {freeze_threshold_pct:.1f}%). "
                f"Override label present but no team lead approvals found. "
                f"Request approval from {pipeline_id} team leads."
            ),
        )

    return GateResult(
        allowed=False,
        reason=(
            f"Error budget depleted: {budget_remaining_pct:.1f}% remaining "
            f"(threshold: {freeze_threshold_pct:.1f}%). "
            f"Deployment blocked for pipeline '{pipeline_id}'. "
            f"Add the 'override-error-budget' label and obtain team lead approval to proceed."
        ),
    )


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Watch Error Budget Gate — blocks deploys when budget is low."
    )
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--budget-remaining-pct", type=float, required=True)
    parser.add_argument("--freeze-threshold", type=float, default=10.0)
    parser.add_argument("--override-label", default=None,
                        help="Name of override label if present on the PR")
    parser.add_argument("--approver-teams", default="",
                        help="Comma-separated list of approving team leads")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)

    has_override = bool(args.override_label)
    approvers = [t.strip() for t in args.approver_teams.split(",") if t.strip()]

    result = evaluate_gate(
        pipeline_id=args.pipeline_id,
        budget_remaining_pct=args.budget_remaining_pct,
        freeze_threshold_pct=args.freeze_threshold,
        has_override_label=has_override,
        approver_teams=approvers if approvers else None,
    )

    status = "PASS" if result.allowed else "BLOCK"
    logger.info("[watch-gate] %s | pipeline=%s | %s", status, args.pipeline_id, result.reason)
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
