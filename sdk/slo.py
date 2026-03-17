"""
Watch SLO/SLI Computation — Phase 8.

Provides utilities for computing SLIs from Prometheus-style metrics,
tracking error budget consumption, and evaluating deployment gates.

All computation is pure Python — no live Prometheus dependency.
Callers inject raw metric values; recording rules provide the aggregation.

SLI recording rules (managed centrally):
  pipeline:freshness_sli:good   — ratio of runs within sla_seconds
  pipeline:success_rate_sli:good — ratio of runs with status=success
  pipeline:latency_sli:good     — ratio of runs within latency SLA
  pipeline:completeness_sli:good — ratio of runs within volume range
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("watch_obs.slo")


# ── SLI Computation ───────────────────────────────────────────────────────────

def compute_success_rate_sli(success_count: int, total_count: int) -> float:
    """
    Compute the success-rate SLI as a ratio in [0.0, 1.0].

    Args:
        success_count: Number of successful pipeline runs.
        total_count: Total number of pipeline run attempts.

    Returns:
        SLI value (0.0 = all failed, 1.0 = all succeeded).
    """
    if total_count <= 0:
        return 1.0  # No data = assume good (avoid false alerts on new pipelines)
    return max(0.0, min(1.0, success_count / total_count))


def compute_freshness_sli(
    run_durations_seconds: list[float],
    sla_seconds: float,
) -> float:
    """
    Compute the freshness SLI: fraction of runs where data arrived within sla_seconds.

    Args:
        run_durations_seconds: List of observed run durations (seconds from trigger to data arrival).
        sla_seconds: Maximum allowed duration to count as "good".

    Returns:
        SLI value in [0.0, 1.0].
    """
    if not run_durations_seconds:
        return 1.0
    good = sum(1 for d in run_durations_seconds if d <= sla_seconds)
    return good / len(run_durations_seconds)


def compute_latency_sli(
    run_durations_seconds: list[float],
    sla_seconds: float,
) -> float:
    """
    Compute the latency SLI: fraction of runs completing within sla_seconds.
    Semantically identical to freshness_sli but named for clarity in latency contexts.
    """
    return compute_freshness_sli(run_durations_seconds, sla_seconds)


def compute_completeness_sli(
    row_counts: list[int],
    min_rows: Optional[int] = None,
    max_rows: Optional[int] = None,
) -> float:
    """
    Compute the completeness SLI: fraction of runs where row count was within [min_rows, max_rows].

    Args:
        row_counts: List of observed row counts per run.
        min_rows: Minimum acceptable row count (None = no lower bound).
        max_rows: Maximum acceptable row count (None = no upper bound).

    Returns:
        SLI value in [0.0, 1.0].
    """
    if not row_counts:
        return 1.0

    def is_good(count: int) -> bool:
        if min_rows is not None and count < min_rows:
            return False
        if max_rows is not None and count > max_rows:
            return False
        return True

    good = sum(1 for c in row_counts if is_good(c))
    return good / len(row_counts)


# ── Error Budget ──────────────────────────────────────────────────────────────

@dataclass
class ErrorBudgetStatus:
    pipeline_id: str
    slo_name: str
    target_pct: float         # e.g. 99.5
    current_sli: float        # 0.0–1.0
    budget_remaining_pct: float  # percentage of error budget remaining
    is_frozen: bool           # True if below freeze_threshold_pct

    @property
    def sli_pct(self) -> float:
        return self.current_sli * 100.0

    @property
    def is_breaching(self) -> bool:
        return self.sli_pct < self.target_pct


def compute_error_budget(
    pipeline_id: str,
    slo_name: str,
    target_pct: float,
    current_sli: float,
    freeze_threshold_pct: float = 10.0,
) -> ErrorBudgetStatus:
    """
    Compute error budget remaining for a single SLO.

    Error budget = allowed failure rate = (1 - target/100).
    Remaining budget = (current failure rate's distance from total allowance).

    Args:
        pipeline_id: Pipeline identifier.
        slo_name: SLO name (e.g. "freshness").
        target_pct: SLO target as a percentage (e.g. 99.5).
        current_sli: Current SLI value as a ratio (0.0–1.0).
        freeze_threshold_pct: Block deploys below this remaining budget %.

    Returns:
        ErrorBudgetStatus with remaining budget and freeze flag.
    """
    allowed_error_rate = 1.0 - (target_pct / 100.0)
    actual_error_rate = 1.0 - current_sli

    if allowed_error_rate <= 0:
        # Unachievable SLO (100%) — treat as frozen
        budget_remaining_pct = 0.0
    else:
        # How much of the error budget has been consumed
        consumed_fraction = actual_error_rate / allowed_error_rate
        budget_remaining_pct = max(0.0, (1.0 - consumed_fraction) * 100.0)

    is_frozen = budget_remaining_pct < freeze_threshold_pct

    return ErrorBudgetStatus(
        pipeline_id=pipeline_id,
        slo_name=slo_name,
        target_pct=target_pct,
        current_sli=current_sli,
        budget_remaining_pct=round(budget_remaining_pct, 2),
        is_frozen=is_frozen,
    )
