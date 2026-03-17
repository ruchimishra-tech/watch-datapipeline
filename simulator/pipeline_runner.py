"""
Pipeline Runner — executes a single scenario run using the Watch SDK.

Imports sdk.pipeline / sdk.phase directly so spans are real OTel spans
exported to the collector via OTEL_EXPORTER_OTLP_ENDPOINT.
"""
from __future__ import annotations

import logging
import random
import time
import uuid
from typing import Optional

from simulator.scenarios import ScenarioConfig, PhaseConfig, DQConfig
from sdk.pipeline import Pipeline
from sdk.dq import DataQualityChecker, DataQualityFailure, ACTION_WARN, ACTION_FAIL

logger = logging.getLogger("watch.simulator.runner")


class RunResult:
    def __init__(self, pipeline_id: str, run_id: str, status: str,
                 duration_seconds: float, dq_violations: int = 0,
                 failed_phase: Optional[str] = None):
        self.pipeline_id = pipeline_id
        self.run_id = run_id
        self.status = status                    # "success" | "failed"
        self.duration_seconds = duration_seconds
        self.dq_violations = dq_violations
        self.failed_phase = failed_phase


def _simulate_duration(phase: PhaseConfig) -> float:
    """Return a realistic random duration with slight skew towards max."""
    base = random.uniform(phase.min_seconds, phase.max_seconds)
    # Occasional slow runs (10% chance of 1.5–2x slowdown)
    if random.random() < 0.10:
        base *= random.uniform(1.5, 2.0)
    return base


def _simulate_row_count(phase: PhaseConfig) -> int:
    return random.randint(phase.rows_min, phase.rows_max)


def _run_dq_checks(checker: DataQualityChecker, dq_configs: list[DQConfig],
                   total_rows: int) -> int:
    """Run all DQ checks. Returns count of violations."""
    violations = 0
    for dq in dq_configs:
        if random.random() < dq.spike_probability:
            null_rate = dq.null_rate_spike
        else:
            null_rate = dq.null_rate_normal

        null_count = int(total_rows * null_rate)
        max_pct = 0.03   # 3% threshold: above = warning

        checker.check_null_rate(
            column=dq.column,
            null_count=null_count,
            total_rows=total_rows,
            max_pct=max_pct,
            action=ACTION_WARN,
        )
        if null_rate > max_pct:
            violations += 1

    return violations


def execute_scenario(scenario: ScenarioConfig) -> RunResult:
    """
    Execute one run of a scenario pipeline using the Watch SDK.
    Emits real OTel spans to the configured collector endpoint.
    """
    run_id = uuid.uuid4().hex[:16]
    start_time = time.time()
    dq_violations = 0
    failed_phase = None

    logger.info("[%s] Starting run %s", scenario.pipeline_id, run_id)

    try:
        with Pipeline(
            name=scenario.pipeline_name,
            pipeline_id=scenario.pipeline_id,
            run_id=run_id,
        ) as pipeline:

            for phase_cfg in scenario.phases:
                duration = _simulate_duration(phase_cfg)
                row_count = _simulate_row_count(phase_cfg)

                # Simulate the phase work
                with pipeline.phase(
                    phase_cfg.name,
                    source_system="simulator",
                    row_count=row_count,
                ) as phase:
                    phase.set_attribute("simulator.duration_planned_s", round(duration, 2))
                    phase.set_attribute("simulator.row_count", row_count)

                    # Simulate actual work time
                    time.sleep(duration)

                    # DQ checks happen after load phase reads data
                    if phase_cfg.name == "extract" and scenario.dq_checks:
                        checker = DataQualityChecker(
                            pipeline_id=scenario.pipeline_id,
                        )
                        dq_violations = _run_dq_checks(checker, scenario.dq_checks, row_count)
                        try:
                            results = checker.evaluate()
                            for r in results:
                                phase.emit_event(
                                    "dq.check",
                                    **{
                                        "dq.rule": r.rule_id,
                                        "dq.column": r.column or "",
                                        "dq.severity": r.severity,
                                        "dq.failure_rate_pct": r.failure_rate_pct,
                                    }
                                )
                        except DataQualityFailure:
                            pass  # warn-only in simulator

                    # Simulate phase failure
                    if random.random() < phase_cfg.fail_probability:
                        failed_phase = phase_cfg.name
                        raise RuntimeError(
                            f"Simulated failure in {phase_cfg.name} phase "
                            f"for pipeline {scenario.pipeline_id}"
                        )

    except RuntimeError as e:
        duration_seconds = time.time() - start_time
        logger.warning("[%s] Run %s FAILED: %s", scenario.pipeline_id, run_id, e)
        return RunResult(
            pipeline_id=scenario.pipeline_id,
            run_id=run_id,
            status="failed",
            duration_seconds=duration_seconds,
            dq_violations=dq_violations,
            failed_phase=failed_phase,
        )

    duration_seconds = time.time() - start_time

    # Check SLA
    if scenario.sla_seconds and duration_seconds > scenario.sla_seconds:
        logger.warning(
            "[%s] Run %s breached SLA: %.1fs > %ds",
            scenario.pipeline_id, run_id, duration_seconds, scenario.sla_seconds
        )

    logger.info(
        "[%s] Run %s SUCCESS in %.1fs (dq_violations=%d)",
        scenario.pipeline_id, run_id, duration_seconds, dq_violations
    )

    return RunResult(
        pipeline_id=scenario.pipeline_id,
        run_id=run_id,
        status="success",
        duration_seconds=duration_seconds,
        dq_violations=dq_violations,
    )
