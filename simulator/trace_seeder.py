"""
Trace Seeder — injects demo pipeline traces into Tempo for L2 dashboard.

Runs all scenarios in fast mode (50ms phase durations) to generate a
realistic set of traces without waiting for real-time simulation.

Usage (from docker-compose or host):
  OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317 \\
  WATCH_DEPLOYMENT_ENVIRONMENT=docker-dev \\
  python -m simulator.trace_seeder
"""
from __future__ import annotations

import copy
import logging
import os
import time

from simulator.scenarios import SCENARIOS, ScenarioConfig, PhaseConfig
from simulator.pipeline_runner import execute_scenario

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("trace_seeder")

RUNS_PER_SCENARIO = 8   # 5 scenarios × 8 runs = 40 traces, enough for demo


def make_fast(s: ScenarioConfig) -> ScenarioConfig:
    """Return scenario copy with 50ms phase durations for rapid seeding."""
    fast = copy.deepcopy(s)
    for p in fast.phases:
        p.min_seconds = 0.05
        p.max_seconds = 0.10
    fast.run_interval_seconds = 0
    return fast


def main() -> None:
    # Ensure OTLP endpoint is set before any SDK call
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = endpoint
    env = os.getenv("WATCH_DEPLOYMENT_ENVIRONMENT", "docker-dev")
    os.environ["WATCH_DEPLOYMENT_ENVIRONMENT"] = env

    logger.info("=" * 55)
    logger.info("Watch Trace Seeder")
    logger.info("  OTLP endpoint : %s", endpoint)
    logger.info("  Environment   : %s", env)
    logger.info("  Runs/scenario : %d", RUNS_PER_SCENARIO)
    logger.info("=" * 55)

    # Short pause so collector is ready (important in docker-compose startup)
    logger.info("Waiting 5s for collector to be ready...")
    time.sleep(5)

    scenarios = [make_fast(s) for s in SCENARIOS if s.enabled]
    total = 0

    for s in scenarios:
        logger.info("--- %s (%s) ---", s.pipeline_id, s.domain)
        successes = 0
        failures = 0
        for i in range(RUNS_PER_SCENARIO):
            result = execute_scenario(s)
            if result.status == "success":
                successes += 1
            else:
                failures += 1
            logger.info(
                "  [%d/%d] %-10s  %.3fs  dq_violations=%d",
                i + 1, RUNS_PER_SCENARIO,
                result.status,
                result.duration_seconds,
                result.dq_violations,
            )
            total += 1
            time.sleep(0.05)  # small gap so spans get distinct timestamps

        logger.info("  => %d success  %d failed", successes, failures)

    # Flush all pending spans before exit
    logger.info("Flushing spans to collector...")
    from sdk.emitter import flush
    flush(timeout_ms=15000)

    logger.info("=" * 55)
    logger.info("Seeding complete: %d traces sent to %s", total, endpoint)
    logger.info("Open L2 dashboard and select a pipeline to see runs.")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
