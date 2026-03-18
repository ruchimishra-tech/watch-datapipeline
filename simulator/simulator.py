"""
Watch Pipeline Simulator — main entry point.

Runs all enabled scenarios on their configured intervals using
a thread-per-scenario model. Each thread sleeps between runs.

Environment variables:
  OTEL_EXPORTER_OTLP_ENDPOINT  — OTLP gRPC endpoint (e.g. http://collector:4317)
  WATCH_DEPLOYMENT_ENVIRONMENT — environment label (default: docker-dev)
  SIMULATOR_METRICS_PORT       — Prometheus metrics port (default: 8099)
  SIMULATOR_SCENARIO           — run only this scenario (optional, for debugging)
"""
from __future__ import annotations

import logging
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from simulator.scenarios import SCENARIOS, ScenarioConfig
from simulator.pipeline_runner import execute_scenario, RunResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("watch.simulator")

METRICS_PORT = int(os.getenv("SIMULATOR_METRICS_PORT", "8099"))
ONLY_SCENARIO = os.getenv("SIMULATOR_SCENARIO", "")

# ── Prometheus metrics (in-process, no extra library needed) ──────────────────

_lock = threading.Lock()
_counters: dict[str, int] = {}
_gauges: dict[str, float] = {}


def _inc(name: str, labels: dict) -> None:
    key = _metric_key(name, labels)
    with _lock:
        _counters[key] = _counters.get(key, 0) + 1


def _set_gauge(name: str, labels: dict, value: float) -> None:
    key = _metric_key(name, labels)
    with _lock:
        _gauges[key] = value


def _metric_key(name: str, labels: dict) -> str:
    lstr = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{lstr}}}"


def _render_metrics() -> str:
    lines = []
    with _lock:
        for key, val in _counters.items():
            lines.append(f"{key} {val}")
        for key, val in _gauges.items():
            lines.append(f"{key} {val:.3f}")
    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            body = _render_metrics().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path in ("/health", "/"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress HTTP access log noise


def _start_metrics_server() -> None:
    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info("Metrics server started on :%d/metrics", METRICS_PORT)


# ── Scenario thread ────────────────────────────────────────────────────────────

def _emit_pipeline_info(scenarios: list) -> None:
    """Emit static info metrics — one gauge per pipeline with all catalogue metadata."""
    env = os.getenv("WATCH_DEPLOYMENT_ENVIRONMENT", "docker-dev")
    for s in scenarios:
        labels = {
            "pipeline_id": s.pipeline_id,
            "pipeline_name": s.pipeline_name,
            "domain": s.domain,
            "owner": s.owner,
            "environment": env,
            "description": (s.description[:80] if s.description else ""),
        }
        _set_gauge("simulator_pipeline_info", labels, 1.0)


def _record_result(result: RunResult, scenario: ScenarioConfig) -> None:
    env = os.getenv("WATCH_DEPLOYMENT_ENVIRONMENT", "docker-dev")
    labels = {
        "pipeline_id": result.pipeline_id,
        "pipeline_name": scenario.pipeline_name,
        "domain": scenario.domain,
        "owner": scenario.owner,
        "environment": env,
    }
    _inc("simulator_pipeline_runs_total", {**labels, "status": result.status})
    _set_gauge("simulator_pipeline_last_duration_seconds", labels, result.duration_seconds)

    if result.dq_violations > 0:
        _inc("simulator_dq_violations_total", labels)

    if scenario.sla_seconds and result.duration_seconds > scenario.sla_seconds:
        _inc("simulator_sla_breaches_total", labels)


def _run_scenario_loop(scenario: ScenarioConfig) -> None:
    """Thread target: run a scenario repeatedly on its configured interval."""
    # Stagger startup so all pipelines don't fire simultaneously
    jitter = random.uniform(0, scenario.run_interval_seconds * 0.5)
    logger.info("[%s] Waiting %.1fs before first run (jitter)", scenario.pipeline_id, jitter)
    time.sleep(jitter)

    while True:
        try:
            result = execute_scenario(scenario)
            _record_result(result, scenario)
        except Exception as e:
            logger.error("[%s] Unhandled error in scenario: %s", scenario.pipeline_id, e)

        logger.info("[%s] Sleeping %ds until next run", scenario.pipeline_id,
                    scenario.run_interval_seconds)
        time.sleep(scenario.run_interval_seconds)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    env = os.getenv("WATCH_DEPLOYMENT_ENVIRONMENT", "docker-dev")
    logger.info("Watch Pipeline Simulator starting")
    logger.info("  OTLP endpoint : %s", endpoint or "(not set — spans go to /dev/null)")
    logger.info("  Environment   : %s", env)
    logger.info("  Metrics port  : %d", METRICS_PORT)

    _start_metrics_server()

    active = [s for s in SCENARIOS if s.enabled]
    _emit_pipeline_info(active)
    if ONLY_SCENARIO:
        active = [s for s in active if s.pipeline_id == ONLY_SCENARIO]
        if not active:
            logger.error("No scenario found with pipeline_id=%s", ONLY_SCENARIO)
            return

    logger.info("Starting %d scenario thread(s): %s",
                len(active), [s.pipeline_id for s in active])

    threads = []
    for scenario in active:
        t = threading.Thread(
            target=_run_scenario_loop,
            args=(scenario,),
            name=f"sim-{scenario.pipeline_id}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
            alive = [t.name for t in threads if t.is_alive()]
            logger.info("Heartbeat — %d/%d scenario threads alive: %s",
                        len(alive), len(threads), alive)
    except KeyboardInterrupt:
        logger.info("Simulator stopped.")


if __name__ == "__main__":
    main()
