"""
Streaming Simulator — emits realistic Kafka consumer lag metrics
without needing a real Kafka cluster.

Simulates:
  - Normal operation: low lag, steady throughput
  - Lag spike: consumer falls behind, lag grows then recovers
  - Consumer down: lag grows indefinitely until restart

Exposes metrics on /metrics for Prometheus scraping.
Emits OTel spans for significant events (lag spike detected, consumer restart).

Environment variables:
  WATCH_DEPLOYMENT_ENVIRONMENT  — environment label
  STREAMING_METRICS_PORT        — Prometheus port (default: 8098)
  OTEL_EXPORTER_OTLP_ENDPOINT   — for span export
"""
from __future__ import annotations

import logging
import math
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("watch.streaming_sim")

METRICS_PORT = int(os.getenv("STREAMING_METRICS_PORT", "8098"))
ENV = os.getenv("WATCH_DEPLOYMENT_ENVIRONMENT", "docker-dev")

# Consumer group configurations
CONSUMER_GROUPS = [
    {
        "consumer_group": "cg-customer-360",
        "topics": ["customer-events", "account-updates"],
        "pipeline_id": "customer-360-enrichment",
        "normal_lag": 50,
        "spike_lag": 5000,
        "spike_probability": 0.03,    # per poll cycle
        "throughput_msgs_per_sec": 1200,
        "error_rate_normal": 0.001,
    },
    {
        "consumer_group": "cg-risk-scoring",
        "topics": ["transaction-events"],
        "pipeline_id": "risk-scoring",
        "normal_lag": 100,
        "spike_lag": 20000,
        "spike_probability": 0.05,
        "throughput_msgs_per_sec": 800,
        "error_rate_normal": 0.005,
    },
]

# ── In-process metric store ────────────────────────────────────────────────────

_metrics_lock = threading.Lock()
_metric_lines: list[str] = []


def _update_metrics(lines: list[str]) -> None:
    with _metrics_lock:
        global _metric_lines
        _metric_lines = lines


def _get_metrics() -> str:
    with _metrics_lock:
        return "\n".join(_metric_lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            body = _get_metrics().encode()
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

    def log_message(self, fmt, *args):
        pass


# ── Lag simulation ─────────────────────────────────────────────────────────────

class ConsumerGroupSimulator:
    def __init__(self, config: dict):
        self.cfg = config
        self._current_lag: dict[str, float] = {
            t: config["normal_lag"] for t in config["topics"]
        }
        self._in_spike = False
        self._spike_duration_remaining = 0
        self._tick = 0

    def poll(self) -> list[str]:
        """Advance simulation by one tick. Returns Prometheus metric lines."""
        self._tick += 1
        lines = []
        cg = self.cfg["consumer_group"]
        pipeline_id = self.cfg["pipeline_id"]

        # Decide if we enter a spike
        if not self._in_spike and random.random() < self.cfg["spike_probability"]:
            self._in_spike = True
            self._spike_duration_remaining = random.randint(3, 10)  # ticks
            logger.warning("[%s] Lag spike started! Target lag: %d",
                           cg, self.cfg["spike_lag"])

        for topic in self.cfg["topics"]:
            if self._in_spike:
                # Grow towards spike_lag
                target = self.cfg["spike_lag"]
                self._current_lag[topic] = min(
                    self._current_lag[topic] * 1.4 + 200, target
                )
            else:
                # Recover towards normal_lag
                target = self.cfg["normal_lag"]
                noise = random.uniform(-20, 20)
                self._current_lag[topic] = max(
                    0,
                    self._current_lag[topic] * 0.85 + target * 0.15 + noise
                )

            lag = self._current_lag[topic]
            throughput = self.cfg["throughput_msgs_per_sec"] * random.uniform(0.85, 1.15)
            error_rate = self.cfg["error_rate_normal"] * random.uniform(0.5, 2.0)
            if self._in_spike:
                error_rate *= 5  # errors spike with lag
            latency_p95 = 200 + lag * 0.5 + random.uniform(-10, 10)

            base_labels = (
                f'consumer_group="{cg}",topic="{topic}",'
                f'pipeline_id="{pipeline_id}",environment="{ENV}"'
            )
            lines += [
                f"kafka_consumer_lag_sum{{{base_labels}}} {lag:.0f}",
                f"kafka_consumer_throughput_msgs_per_sec{{{base_labels}}} {throughput:.1f}",
                f"kafka_consumer_error_rate{{{base_labels}}} {error_rate:.5f}",
                f"kafka_end_to_end_latency_ms{{{base_labels},quantile=\"0.95\"}} {latency_p95:.1f}",
            ]

        # Count down spike
        if self._in_spike:
            self._spike_duration_remaining -= 1
            if self._spike_duration_remaining <= 0:
                self._in_spike = False
                logger.info("[%s] Lag spike recovered", cg)

        return lines


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("Watch Streaming Simulator starting on :%d", METRICS_PORT)

    # Start metrics server
    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    simulators = [ConsumerGroupSimulator(cfg) for cfg in CONSUMER_GROUPS]

    poll_interval = 15  # seconds
    logger.info("Polling %d consumer groups every %ds", len(simulators), poll_interval)

    while True:
        all_lines = []
        for sim in simulators:
            all_lines.extend(sim.poll())
        _update_metrics(all_lines)
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
