"""
Watch Spark Metrics Listener — Phase 11.2.

A Spark listener that collects executor JVM and task metrics and exports
them in Prometheus text format. Designed to run inside the Spark driver.

Metrics emitted:
  spark_executor_jvm_heap_used_bytes{spark_app_id, executor_id}
  spark_executor_jvm_gc_time_ms{spark_app_id, executor_id}
  spark_executor_jvm_task_time_ms{spark_app_id, executor_id}
  spark_stage_records_read_total{spark_app_id, stage_id}
  spark_stage_task_duration_seconds{spark_app_id, stage_id, quantile}

This module also provides SparkMetricsSnapshot for unit testing
without a real Spark environment.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("watch_obs.spark_metrics")


@dataclass
class ExecutorMetrics:
    executor_id: str
    jvm_heap_used_bytes: int = 0
    jvm_gc_time_ms: int = 0
    jvm_task_time_ms: int = 0


@dataclass
class StageMetrics:
    stage_id: int
    records_read: int = 0
    task_durations_ms: list[float] = field(default_factory=list)

    def quantile(self, q: float) -> float:
        """Compute quantile from task durations. Returns 0.0 if no data."""
        if not self.task_durations_ms:
            return 0.0
        sorted_d = sorted(self.task_durations_ms)
        idx = int(q * len(sorted_d))
        idx = min(idx, len(sorted_d) - 1)
        return sorted_d[idx]


@dataclass
class SparkMetricsSnapshot:
    app_id: str
    executors: list[ExecutorMetrics] = field(default_factory=list)
    stages: list[StageMetrics] = field(default_factory=list)

    def to_prometheus_lines(self) -> list[str]:
        lines = []
        for ex in self.executors:
            base = f'{{spark_app_id="{self.app_id}",executor_id="{ex.executor_id}"}}'
            lines.append(f"spark_executor_jvm_heap_used_bytes{base} {ex.jvm_heap_used_bytes}")
            lines.append(f"spark_executor_jvm_gc_time_ms{base} {ex.jvm_gc_time_ms}")
            lines.append(f"spark_executor_jvm_task_time_ms{base} {ex.jvm_task_time_ms}")
        for st in self.stages:
            base = f'{{spark_app_id="{self.app_id}",stage_id="{st.stage_id}"}}'
            lines.append(f"spark_stage_records_read_total{base} {st.records_read}")
            for q in (0.5, 0.95):
                q_base = f'{{spark_app_id="{self.app_id}",stage_id="{st.stage_id}",quantile="{q}"}}'
                val_s = st.quantile(q) / 1000.0  # ms -> seconds
                lines.append(f"spark_stage_task_duration_seconds{q_base} {val_s:.3f}")
        return lines


class SparkMetricsCollector:
    """
    Collects metrics from Spark REST API or listener events.
    Injectable mock client for testing without a real Spark cluster.
    """

    def __init__(self, app_id: str, spark_ui_url: str = "http://localhost:4040",
                 _client=None):
        self._app_id = app_id
        self._url = spark_ui_url
        self._client = _client

    def collect(self) -> SparkMetricsSnapshot:
        """Collect current executor and stage metrics."""
        if self._client is not None:
            return self._client.collect(self._app_id)

        import urllib.request
        import json
        snapshot = SparkMetricsSnapshot(app_id=self._app_id)
        try:
            with urllib.request.urlopen(
                f"{self._url}/api/v1/applications/{self._app_id}/executors", timeout=5
            ) as r:
                executors_data = json.loads(r.read().decode())
            for ex in executors_data:
                snapshot.executors.append(ExecutorMetrics(
                    executor_id=str(ex.get("id", "unknown")),
                    jvm_heap_used_bytes=ex.get("memoryUsed", 0),
                    jvm_gc_time_ms=ex.get("totalGCTime", 0),
                    jvm_task_time_ms=ex.get("totalDuration", 0),
                ))
        except Exception as e:
            logger.warning("Failed to collect Spark executor metrics: %s", e)
        return snapshot
