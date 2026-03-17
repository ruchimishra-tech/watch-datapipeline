"""
Watch Cardinality Guard — Phase 12.2.

A metric pipeline processor that:
  1. Strips `pipeline_run_id` from all metric data points (too high cardinality)
  2. Strips any label with cardinality > max_cardinality unique values
  3. Tracks stripped labels in `metrics_labels_stripped_total` counter

This is designed to run as a processor step in the ADOT collector pipeline,
but the stripping logic is pure Python and injectable for testing.

Usage in tests:
    guard = CardinalityGuard(max_cardinality=1000, always_strip={"pipeline_run_id"})
    metrics_out, stripped = guard.process(metrics_in)
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("watch_obs.cardinality_guard")

# Labels always stripped regardless of cardinality
DEFAULT_ALWAYS_STRIP = {"pipeline_run_id"}


@dataclass
class MetricDataPoint:
    """A single metric observation with its label set."""
    metric_name: str
    labels: dict[str, str]
    value: float


@dataclass
class CardinalityGuardResult:
    processed: list[MetricDataPoint]
    stripped_labels: dict[str, int]   # label_name -> count of data points affected
    total_stripped_count: int


class CardinalityGuard:
    """
    Strips high-cardinality and always-strip labels from metric data points.

    Args:
        max_cardinality: Maximum unique label values allowed in a batch.
        always_strip: Set of label names to always remove (e.g. pipeline_run_id).
    """

    def __init__(
        self,
        max_cardinality: int = 1000,
        always_strip: Optional[set[str]] = None,
    ):
        self._max_cardinality = max_cardinality
        self._always_strip = always_strip if always_strip is not None else DEFAULT_ALWAYS_STRIP
        self._stripped_counters: dict[str, int] = defaultdict(int)

    def process(self, data_points: list[MetricDataPoint]) -> CardinalityGuardResult:
        """
        Process a batch of metric data points, stripping high-cardinality labels.

        Returns the cleaned data points and a record of what was stripped.
        """
        # First pass: compute cardinality of each label in this batch
        label_values: dict[str, set[str]] = defaultdict(set)
        for dp in data_points:
            for k, v in dp.labels.items():
                label_values[k].add(v)

        high_cardinality = {
            k for k, vs in label_values.items()
            if len(vs) > self._max_cardinality
        }
        labels_to_strip = self._always_strip | high_cardinality

        # Second pass: strip labels
        stripped_counts: dict[str, int] = defaultdict(int)
        processed = []
        for dp in data_points:
            cleaned_labels = {}
            for k, v in dp.labels.items():
                if k in labels_to_strip:
                    stripped_counts[k] += 1
                    self._stripped_counters[k] += 1
                    logger.debug("stripped label %s from metric %s", k, dp.metric_name)
                else:
                    cleaned_labels[k] = v
            processed.append(MetricDataPoint(dp.metric_name, cleaned_labels, dp.value))

        total = sum(stripped_counts.values())
        return CardinalityGuardResult(
            processed=processed,
            stripped_labels=dict(stripped_counts),
            total_stripped_count=total,
        )

    def to_prometheus_lines(self) -> list[str]:
        """Render cumulative strip counters as Prometheus text format."""
        lines = []
        for label_name, count in self._stripped_counters.items():
            lines.append(
                f'metrics_labels_stripped_total{{label="{label_name}"}} {count}'
            )
        return lines

    def total_stripped(self) -> int:
        return sum(self._stripped_counters.values())
