"""
Watch Tail-Based Span Sampling — Phase 12.3.

Configurable sampling for high-volume pipelines:
  - Success spans: sampled at pipeline-configured rate (e.g. 10%)
  - Error spans: always retained at 100%

Sampling config is per pipeline_id, loaded from the central platform config.

Usage:
    sampler = SpanSampler(config={"high-volume-pipe": 0.1})
    decision = sampler.should_sample(span)
    if decision:
        forward_to_tempo(span)
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("watch_obs.span_sampling")

# Span status codes
STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_UNSET = "unset"

DEFAULT_SAMPLE_RATE = 1.0  # 100% by default


@dataclass
class SpanSamplingDecision:
    sampled: bool
    reason: str
    sample_rate: float


@dataclass
class SpanDescriptor:
    """Minimal representation of a span for sampling decisions."""
    span_id: str
    trace_id: str
    pipeline_id: str
    status: str = STATUS_OK  # "ok", "error", "unset"


class SpanSampler:
    """
    Makes tail-based sampling decisions for pipeline spans.

    Args:
        config: dict mapping pipeline_id -> success sample rate (0.0–1.0).
                Pipelines not in config get DEFAULT_SAMPLE_RATE (100%).
        seed: Optional seed for deterministic sampling in tests.
    """

    def __init__(
        self,
        config: Optional[dict[str, float]] = None,
        seed: Optional[int] = None,
    ):
        self._config = config or {}
        self._seed = seed

    def _hash_fraction(self, span_id: str) -> float:
        """Map span_id deterministically to [0.0, 1.0)."""
        seed_suffix = str(self._seed) if self._seed is not None else ""
        raw = (span_id + seed_suffix).encode()
        digest = hashlib.md5(raw).hexdigest()
        return int(digest[:8], 16) / 0xFFFFFFFF

    def should_sample(self, span: SpanDescriptor) -> SpanSamplingDecision:
        """
        Decide whether to retain this span.

        Rules:
          - Error spans: always sampled (100%)
          - Success spans: sampled at pipeline-configured rate
        """
        if span.status == STATUS_ERROR:
            return SpanSamplingDecision(
                sampled=True,
                reason="error_spans_always_sampled",
                sample_rate=1.0,
            )

        rate = self._config.get(span.pipeline_id, DEFAULT_SAMPLE_RATE)
        if rate >= 1.0:
            return SpanSamplingDecision(sampled=True, reason="full_rate", sample_rate=1.0)
        if rate <= 0.0:
            return SpanSamplingDecision(sampled=False, reason="rate_zero", sample_rate=0.0)

        fraction = self._hash_fraction(span.span_id)
        sampled = fraction < rate

        return SpanSamplingDecision(
            sampled=sampled,
            reason="sampled" if sampled else "dropped",
            sample_rate=rate,
        )

    def sample_batch(self, spans: list[SpanDescriptor]) -> tuple[list[SpanDescriptor], list[SpanDescriptor]]:
        """
        Split spans into (retained, dropped).

        Returns (retained, dropped).
        """
        retained = []
        dropped = []
        for span in spans:
            decision = self.should_sample(span)
            if decision.sampled:
                retained.append(span)
            else:
                dropped.append(span)
        return retained, dropped
