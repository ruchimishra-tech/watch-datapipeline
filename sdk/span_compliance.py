"""
Watch Span Compliance Gate — Phase 9.1 / 9.3.

Validates that spans emitted by pipelines carry the required attributes
before they reach Tempo. Non-compliant spans are quarantined (logged, not stored).

Required span attributes:
  pipeline.run_id        — unique identifier for the pipeline run
  pipeline.name          — human-readable pipeline name
  deployment.environment — environment tag (dev/staging/prod)

This module provides:
  - SpanComplianceChecker: pure validator (no I/O), injectable into the ADOT pipeline
  - SpanQuarantineLog: collects quarantined spans with reasons for inspection
  - check_for_pii: heuristic detector for unredacted PII values in span attributes
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("watch_obs.span_compliance")

# ── Required attributes ────────────────────────────────────────────────────────

REQUIRED_ATTRIBUTES = [
    "pipeline.run_id",
    "pipeline.name",
    "deployment.environment",
]

VALID_ENVIRONMENTS = {"dev", "staging", "prod", "production", "test"}

# ── PII heuristics ─────────────────────────────────────────────────────────────

_PII_PATTERNS = [
    # Email addresses
    (re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"), "email"),
    # SSN (US) — must be exactly NNN-NN-NNNN, not part of larger strings
    (re.compile(r"(?<!\w)\d{3}-\d{2}-\d{4}(?!\w)"), "ssn"),
    # Credit card (16-digit groups separated by spaces or hyphens)
    (re.compile(r"\b\d{4}[\s\-]\d{4}[\s\-]\d{4}[\s\-]\d{4}\b"), "credit_card"),
    # IBAN (simplified) — starts with 2 letters then 2 digits then alphanumeric
    (re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,28}\b"), "iban"),
]


def check_for_pii(value: str) -> Optional[str]:
    """
    Heuristic check: returns the PII type if the value looks like PII, else None.

    This is a best-effort heuristic — not exhaustive. Production deployments
    should supplement with a dedicated PII detection service.
    """
    for pattern, pii_type in _PII_PATTERNS:
        if pattern.search(str(value)):
            return pii_type
    return None


# ── Compliance result ──────────────────────────────────────────────────────────

@dataclass
class ComplianceResult:
    span_id: str
    is_compliant: bool
    violations: list[str] = field(default_factory=list)
    pii_violations: list[str] = field(default_factory=list)

    @property
    def primary_reason(self) -> Optional[str]:
        """Primary quarantine reason (first violation), for Prometheus label."""
        if self.violations:
            return self.violations[0]
        if self.pii_violations:
            return "pii_detected"
        return None


# ── Compliance checker ─────────────────────────────────────────────────────────

class SpanComplianceChecker:
    """
    Pure span validator — no I/O, fully testable.

    Usage:
        checker = SpanComplianceChecker()
        result = checker.check(span_id="abc123", attributes={...})
        if not result.is_compliant:
            quarantine_log.add(result)
    """

    def __init__(
        self,
        required_attributes: Optional[list[str]] = None,
        valid_environments: Optional[set[str]] = None,
        check_pii: bool = True,
    ):
        self._required = required_attributes or REQUIRED_ATTRIBUTES
        self._valid_envs = valid_environments or VALID_ENVIRONMENTS
        self._check_pii = check_pii

    def check(self, span_id: str, attributes: dict) -> ComplianceResult:
        """
        Validate a span's attributes.

        Args:
            span_id: Unique span identifier (for logging/quarantine).
            attributes: Dict of span attribute key->value.

        Returns:
            ComplianceResult with violations list (empty = compliant).
        """
        violations = []
        pii_violations = []

        # Required attribute presence
        for attr in self._required:
            if attr not in attributes or attributes[attr] is None or attributes[attr] == "":
                violations.append(f"missing_{attr.replace('.', '_')}")

        # Environment value check
        env = attributes.get("deployment.environment", "")
        if env and env not in self._valid_envs:
            violations.append(f"invalid_environment:{env}")

        # PII detection
        if self._check_pii:
            for key, value in attributes.items():
                pii_type = check_for_pii(str(value))
                if pii_type:
                    pii_violations.append(f"{key}={pii_type}")

        is_compliant = not violations and not pii_violations

        return ComplianceResult(
            span_id=span_id,
            is_compliant=is_compliant,
            violations=violations,
            pii_violations=pii_violations,
        )


# ── Quarantine log ─────────────────────────────────────────────────────────────

class SpanQuarantineLog:
    """
    In-memory quarantine log for non-compliant spans.

    In production, this backs a structured log sink (Loki) and
    increments `spans_quarantined_total{reason=...}` in Prometheus.
    """

    def __init__(self):
        self._entries: list[ComplianceResult] = []
        self._counters: dict[str, int] = {}

    def add(self, result: ComplianceResult) -> None:
        """Add a non-compliant span to the quarantine log."""
        self._entries.append(result)
        reason = result.primary_reason or "unknown"
        self._counters[reason] = self._counters.get(reason, 0) + 1
        logger.warning(
            "span_quarantined span_id=%s reason=%s violations=%s pii=%s",
            result.span_id,
            reason,
            result.violations,
            result.pii_violations,
        )

    def total(self) -> int:
        return len(self._entries)

    def count_by_reason(self, reason: str) -> int:
        return self._counters.get(reason, 0)

    def has_span(self, span_id: str) -> bool:
        return any(e.span_id == span_id for e in self._entries)

    def entries(self) -> list[ComplianceResult]:
        return list(self._entries)

    def to_prometheus_lines(self) -> list[str]:
        """Render quarantine counters as Prometheus text format lines."""
        lines = []
        for reason, count in self._counters.items():
            lines.append(f'spans_quarantined_total{{reason="{reason}"}} {count}')
        return lines
