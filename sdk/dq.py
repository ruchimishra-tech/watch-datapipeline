"""
Watch Data Quality Rule Engine — Phase 7, Feature 7.1/7.2.

Evaluates configurable DQ rules and emits span events for each result.
No raw data values are ever stored — only statistics (counts, rates).

Usage:
    with run.phase("transform") as phase:
        dq = DataQualityChecker(phase_span=phase._span, pipeline_id="my-pipe")
        dq.check_null_rate("account_id", null_count=50, total_rows=5000, max_pct=0.01)
        dq.check_volume(row_count=500, min_rows=1000, max_rows=100000)
        dq.check_freshness(data_age_hours=30, max_age_hours=25)
        dq.check_schema(expected_columns={"id","name"}, actual_columns={"id","email"})
        results = dq.evaluate()   # emits span events, raises if any rule action=fail
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("watch_obs.dq")

# ── Constants ─────────────────────────────────────────────────────────────────

SEVERITY_PASS = "pass"
SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"

ACTION_WARN = "warn"
ACTION_QUARANTINE = "quarantine"
ACTION_FAIL = "fail"


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class DQResult:
    rule_id: str
    column: Optional[str]
    severity: str                    # pass | warning | critical
    failure_rate_pct: float          # 0.0–100.0
    failed_row_count: int
    action_taken: str                # warn | quarantined | pipeline_failed | none
    schema_drift_detected: bool = False
    schema_drift_detail: str = ""
    rows_quarantined: int = 0
    extra: dict = field(default_factory=dict)

    def span_event_attributes(self) -> dict:
        """Returns only statistics — never raw data values."""
        attrs = {
            "dq.rule_id":          self.rule_id,
            "dq.severity":         self.severity,
            "dq.failure_rate_pct": round(self.failure_rate_pct, 4),
            "dq.failed_row_count": self.failed_row_count,
            "dq.action_taken":     self.action_taken,
        }
        if self.column:
            attrs["dq.column"] = self.column
        if self.schema_drift_detected:
            attrs["dq.schema_drift_detected"] = True
            attrs["dq.schema_drift_detail"] = self.schema_drift_detail
        if self.rows_quarantined > 0:
            attrs["dq.rows_quarantined"] = self.rows_quarantined
        return attrs


# ── Rule definitions ──────────────────────────────────────────────────────────

@dataclass
class _NullRateRule:
    column: str
    null_count: int
    total_rows: int
    max_pct: float          # 0.0–1.0
    warn_pct: Optional[float] = None   # if set: warn above this, critical above max_pct
    action: str = ACTION_WARN


@dataclass
class _VolumeRule:
    row_count: int
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    action: str = ACTION_FAIL


@dataclass
class _FreshnessRule:
    data_age_hours: float
    max_age_hours: float
    action: str = ACTION_WARN


@dataclass
class _SchemaRule:
    expected_columns: set
    actual_columns: set
    action: str = ACTION_WARN


# ── Engine ────────────────────────────────────────────────────────────────────

class DataQualityChecker:
    """
    Registers DQ rules and evaluates them on .evaluate().

    Args:
        phase_span: OTel Span to add events to (can be a mock in tests).
        pipeline_id: Used in log messages.
    """

    def __init__(self, phase_span=None, pipeline_id: str = "unknown"):
        self._span = phase_span
        self._pipeline_id = pipeline_id
        self._rules: list = []

    # ── Rule registration ─────────────────────────────────────────────────────

    def check_null_rate(
        self,
        column: str,
        null_count: int,
        total_rows: int,
        max_pct: float,
        warn_pct: Optional[float] = None,
        action: str = ACTION_WARN,
    ) -> "DataQualityChecker":
        self._rules.append(_NullRateRule(
            column=column,
            null_count=null_count,
            total_rows=total_rows,
            max_pct=max_pct,
            warn_pct=warn_pct,
            action=action,
        ))
        return self

    def check_volume(
        self,
        row_count: int,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
        action: str = ACTION_FAIL,
    ) -> "DataQualityChecker":
        self._rules.append(_VolumeRule(
            row_count=row_count,
            min_rows=min_rows,
            max_rows=max_rows,
            action=action,
        ))
        return self

    def check_freshness(
        self,
        data_age_hours: float,
        max_age_hours: float,
        action: str = ACTION_WARN,
    ) -> "DataQualityChecker":
        self._rules.append(_FreshnessRule(
            data_age_hours=data_age_hours,
            max_age_hours=max_age_hours,
            action=action,
        ))
        return self

    def check_schema(
        self,
        expected_columns: set,
        actual_columns: set,
        action: str = ACTION_WARN,
    ) -> "DataQualityChecker":
        self._rules.append(_SchemaRule(
            expected_columns=set(expected_columns),
            actual_columns=set(actual_columns),
            action=action,
        ))
        return self

    # ── Evaluation ────────────────────────────────────────────────────────────

    def evaluate(self) -> list[DQResult]:
        """
        Evaluate all registered rules. Emits a span event per rule.
        Raises DataQualityFailure if any rule has action=fail and severity=critical.
        Returns list of all DQResult objects (pass + violations).
        """
        results: list[DQResult] = []
        fail_results: list[DQResult] = []

        for rule in self._rules:
            try:
                result = self._evaluate_rule(rule)
                results.append(result)
                self._emit_event(result)
                if result.action_taken == "pipeline_failed":
                    fail_results.append(result)
            except Exception as exc:
                logger.warning("watch_obs.dq: rule evaluation error: %s", exc)

        if fail_results:
            reasons = "; ".join(
                f"{r.rule_id}({r.column or 'n/a'})" for r in fail_results
            )
            raise DataQualityFailure(
                f"Pipeline failed due to DQ violations: {reasons}",
                results=fail_results,
            )

        return results

    # ── Rule evaluators ───────────────────────────────────────────────────────

    def _evaluate_rule(self, rule) -> DQResult:
        if isinstance(rule, _NullRateRule):
            return self._eval_null_rate(rule)
        if isinstance(rule, _VolumeRule):
            return self._eval_volume(rule)
        if isinstance(rule, _FreshnessRule):
            return self._eval_freshness(rule)
        if isinstance(rule, _SchemaRule):
            return self._eval_schema(rule)
        raise ValueError(f"Unknown rule type: {type(rule)}")

    def _eval_null_rate(self, rule: _NullRateRule) -> DQResult:
        total = max(rule.total_rows, 1)
        rate_pct = (rule.null_count / total) * 100.0
        threshold_pct = rule.max_pct * 100.0
        warn_threshold_pct = (rule.warn_pct or rule.max_pct) * 100.0

        if rate_pct > threshold_pct:
            if rule.action == ACTION_FAIL:
                severity = SEVERITY_CRITICAL
            else:
                severity = SEVERITY_WARNING
            action_taken = "pipeline_failed" if rule.action == ACTION_FAIL else (
                "quarantined" if rule.action == ACTION_QUARANTINE else "warn"
            )
            rows_quarantined = rule.null_count if rule.action == ACTION_QUARANTINE else 0
        elif rule.warn_pct is not None and rate_pct > warn_threshold_pct:
            severity = SEVERITY_WARNING
            action_taken = "warn"
            rows_quarantined = 0
        else:
            severity = SEVERITY_PASS
            action_taken = "none"
            rows_quarantined = 0

        return DQResult(
            rule_id="null_rate",
            column=rule.column,
            severity=severity,
            failure_rate_pct=round(rate_pct, 4),
            failed_row_count=rule.null_count,
            action_taken=action_taken,
            rows_quarantined=rows_quarantined,
        )

    def _eval_volume(self, rule: _VolumeRule) -> DQResult:
        violated = False
        if rule.min_rows is not None and rule.row_count < rule.min_rows:
            violated = True
            diff = rule.min_rows - rule.row_count
            rate_pct = (diff / rule.min_rows) * 100.0
            failed = diff
        elif rule.max_rows is not None and rule.row_count > rule.max_rows:
            violated = True
            diff = rule.row_count - rule.max_rows
            rate_pct = (diff / rule.max_rows) * 100.0
            failed = diff
        else:
            rate_pct = 0.0
            failed = 0

        if violated:
            severity = SEVERITY_CRITICAL
            action_taken = "pipeline_failed" if rule.action == ACTION_FAIL else "warn"
        else:
            severity = SEVERITY_PASS
            action_taken = "none"

        return DQResult(
            rule_id="volume_check",
            column=None,
            severity=severity,
            failure_rate_pct=round(rate_pct, 4),
            failed_row_count=failed,
            action_taken=action_taken,
        )

    def _eval_freshness(self, rule: _FreshnessRule) -> DQResult:
        if rule.data_age_hours > rule.max_age_hours:
            severity = SEVERITY_WARNING
            action_taken = "pipeline_failed" if rule.action == ACTION_FAIL else "warn"
            excess_hours = rule.data_age_hours - rule.max_age_hours
            rate_pct = (excess_hours / rule.max_age_hours) * 100.0
        else:
            severity = SEVERITY_PASS
            action_taken = "none"
            rate_pct = 0.0

        return DQResult(
            rule_id="freshness_check",
            column=None,
            severity=severity,
            failure_rate_pct=round(rate_pct, 4),
            failed_row_count=0,
            action_taken=action_taken,
        )

    def _eval_schema(self, rule: _SchemaRule) -> DQResult:
        missing = rule.expected_columns - rule.actual_columns
        added = rule.actual_columns - rule.expected_columns
        drift = bool(missing or added)

        if drift:
            parts = []
            if missing:
                parts.append(f"removed: {sorted(missing)}")
            if added:
                parts.append(f"added: {sorted(added)}")
            drift_detail = "; ".join(parts)
            severity = SEVERITY_CRITICAL
            action_taken = "pipeline_failed" if rule.action == ACTION_FAIL else "warn"
        else:
            drift_detail = ""
            severity = SEVERITY_PASS
            action_taken = "none"

        return DQResult(
            rule_id="schema_check",
            column=None,
            severity=severity,
            failure_rate_pct=0.0,
            failed_row_count=len(missing) + len(added),
            action_taken=action_taken,
            schema_drift_detected=drift,
            schema_drift_detail=drift_detail,
        )

    # ── Span emission ─────────────────────────────────────────────────────────

    def _emit_event(self, result: DQResult) -> None:
        if self._span is None:
            return
        try:
            self._span.add_event(
                f"dq.check.{result.rule_id}",
                attributes=result.span_event_attributes(),
            )
        except Exception as exc:
            logger.warning("watch_obs.dq: failed to emit span event: %s", exc)


# ── Exception ─────────────────────────────────────────────────────────────────

class DataQualityFailure(Exception):
    """Raised by DataQualityChecker.evaluate() when a critical rule uses action=fail."""

    def __init__(self, message: str, results: list[DQResult]):
        super().__init__(message)
        self.results = results
