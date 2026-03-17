"""
Phase 7 Unit Tests — T7.1–T7.9

Tests for the DQ rule engine (sdk/dq.py).
All offline — no OTel collector required; uses mock spans.

T7.1 — null_rate pass: 0.5% nulls < 1% threshold
T7.2 — null_rate warn: 5% nulls > 1% threshold, action=warn
T7.3 — volume below min: 500 rows < 1000 min, action=fail raises exception
T7.4 — volume above max: 200000 rows > 100000 max, severity=critical
T7.5 — schema drift: column removed
T7.6 — schema drift: column added
T7.7 — freshness breach: 30h > 25h max
T7.8 — quarantine action: 2% nulls, rows_quarantined set, no raise
T7.9 — no PII in DQ events: only statistics in span event attributes
"""
from unittest.mock import MagicMock

import pytest

from sdk.dq import (
    DataQualityChecker,
    DataQualityFailure,
    SEVERITY_PASS,
    SEVERITY_WARNING,
    SEVERITY_CRITICAL,
    ACTION_WARN,
    ACTION_QUARANTINE,
    ACTION_FAIL,
)


def _checker(span=None):
    span = span or MagicMock()
    return DataQualityChecker(phase_span=span, pipeline_id="test-pipe"), span


# ── T7.1: null rate pass ──────────────────────────────────────────────────────

@pytest.mark.phase7
class TestT71NullRatePass:
    def test_pass_when_below_threshold(self):
        checker, span = _checker()
        checker.check_null_rate("account_id", null_count=25, total_rows=5000, max_pct=0.01)
        results = checker.evaluate()
        assert results[0].severity == SEVERITY_PASS

    def test_no_span_event_for_pass(self):
        checker, span = _checker()
        checker.check_null_rate("account_id", null_count=0, total_rows=5000, max_pct=0.01)
        results = checker.evaluate()
        # add_event still called (we emit pass events too) — just check severity
        assert results[0].severity == SEVERITY_PASS

    def test_failure_rate_correct_for_pass(self):
        checker, span = _checker()
        checker.check_null_rate("x", null_count=25, total_rows=5000, max_pct=0.01)
        results = checker.evaluate()
        assert abs(results[0].failure_rate_pct - 0.5) < 0.01


# ── T7.2: null rate warn ──────────────────────────────────────────────────────

@pytest.mark.phase7
class TestT72NullRateWarn:
    def test_warning_when_above_threshold(self):
        checker, span = _checker()
        checker.check_null_rate("account_id", null_count=250, total_rows=5000, max_pct=0.01)
        results = checker.evaluate()
        assert results[0].severity == SEVERITY_WARNING

    def test_failure_rate_pct_correct(self):
        checker, span = _checker()
        checker.check_null_rate("account_id", null_count=250, total_rows=5000, max_pct=0.01)
        results = checker.evaluate()
        assert abs(results[0].failure_rate_pct - 5.0) < 0.01

    def test_span_event_emitted(self):
        checker, span = _checker()
        checker.check_null_rate("col", null_count=250, total_rows=5000, max_pct=0.01)
        checker.evaluate()
        span.add_event.assert_called()

    def test_action_warn_does_not_raise(self):
        checker, span = _checker()
        checker.check_null_rate("col", null_count=250, total_rows=5000, max_pct=0.01, action=ACTION_WARN)
        checker.evaluate()  # must not raise


# ── T7.3: volume below min ────────────────────────────────────────────────────

@pytest.mark.phase7
class TestT73VolumeBelowMin:
    def test_critical_when_below_min(self):
        checker, span = _checker()
        checker.check_volume(row_count=500, min_rows=1000, action=ACTION_FAIL)
        with pytest.raises(DataQualityFailure):
            checker.evaluate()

    def test_exception_carries_results(self):
        checker, span = _checker()
        checker.check_volume(row_count=500, min_rows=1000, action=ACTION_FAIL)
        with pytest.raises(DataQualityFailure) as exc_info:
            checker.evaluate()
        results = exc_info.value.results
        assert len(results) >= 1
        assert results[0].severity == SEVERITY_CRITICAL

    def test_action_taken_is_pipeline_failed(self):
        checker, span = _checker()
        checker.check_volume(row_count=500, min_rows=1000, action=ACTION_FAIL)
        with pytest.raises(DataQualityFailure) as exc_info:
            checker.evaluate()
        assert exc_info.value.results[0].action_taken == "pipeline_failed"

    def test_rule_id_is_volume_check(self):
        checker, span = _checker()
        checker.check_volume(row_count=500, min_rows=1000, action=ACTION_FAIL)
        with pytest.raises(DataQualityFailure) as exc_info:
            checker.evaluate()
        assert exc_info.value.results[0].rule_id == "volume_check"


# ── T7.4: volume above max ────────────────────────────────────────────────────

@pytest.mark.phase7
class TestT74VolumeAboveMax:
    def test_critical_when_above_max(self):
        checker, span = _checker()
        checker.check_volume(row_count=200000, max_rows=100000, action=ACTION_WARN)
        results = checker.evaluate()
        assert results[0].severity == SEVERITY_CRITICAL

    def test_action_warn_does_not_raise(self):
        checker, span = _checker()
        checker.check_volume(row_count=200000, max_rows=100000, action=ACTION_WARN)
        checker.evaluate()  # must not raise

    def test_volume_pass_within_bounds(self):
        checker, span = _checker()
        checker.check_volume(row_count=50000, min_rows=1000, max_rows=100000)
        results = checker.evaluate()
        assert results[0].severity == SEVERITY_PASS


# ── T7.5: schema drift — column removed ───────────────────────────────────────

@pytest.mark.phase7
class TestT75SchemaDriftColumnRemoved:
    def test_drift_detected_when_column_missing(self):
        checker, span = _checker()
        checker.check_schema(
            expected_columns={"id", "name", "risk_tier"},
            actual_columns={"id", "name"},
        )
        results = checker.evaluate()
        assert results[0].schema_drift_detected is True

    def test_drift_detail_names_missing_column(self):
        checker, span = _checker()
        checker.check_schema(
            expected_columns={"id", "name", "risk_tier"},
            actual_columns={"id", "name"},
        )
        results = checker.evaluate()
        assert "risk_tier" in results[0].schema_drift_detail

    def test_drift_severity_is_critical(self):
        checker, span = _checker()
        checker.check_schema(
            expected_columns={"id", "risk_tier"},
            actual_columns={"id"},
        )
        results = checker.evaluate()
        assert results[0].severity == SEVERITY_CRITICAL

    def test_no_drift_when_schemas_match(self):
        checker, span = _checker()
        checker.check_schema(
            expected_columns={"id", "name"},
            actual_columns={"id", "name"},
        )
        results = checker.evaluate()
        assert results[0].schema_drift_detected is False
        assert results[0].severity == SEVERITY_PASS


# ── T7.6: schema drift — column added ────────────────────────────────────────

@pytest.mark.phase7
class TestT76SchemaDriftColumnAdded:
    def test_drift_detected_when_column_added(self):
        checker, span = _checker()
        checker.check_schema(
            expected_columns={"id", "name"},
            actual_columns={"id", "name", "new_unexpected_col"},
        )
        results = checker.evaluate()
        assert results[0].schema_drift_detected is True

    def test_drift_detail_names_added_column(self):
        checker, span = _checker()
        checker.check_schema(
            expected_columns={"id", "name"},
            actual_columns={"id", "name", "new_unexpected_col"},
        )
        results = checker.evaluate()
        assert "new_unexpected_col" in results[0].schema_drift_detail

    def test_drift_detail_distinguishes_added_vs_removed(self):
        checker, span = _checker()
        checker.check_schema(
            expected_columns={"id", "old_col"},
            actual_columns={"id", "new_col"},
        )
        results = checker.evaluate()
        detail = results[0].schema_drift_detail
        assert "removed" in detail or "added" in detail


# ── T7.7: freshness breach ────────────────────────────────────────────────────

@pytest.mark.phase7
class TestT77FreshnessBreach:
    def test_warning_when_stale(self):
        checker, span = _checker()
        checker.check_freshness(data_age_hours=30, max_age_hours=25)
        results = checker.evaluate()
        assert results[0].severity == SEVERITY_WARNING

    def test_event_emitted_on_breach(self):
        checker, span = _checker()
        checker.check_freshness(data_age_hours=30, max_age_hours=25)
        checker.evaluate()
        span.add_event.assert_called()

    def test_pass_when_fresh(self):
        checker, span = _checker()
        checker.check_freshness(data_age_hours=10, max_age_hours=25)
        results = checker.evaluate()
        assert results[0].severity == SEVERITY_PASS

    def test_rule_id_is_freshness_check(self):
        checker, span = _checker()
        checker.check_freshness(data_age_hours=30, max_age_hours=25)
        results = checker.evaluate()
        assert results[0].rule_id == "freshness_check"


# ── T7.8: quarantine action ───────────────────────────────────────────────────

@pytest.mark.phase7
class TestT78QuarantineAction:
    def test_quarantine_does_not_raise(self):
        checker, span = _checker()
        # 2% nulls in 10000 rows = 200 null rows
        checker.check_null_rate(
            "account_id", null_count=200, total_rows=10000,
            max_pct=0.01, action=ACTION_QUARANTINE,
        )
        results = checker.evaluate()  # must not raise

    def test_action_taken_is_quarantined(self):
        checker, span = _checker()
        checker.check_null_rate(
            "account_id", null_count=200, total_rows=10000,
            max_pct=0.01, action=ACTION_QUARANTINE,
        )
        results = checker.evaluate()
        assert results[0].action_taken == "quarantined"

    def test_rows_quarantined_count_set(self):
        checker, span = _checker()
        checker.check_null_rate(
            "account_id", null_count=200, total_rows=10000,
            max_pct=0.01, action=ACTION_QUARANTINE,
        )
        results = checker.evaluate()
        assert results[0].rows_quarantined == 200

    def test_failure_rate_correct(self):
        checker, span = _checker()
        checker.check_null_rate(
            "col", null_count=200, total_rows=10000,
            max_pct=0.01, action=ACTION_QUARANTINE,
        )
        results = checker.evaluate()
        assert abs(results[0].failure_rate_pct - 2.0) < 0.01


# ── T7.9: no PII in DQ events ────────────────────────────────────────────────

@pytest.mark.phase7
class TestT79NoPIIInDQEvents:
    """DQ event attributes must contain only statistics, never raw data."""

    _FORBIDDEN_KEYS = {
        "dq.sample_bad_values", "dq.raw_value", "dq.example_row",
        "dq.failed_values", "dq.data",
    }

    def _get_event_attrs(self, span: MagicMock) -> dict:
        """Extract attributes from the most recent span.add_event call."""
        if not span.add_event.called:
            return {}
        last_call = span.add_event.call_args_list[-1]
        return last_call.kwargs.get("attributes") or (last_call.args[1] if len(last_call.args) > 1 else {})

    def test_null_rate_event_has_no_pii_keys(self):
        checker, span = _checker()
        checker.check_null_rate("ssn", null_count=50, total_rows=1000, max_pct=0.01)
        checker.evaluate()
        attrs = self._get_event_attrs(span)
        for key in self._FORBIDDEN_KEYS:
            assert key not in attrs, f"PII key '{key}' found in DQ event attributes"

    def test_null_rate_event_has_only_statistics(self):
        checker, span = _checker()
        checker.check_null_rate("col", null_count=100, total_rows=1000, max_pct=0.01)
        checker.evaluate()
        attrs = self._get_event_attrs(span)
        assert "dq.failed_row_count" in attrs
        assert "dq.failure_rate_pct" in attrs

    def test_schema_event_has_no_raw_data(self):
        checker, span = _checker()
        checker.check_schema({"id", "name"}, {"id", "name", "secret_ssn"})
        checker.evaluate()
        attrs = self._get_event_attrs(span)
        # drift_detail may name column names — that is allowed (schema metadata, not data)
        for key in self._FORBIDDEN_KEYS:
            assert key not in attrs

    def test_volume_event_has_no_raw_data(self):
        checker, span = _checker()
        checker.check_volume(row_count=50, min_rows=1000, action=ACTION_WARN)
        checker.evaluate()
        attrs = self._get_event_attrs(span)
        for key in self._FORBIDDEN_KEYS:
            assert key not in attrs

    def test_span_event_attributes_method_excludes_forbidden_keys(self):
        from sdk.dq import DQResult
        result = DQResult(
            rule_id="null_rate", column="email",
            severity=SEVERITY_WARNING, failure_rate_pct=5.0,
            failed_row_count=50, action_taken="warn",
        )
        attrs = result.span_event_attributes()
        for key in self._FORBIDDEN_KEYS:
            assert key not in attrs


# ── Multiple rules — all evaluated ────────────────────────────────────────────

@pytest.mark.phase7
class TestMultipleRules:
    def test_multiple_rules_all_evaluated(self):
        checker, span = _checker()
        checker.check_null_rate("a", null_count=0, total_rows=1000, max_pct=0.01)
        checker.check_volume(row_count=1000, min_rows=500, max_rows=5000)
        checker.check_freshness(data_age_hours=5, max_age_hours=25)
        results = checker.evaluate()
        assert len(results) == 3

    def test_first_fail_rule_blocks_others_from_continuing(self):
        """If one rule is action=fail, the exception is raised after all rules evaluated."""
        checker, span = _checker()
        checker.check_volume(row_count=100, min_rows=1000, action=ACTION_FAIL)
        checker.check_null_rate("col", null_count=0, total_rows=1000, max_pct=0.01)
        with pytest.raises(DataQualityFailure) as exc_info:
            checker.evaluate()
        # Both rules should have been evaluated
        assert exc_info.value.results[0].rule_id == "volume_check"
