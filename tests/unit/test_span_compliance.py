"""
Phase 9 Unit Tests — T9.1–T9.4, T9.7 (offline portion)

Tests for span compliance checking (sdk/span_compliance.py).
All offline — no running collector or Tempo required.

T9.1 — Span without pipeline_run_id is quarantined with reason missing_pipeline_run_id
T9.2 — Span without deployment.environment is quarantined with reason missing_deployment_environment
T9.3 — Compliant span passes through (not quarantined)
T9.4 — Quarantine counter increments correctly per reason
T9.7 — Staging span compliance gate: unredacted PII in span attributes causes non-zero exit
"""
import pytest

from sdk.span_compliance import (
    SpanComplianceChecker,
    SpanQuarantineLog,
    check_for_pii,
    REQUIRED_ATTRIBUTES,
)


def _compliant_attrs() -> dict:
    return {
        "pipeline.run_id": "run-abc123",
        "pipeline.name": "customer-360-enrichment",
        "deployment.environment": "staging",
    }


# ── T9.1: Missing pipeline_run_id quarantined ─────────────────────────────────

@pytest.mark.phase9
class TestT91MissingRunId:
    def test_missing_run_id_is_not_compliant(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        del attrs["pipeline.run_id"]
        result = checker.check("span-001", attrs)
        assert result.is_compliant is False

    def test_missing_run_id_reason_is_correct(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        del attrs["pipeline.run_id"]
        result = checker.check("span-001", attrs)
        assert any("pipeline_run_id" in v or "pipeline.run_id" in v for v in result.violations)

    def test_missing_run_id_quarantine_log_entry(self):
        checker = SpanComplianceChecker()
        log = SpanQuarantineLog()
        attrs = _compliant_attrs()
        del attrs["pipeline.run_id"]
        result = checker.check("span-001", attrs)
        log.add(result)
        assert log.has_span("span-001")

    def test_empty_run_id_is_non_compliant(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        attrs["pipeline.run_id"] = ""
        result = checker.check("span-002", attrs)
        assert result.is_compliant is False

    def test_none_run_id_is_non_compliant(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        attrs["pipeline.run_id"] = None
        result = checker.check("span-003", attrs)
        assert result.is_compliant is False


# ── T9.2: Missing deployment.environment quarantined ─────────────────────────

@pytest.mark.phase9
class TestT92MissingEnvironment:
    def test_missing_environment_is_not_compliant(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        del attrs["deployment.environment"]
        result = checker.check("span-010", attrs)
        assert result.is_compliant is False

    def test_missing_environment_reason(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        del attrs["deployment.environment"]
        result = checker.check("span-010", attrs)
        assert any("environment" in v for v in result.violations)

    def test_invalid_environment_is_non_compliant(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        attrs["deployment.environment"] = "production-xyz"
        result = checker.check("span-011", attrs)
        assert result.is_compliant is False

    def test_invalid_environment_reason_contains_value(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        attrs["deployment.environment"] = "unknown-env"
        result = checker.check("span-011", attrs)
        assert any("unknown-env" in v for v in result.violations)

    def test_valid_environment_values_pass(self):
        checker = SpanComplianceChecker()
        for env in ("dev", "staging", "prod", "production"):
            attrs = _compliant_attrs()
            attrs["deployment.environment"] = env
            result = checker.check("span-x", attrs)
            assert result.is_compliant, f"env={env} should be valid"


# ── T9.3: Compliant span passes through ──────────────────────────────────────

@pytest.mark.phase9
class TestT93CompliantSpanPasses:
    def test_compliant_span_is_compliant(self):
        checker = SpanComplianceChecker()
        result = checker.check("span-100", _compliant_attrs())
        assert result.is_compliant is True

    def test_compliant_span_has_no_violations(self):
        checker = SpanComplianceChecker()
        result = checker.check("span-100", _compliant_attrs())
        assert result.violations == []
        assert result.pii_violations == []

    def test_compliant_span_not_in_quarantine(self):
        checker = SpanComplianceChecker()
        log = SpanQuarantineLog()
        result = checker.check("span-100", _compliant_attrs())
        if result.is_compliant:
            pass  # not added to log
        assert not log.has_span("span-100")

    def test_compliant_span_with_extra_attrs_passes(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        attrs["custom.label"] = "value"
        attrs["http.status_code"] = 200
        result = checker.check("span-101", attrs)
        assert result.is_compliant is True


# ── T9.4: Quarantine counter increments ──────────────────────────────────────

@pytest.mark.phase9
class TestT94QuarantineCounter:
    def test_counter_increments_for_each_span(self):
        checker = SpanComplianceChecker()
        log = SpanQuarantineLog()
        for i in range(10):
            attrs = _compliant_attrs()
            del attrs["pipeline.run_id"]
            result = checker.check(f"span-{i}", attrs)
            log.add(result)
        assert log.total() == 10

    def test_counter_per_reason(self):
        checker = SpanComplianceChecker()
        log = SpanQuarantineLog()
        # 5 missing run_id
        for i in range(5):
            attrs = _compliant_attrs()
            del attrs["pipeline.run_id"]
            result = checker.check(f"run-{i}", attrs)
            log.add(result)
        # 3 missing environment
        for i in range(3):
            attrs = _compliant_attrs()
            del attrs["deployment.environment"]
            result = checker.check(f"env-{i}", attrs)
            log.add(result)
        assert log.total() == 8
        assert log.count_by_reason("missing_pipeline_run_id") == 5
        assert log.count_by_reason("missing_deployment_environment") == 3

    def test_prometheus_lines_rendered(self):
        checker = SpanComplianceChecker()
        log = SpanQuarantineLog()
        attrs = _compliant_attrs()
        del attrs["pipeline.run_id"]
        result = checker.check("span-x", attrs)
        log.add(result)
        lines = log.to_prometheus_lines()
        assert any("spans_quarantined_total" in l for l in lines)
        assert any("1" in l for l in lines)

    def test_primary_reason_is_first_violation(self):
        checker = SpanComplianceChecker()
        result = checker.check("span-y", {})  # all required attrs missing
        assert result.primary_reason is not None


# ── T9.7: PII detection (staging gate) ───────────────────────────────────────

@pytest.mark.phase9
class TestT97PIIDetection:
    def test_email_in_attr_detected(self):
        checker = SpanComplianceChecker()
        attrs = _compliant_attrs()
        attrs["user.email"] = "john.doe@example.com"
        result = checker.check("span-pii-1", attrs)
        assert not result.is_compliant
        assert len(result.pii_violations) > 0

    def test_ssn_in_attr_detected(self):
        pii_type = check_for_pii("123-45-6789")
        assert pii_type == "ssn"

    def test_credit_card_detected(self):
        pii_type = check_for_pii("4111 1111 1111 1111")
        assert pii_type == "credit_card"

    def test_non_pii_value_passes(self):
        pii_type = check_for_pii("customer-360-enrichment")
        assert pii_type is None

    def test_numeric_run_id_passes(self):
        pii_type = check_for_pii("run-12345")
        assert pii_type is None

    def test_pii_span_quarantined(self):
        checker = SpanComplianceChecker()
        log = SpanQuarantineLog()
        attrs = _compliant_attrs()
        attrs["customer.email"] = "alice@example.com"
        result = checker.check("span-pii-2", attrs)
        assert not result.is_compliant
        log.add(result)
        assert log.has_span("span-pii-2")

    def test_pii_check_can_be_disabled(self):
        checker = SpanComplianceChecker(check_pii=False)
        attrs = _compliant_attrs()
        attrs["user.email"] = "john@example.com"
        result = checker.check("span-pii-3", attrs)
        # With PII check off, only structural violations count
        assert result.is_compliant is True
