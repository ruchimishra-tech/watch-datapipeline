"""
Phase 8 Unit Tests — T8.6, T8.7, T8.8, T8.9, T8.10, T8.13, T8.14

Tests for SLI computation (sdk/slo.py) and CI gate scripts
(ci/error_budget_gate.py, ci/schema_evolution_gate.py).
All offline — no Prometheus or live infrastructure required.

T8.6  — SLI compute: success rate
T8.7  — SLI compute: freshness
T8.8  — Error budget gate blocks when budget < threshold
T8.9  — Error budget gate allows when budget >= threshold
T8.10 — Error budget gate override with team lead approval
T8.13 — Schema evolution gate: column removal detected
T8.14 — Schema evolution gate: nullable column add is non-breaking
"""
import pytest

from sdk.slo import (
    compute_success_rate_sli,
    compute_freshness_sli,
    compute_latency_sli,
    compute_completeness_sli,
    compute_error_budget,
    ErrorBudgetStatus,
)
from ci.error_budget_gate import evaluate_gate, GateResult
from ci.schema_evolution_gate import (
    evaluate_schema_gate,
    parse_schema_diff,
    classify_changes,
)


# ── T8.6: SLI compute — success rate ─────────────────────────────────────────

@pytest.mark.phase8
class TestT86SuccessRateSLI:
    def test_4_success_1_failure_gives_0_80(self):
        sli = compute_success_rate_sli(success_count=4, total_count=5)
        assert abs(sli - 0.80) < 0.001

    def test_all_success(self):
        sli = compute_success_rate_sli(success_count=100, total_count=100)
        assert abs(sli - 1.0) < 0.001

    def test_all_failure(self):
        sli = compute_success_rate_sli(success_count=0, total_count=10)
        assert abs(sli - 0.0) < 0.001

    def test_no_runs_returns_1(self):
        # No data = assume good (avoid false alerts on new pipelines)
        sli = compute_success_rate_sli(success_count=0, total_count=0)
        assert sli == 1.0

    def test_sli_clamped_to_0_1(self):
        # Should never exceed 1.0
        sli = compute_success_rate_sli(success_count=110, total_count=100)
        assert sli <= 1.0


# ── T8.7: SLI compute — freshness ─────────────────────────────────────────

@pytest.mark.phase8
class TestT87FreshnessSLI:
    def test_3_within_sla_1_late_gives_0_75(self):
        # sla = 3600s; 3 runs ok, 1 run 10 minutes (600s) late
        sla = 3600
        durations = [1800.0, 2100.0, 3000.0, 4200.0]  # last one is 4200s > 3600s
        sli = compute_freshness_sli(durations, sla)
        assert abs(sli - 0.75) < 0.001

    def test_all_within_sla(self):
        sli = compute_freshness_sli([100.0, 200.0, 300.0], sla_seconds=3600)
        assert abs(sli - 1.0) < 0.001

    def test_all_late(self):
        sli = compute_freshness_sli([7200.0, 9000.0], sla_seconds=3600)
        assert abs(sli - 0.0) < 0.001

    def test_empty_list_returns_1(self):
        sli = compute_freshness_sli([], sla_seconds=3600)
        assert sli == 1.0

    def test_exactly_at_sla_boundary_is_good(self):
        sli = compute_freshness_sli([3600.0], sla_seconds=3600)
        assert abs(sli - 1.0) < 0.001

    def test_latency_sli_same_as_freshness(self):
        durations = [100.0, 200.0, 5000.0]
        sla = 3600
        assert compute_latency_sli(durations, sla) == compute_freshness_sli(durations, sla)

    def test_completeness_sli_within_range(self):
        row_counts = [1000, 2000, 500, 3000]
        sli = compute_completeness_sli(row_counts, min_rows=1000, max_rows=5000)
        # 500 is below min — 3 out of 4 are good
        assert abs(sli - 0.75) < 0.001

    def test_completeness_sli_all_good(self):
        sli = compute_completeness_sli([1000, 2000, 3000], min_rows=500, max_rows=10000)
        assert abs(sli - 1.0) < 0.001


# ── Error budget compute ───────────────────────────────────────────────────────

@pytest.mark.phase8
class TestErrorBudgetCompute:
    def test_budget_at_50pct_when_halfway_consumed(self):
        # target=99%, allowed_error=1%, actual_error=0.5% => budget_remaining=50%
        status = compute_error_budget("pipe", "freshness", target_pct=99.0, current_sli=0.995)
        assert abs(status.budget_remaining_pct - 50.0) < 0.5

    def test_budget_fully_remaining_when_meeting_slo(self):
        # SLI == 1.0 => no errors at all => 100% budget remaining
        status = compute_error_budget("pipe", "freshness", target_pct=99.0, current_sli=1.0)
        assert abs(status.budget_remaining_pct - 100.0) < 0.1

    def test_budget_zero_when_sli_at_target(self):
        # SLI exactly at target => 0% budget remaining
        status = compute_error_budget("pipe", "freshness", target_pct=99.0, current_sli=0.99)
        assert abs(status.budget_remaining_pct - 0.0) < 0.5

    def test_is_frozen_when_below_threshold(self):
        status = compute_error_budget("pipe", "freshness", target_pct=99.5,
                                      current_sli=0.99, freeze_threshold_pct=10.0)
        assert status.is_frozen is True

    def test_not_frozen_when_above_threshold(self):
        status = compute_error_budget("pipe", "freshness", target_pct=99.5,
                                      current_sli=0.999, freeze_threshold_pct=10.0)
        assert status.is_frozen is False


# ── T8.8: Error budget gate — blocks ────────────────────────────────────────

@pytest.mark.phase8
class TestT88GateBlocks:
    def test_gate_blocks_when_below_threshold(self):
        result = evaluate_gate(
            pipeline_id="test-pipe",
            budget_remaining_pct=8.0,
            freeze_threshold_pct=10.0,
        )
        assert result.allowed is False
        assert result.exit_code == 1

    def test_gate_block_message_contains_pipeline_id(self):
        result = evaluate_gate(
            pipeline_id="my-pipeline",
            budget_remaining_pct=5.0,
            freeze_threshold_pct=10.0,
        )
        assert "my-pipeline" in result.reason

    def test_gate_block_message_contains_budget_value(self):
        result = evaluate_gate(
            pipeline_id="pipe",
            budget_remaining_pct=8.0,
            freeze_threshold_pct=10.0,
        )
        assert "8.0" in result.reason or "8%" in result.reason

    def test_gate_block_at_zero_budget(self):
        result = evaluate_gate("pipe", budget_remaining_pct=0.0, freeze_threshold_pct=10.0)
        assert result.allowed is False

    def test_gate_block_exactly_at_threshold_minus_epsilon(self):
        result = evaluate_gate("pipe", budget_remaining_pct=9.99, freeze_threshold_pct=10.0)
        assert result.allowed is False


# ── T8.9: Error budget gate — allows ────────────────────────────────────────

@pytest.mark.phase8
class TestT89GateAllows:
    def test_gate_allows_when_above_threshold(self):
        result = evaluate_gate(
            pipeline_id="test-pipe",
            budget_remaining_pct=50.0,
            freeze_threshold_pct=10.0,
        )
        assert result.allowed is True
        assert result.exit_code == 0

    def test_gate_allows_exactly_at_threshold(self):
        result = evaluate_gate("pipe", budget_remaining_pct=10.0, freeze_threshold_pct=10.0)
        assert result.allowed is True

    def test_gate_allows_full_budget(self):
        result = evaluate_gate("pipe", budget_remaining_pct=100.0, freeze_threshold_pct=10.0)
        assert result.allowed is True

    def test_gate_allow_message_contains_budget_value(self):
        result = evaluate_gate("pipe", budget_remaining_pct=75.0, freeze_threshold_pct=10.0)
        assert "75.0" in result.reason


# ── T8.10: Error budget gate — override ─────────────────────────────────────

@pytest.mark.phase8
class TestT810GateOverride:
    def test_override_with_approval_allows(self):
        result = evaluate_gate(
            pipeline_id="test-pipe",
            budget_remaining_pct=8.0,
            freeze_threshold_pct=10.0,
            has_override_label=True,
            approver_teams=["team-lead-alice"],
        )
        assert result.allowed is True

    def test_override_without_approval_still_blocks(self):
        result = evaluate_gate(
            pipeline_id="test-pipe",
            budget_remaining_pct=8.0,
            freeze_threshold_pct=10.0,
            has_override_label=True,
            approver_teams=None,
        )
        assert result.allowed is False

    def test_override_reason_mentions_approver(self):
        result = evaluate_gate(
            pipeline_id="pipe",
            budget_remaining_pct=5.0,
            freeze_threshold_pct=10.0,
            has_override_label=True,
            approver_teams=["team-lead-alice", "team-lead-bob"],
        )
        assert "team-lead-alice" in result.reason or "team-lead-bob" in result.reason

    def test_no_override_label_blocks_regardless_of_approvers(self):
        result = evaluate_gate(
            pipeline_id="pipe",
            budget_remaining_pct=5.0,
            freeze_threshold_pct=10.0,
            has_override_label=False,
            approver_teams=["team-lead-alice"],
        )
        assert result.allowed is False


# ── T8.13: Schema evolution gate — column removal ────────────────────────────

@pytest.mark.phase8
class TestT813ColumnRemovalDetected:
    def test_column_removal_is_breaking(self):
        diff_lines = ["- risk_tier: string\n"]
        result = evaluate_schema_gate(diff_lines)
        assert result.has_breaking_changes is True

    def test_removed_column_name_in_result(self):
        diff_lines = ["- risk_tier: string\n"]
        result = evaluate_schema_gate(diff_lines)
        names = [c.name for c in result.breaking_changes]
        assert "risk_tier" in names

    def test_type_change_is_breaking(self):
        diff_lines = ["~ account_id: integer->string\n"]
        result = evaluate_schema_gate(diff_lines)
        assert result.has_breaking_changes is True

    def test_consumer_teams_listed_on_breaking_change(self):
        diff_lines = ["- risk_tier: string\n"]
        consumers = ["team-risk-analytics", "team-product-recommendations"]
        result = evaluate_schema_gate(diff_lines, contract_consumers=consumers)
        assert "team-risk-analytics" in result.affected_consumer_teams
        assert "team-product-recommendations" in result.affected_consumer_teams

    def test_no_consumers_listed_when_no_contract(self):
        diff_lines = ["- risk_tier: string\n"]
        result = evaluate_schema_gate(diff_lines, contract_consumers=None)
        assert result.affected_consumer_teams == []

    def test_parse_multiple_removals(self):
        diff_lines = [
            "- risk_tier: string\n",
            "- account_id: string\n",
        ]
        changes = parse_schema_diff(diff_lines)
        removed = [c.name for c in changes if c.change_type == "removed"]
        assert "risk_tier" in removed
        assert "account_id" in removed


# ── T8.14: Schema evolution gate — nullable add permitted ────────────────────

@pytest.mark.phase8
class TestT814NullableAddPermitted:
    def test_nullable_column_add_is_non_breaking(self):
        diff_lines = ["+ new_optional_field: string nullable\n"]
        result = evaluate_schema_gate(diff_lines)
        assert result.has_breaking_changes is False

    def test_nullable_add_exit_code_is_0(self):
        diff_lines = ["+ new_col: integer nullable\n"]
        result = evaluate_schema_gate(diff_lines)
        assert not result.has_breaking_changes  # exit code 0

    def test_added_column_in_non_breaking_list(self):
        diff_lines = ["+ extra_col: string nullable\n"]
        result = evaluate_schema_gate(diff_lines)
        names = [c.name for c in result.non_breaking_changes]
        assert "extra_col" in names

    def test_no_consumer_approval_needed_for_add(self):
        diff_lines = ["+ extra_col: string nullable\n"]
        consumers = ["team-a", "team-b"]
        result = evaluate_schema_gate(diff_lines, contract_consumers=consumers)
        # No breaking change => no affected consumers
        assert result.affected_consumer_teams == []

    def test_mixed_breaking_and_nonbreaking(self):
        diff_lines = [
            "- risk_tier: string\n",    # breaking
            "+ new_col: boolean nullable\n",  # non-breaking
        ]
        result = evaluate_schema_gate(diff_lines)
        assert result.has_breaking_changes is True
        assert len(result.non_breaking_changes) == 1
        assert len(result.breaking_changes) == 1
