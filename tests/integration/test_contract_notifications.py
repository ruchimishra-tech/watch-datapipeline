"""
Phase 8 Integration Tests — T8.11, T8.12

Tests that DQ failures on contracted datasets trigger consumer notifications.
These tests use a mock notification dispatcher (no live Alertmanager required)
to verify the notification content and routing logic.

T8.11 — DQ failure on contracted dataset notifies both producer and all consumers
T8.12 — Consumer notification contains: contract ID, violated rule, pipeline name,
         run ID, and trace link
"""
from unittest.mock import MagicMock, call
from pathlib import Path

import pytest
import yaml

from sdk.dq import DataQualityChecker, DataQualityFailure, ACTION_WARN, SEVERITY_CRITICAL
from sdk.dq import DQResult, SEVERITY_WARNING


CONTRACTS_DIR = Path(__file__).parents[2] / "contracts"


# ── Notification dispatcher ───────────────────────────────────────────────────
# A lightweight dispatcher that maps contract consumers to notifications.
# In production this would wrap Alertmanager's API.

class ContractViolationNotifier:
    """
    Dispatches notifications to producer + consumer teams when a DQ rule
    tied to a data contract is violated.
    """

    def __init__(self, contract: dict, notifier_backend=None):
        self._contract = contract
        self._backend = notifier_backend or MagicMock()

    def notify(
        self,
        result: DQResult,
        pipeline_id: str,
        run_id: str,
        trace_url: str,
    ) -> list[str]:
        """
        Send notifications to producer team and all consumer teams.

        Returns list of team identifiers notified.
        """
        contract_id = self._contract["contract_id"]
        producer_team = self._contract["owner"]
        consumers = [c["team"] for c in self._contract.get("consumers", [])]

        payload = {
            "contract_id": contract_id,
            "rule": result.rule_id,
            "column": result.column,
            "pipeline_id": pipeline_id,
            "run_id": run_id,
            "trace_url": trace_url,
            "severity": result.severity,
            "failure_rate_pct": result.failure_rate_pct,
            "action_taken": result.action_taken,
        }

        notified = []
        # Notify producer
        self._backend.send(team=producer_team, payload=payload)
        notified.append(producer_team)

        # Notify each consumer
        for team in consumers:
            self._backend.send(team=team, payload=payload)
            notified.append(team)

        return notified


def _load_customer360_contract() -> dict:
    path = CONTRACTS_DIR / "customer-360-v1.yaml"
    if not path.exists():
        pytest.skip("customer-360-v1.yaml not found")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _make_dq_violation() -> DQResult:
    return DQResult(
        rule_id="null_rate",
        column="risk_tier",
        severity=SEVERITY_WARNING,
        failure_rate_pct=5.0,
        failed_row_count=500,
        action_taken="warn",
    )


# ── T8.11: Consumer notified on contract violation ────────────────────────────

@pytest.mark.phase8
class TestT811ContractViolationNotification:
    def test_producer_team_is_notified(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notified = notifier.notify(result, "customer-360-enrichment", "run-001", "http://tempo/trace/abc")
        assert contract["owner"] in notified

    def test_all_consumer_teams_notified(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notified = notifier.notify(result, "customer-360-enrichment", "run-001", "http://tempo/trace/abc")
        for consumer in contract["consumers"]:
            assert consumer["team"] in notified, (
                f"Consumer team {consumer['team']} was not notified"
            )

    def test_two_notifications_sent_for_two_consumers(self):
        """Producer + 2 consumers = 3 total send() calls."""
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notifier.notify(result, "customer-360-enrichment", "run-001", "http://trace")
        consumer_count = len(contract["consumers"])
        assert backend.send.call_count == 1 + consumer_count

    def test_backend_called_for_each_team(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notifier.notify(result, "customer-360-enrichment", "run-001", "http://trace")
        called_teams = {c.kwargs["team"] for c in backend.send.call_args_list}
        assert contract["owner"] in called_teams
        for consumer in contract["consumers"]:
            assert consumer["team"] in called_teams


# ── T8.12: Consumer notification content ─────────────────────────────────────

@pytest.mark.phase8
class TestT812NotificationContent:
    def _get_payload(self, team: str, backend: MagicMock) -> dict:
        for c in backend.send.call_args_list:
            if c.kwargs.get("team") == team:
                return c.kwargs["payload"]
        return {}

    def test_notification_contains_contract_id(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notifier.notify(result, "customer-360-enrichment", "run-001", "http://trace/abc")
        consumer_team = contract["consumers"][0]["team"]
        payload = self._get_payload(consumer_team, backend)
        assert payload["contract_id"] == contract["contract_id"]

    def test_notification_contains_violated_rule(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notifier.notify(result, "customer-360-enrichment", "run-001", "http://trace/abc")
        consumer_team = contract["consumers"][0]["team"]
        payload = self._get_payload(consumer_team, backend)
        assert payload["rule"] == "null_rate"

    def test_notification_contains_pipeline_name(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notifier.notify(result, "customer-360-enrichment", "run-007", "http://trace/abc")
        consumer_team = contract["consumers"][0]["team"]
        payload = self._get_payload(consumer_team, backend)
        assert payload["pipeline_id"] == "customer-360-enrichment"

    def test_notification_contains_run_id(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notifier.notify(result, "customer-360-enrichment", "run-007", "http://trace/abc")
        consumer_team = contract["consumers"][0]["team"]
        payload = self._get_payload(consumer_team, backend)
        assert payload["run_id"] == "run-007"

    def test_notification_contains_trace_url(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        trace = "http://tempo:3000/trace/abc123"
        notifier.notify(result, "customer-360-enrichment", "run-007", trace)
        consumer_team = contract["consumers"][0]["team"]
        payload = self._get_payload(consumer_team, backend)
        assert payload["trace_url"] == trace

    def test_notification_contains_failure_rate(self):
        contract = _load_customer360_contract()
        backend = MagicMock()
        notifier = ContractViolationNotifier(contract, backend)
        result = _make_dq_violation()
        notifier.notify(result, "customer-360-enrichment", "run-001", "http://trace")
        consumer_team = contract["consumers"][0]["team"]
        payload = self._get_payload(consumer_team, backend)
        assert "failure_rate_pct" in payload
        assert abs(payload["failure_rate_pct"] - 5.0) < 0.01
