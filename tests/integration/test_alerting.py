"""
Phase 4 Integration Tests — T4.5–T4.8

Requires the full docker stack running (Prometheus + Alertmanager).
Tests are auto-skipped when Prometheus is not reachable.

T4.5 — PipelineRunFailed alert rule is loaded by Prometheus
T4.6 — Alertmanager is reachable and has the watch receiver configured
T4.7 — Alert inhibition rule is configured (critical suppresses delay warning)
T4.8 — PipelineRunDelayed alert rule is loaded by Prometheus
"""
import os
import time

import pytest
import requests

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://localhost:9093")
TIMEOUT_S = 5


def _prom_reachable() -> bool:
    try:
        r = requests.get(f"{PROMETHEUS_URL}/-/healthy", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def _am_reachable() -> bool:
    try:
        r = requests.get(f"{ALERTMANAGER_URL}/-/healthy", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _prom_reachable(),
    reason="Prometheus not reachable at %s" % PROMETHEUS_URL,
)


# ── T4.5 / T4.8: Alert rules loaded ──────────────────────────────────────────

@pytest.mark.phase4
@pytest.mark.parametrize("alert_name", [
    "PipelineRunFailed",
    "PipelineRunDelayed",
    "PipelineSLABreached",
    "PipelineDQCriticalViolationHigh",
])
def test_alert_rule_loaded_in_prometheus(alert_name):
    """T4.5 / T4.8 — Alert rules from alerts/ are loaded by Prometheus."""
    r = requests.get(
        f"{PROMETHEUS_URL}/api/v1/rules",
        timeout=TIMEOUT_S,
    )
    assert r.status_code == 200, f"Rules API failed: {r.text}"

    body = r.json()
    all_rules = [
        rule
        for group in body.get("data", {}).get("groups", [])
        for rule in group.get("rules", [])
    ]
    rule_names = [rule.get("name") for rule in all_rules]
    assert alert_name in rule_names, (
        f"Alert '{alert_name}' not found in Prometheus rules. "
        f"Loaded rules: {rule_names}"
    )


# ── T4.6: Alertmanager is up and has watch receivers ─────────────────────────

@pytest.mark.phase4
def test_alertmanager_healthy():
    """T4.6 — Alertmanager is reachable and healthy."""
    if not _am_reachable():
        pytest.skip("Alertmanager not reachable at %s" % ALERTMANAGER_URL)
    r = requests.get(f"{ALERTMANAGER_URL}/-/healthy", timeout=TIMEOUT_S)
    assert r.status_code == 200


@pytest.mark.phase4
def test_alertmanager_has_watch_receiver():
    """T4.6 — Alertmanager config has a Watch receiver (watch-ui, slack-warnings, or watch-alerts)."""
    if not _am_reachable():
        pytest.skip("Alertmanager not reachable at %s" % ALERTMANAGER_URL)
    r = requests.get(f"{ALERTMANAGER_URL}/api/v2/status", timeout=TIMEOUT_S)
    assert r.status_code == 200
    body = r.json()
    config = body.get("config", {}).get("original", "")
    assert any(kw in config for kw in ("watch-ui", "slack-warnings", "watch-alerts")), (
        "Alertmanager config should contain a Watch-namespaced receiver"
    )


# ── T4.7: Inhibition rules are configured ─────────────────────────────────────

@pytest.mark.phase4
def test_alertmanager_inhibition_rules_configured():
    """T4.7 — Inhibition rules present: critical failure suppresses delay warning."""
    if not _am_reachable():
        pytest.skip("Alertmanager not reachable at %s" % ALERTMANAGER_URL)
    r = requests.get(f"{ALERTMANAGER_URL}/api/v2/status", timeout=TIMEOUT_S)
    assert r.status_code == 200
    body = r.json()
    config = body.get("config", {}).get("original", "")
    assert "inhibit_rules" in config, (
        "Alertmanager config should have inhibit_rules section"
    )


# ── Prometheus rules file valid syntax check ──────────────────────────────────

@pytest.mark.phase4
def test_prometheus_rules_have_no_errors():
    """All loaded alert rules are error-free (no PromQL parse failures)."""
    r = requests.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=TIMEOUT_S)
    assert r.status_code == 200

    body = r.json()
    errors = []
    for group in body.get("data", {}).get("groups", []):
        for rule in group.get("rules", []):
            if rule.get("health") == "err":
                errors.append(f"{rule.get('name')}: {rule.get('lastError')}")

    assert not errors, f"Alert rules with errors: {errors}"
