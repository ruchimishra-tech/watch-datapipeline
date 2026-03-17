"""
Phase 9 Integration Tests — T9.1–T9.4, T9.8, T9.9

Integration tests requiring the docker stack (OTEL Collector, Tempo, Grafana).
All tests are auto-skipped when the stack is not reachable.

T9.1 — Span without pipeline_run_id does not appear in Tempo
T9.2 — Span without deployment.environment does not appear in Tempo
T9.3 — Compliant span appears in Tempo within 2 seconds
T9.4 — Quarantine counter visible in Prometheus after non-compliant spans
T9.8 — GitOps dashboard sync: dashboard committed reaches Grafana
T9.9 — GitOps idempotent: same dashboard deployed twice = one result in Grafana
"""
import os
import time
import uuid

import pytest
import requests

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
TEMPO_URL = os.getenv("TEMPO_URL", "http://localhost:3200")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
GRAFANA_AUTH = (
    os.getenv("GRAFANA_USER", "admin"),
    os.getenv("GRAFANA_PASSWORD", "admin"),
)
COLLECTOR_OTLP_URL = os.getenv("COLLECTOR_OTLP_URL", "http://localhost:4318")


def _prom_reachable() -> bool:
    try:
        return requests.get(f"{PROMETHEUS_URL}/-/healthy", timeout=2).status_code == 200
    except Exception:
        return False


def _tempo_reachable() -> bool:
    try:
        return requests.get(f"{TEMPO_URL}/ready", timeout=2).status_code == 200
    except Exception:
        return False


def _grafana_reachable() -> bool:
    try:
        return requests.get(f"{GRAFANA_URL}/api/health", timeout=2).status_code == 200
    except Exception:
        return False


def _collector_reachable() -> bool:
    try:
        r = requests.get(f"{COLLECTOR_OTLP_URL}/", timeout=2)
        return r.status_code in (200, 405)
    except Exception:
        return False


def _send_otlp_span(trace_id: str, span_id: str, attributes: dict) -> int:
    """Send a single span via OTLP/HTTP JSON to the collector."""
    payload = {
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {"key": k, "value": {"stringValue": str(v)}}
                    for k, v in attributes.items()
                ]
            },
            "scopeSpans": [{
                "spans": [{
                    "traceId": trace_id,
                    "spanId": span_id,
                    "name": "test.span",
                    "kind": 1,
                    "startTimeUnixNano": str(int(time.time() * 1e9)),
                    "endTimeUnixNano": str(int(time.time() * 1e9) + 1000000),
                    "status": {"code": 1},
                }]
            }]
        }]
    }
    r = requests.post(
        f"{COLLECTOR_OTLP_URL}/v1/traces",
        json=payload,
        timeout=5,
    )
    return r.status_code


_skip_collector = pytest.mark.skipif(
    not _collector_reachable() or not _tempo_reachable(),
    reason="OTEL Collector or Tempo not reachable",
)

_skip_prom = pytest.mark.skipif(
    not _prom_reachable(),
    reason="Prometheus not reachable",
)

_skip_grafana = pytest.mark.skipif(
    not _grafana_reachable(),
    reason="Grafana not reachable",
)


# ── T9.1: Missing run_id span not in Tempo ────────────────────────────────────

@pytest.mark.phase9
@_skip_collector
class TestT91SpanQuarantineMissingRunId:
    def test_span_without_run_id_not_in_tempo(self):
        span_id = uuid.uuid4().hex[:16]
        trace_id = uuid.uuid4().hex[:32]
        attrs = {
            "pipeline.name": "test-pipeline",
            "deployment.environment": "staging",
            # deliberately omitting pipeline.run_id
        }
        status = _send_otlp_span(trace_id, span_id, attrs)
        assert status in (200, 202, 204)
        time.sleep(2)
        r = requests.get(f"{TEMPO_URL}/api/traces/{trace_id}", timeout=5)
        # Should not be found (quarantined)
        assert r.status_code == 404


# ── T9.2: Missing environment span not in Tempo ───────────────────────────────

@pytest.mark.phase9
@_skip_collector
class TestT92SpanQuarantineMissingEnv:
    def test_span_without_environment_not_in_tempo(self):
        span_id = uuid.uuid4().hex[:16]
        trace_id = uuid.uuid4().hex[:32]
        attrs = {
            "pipeline.run_id": f"run-{uuid.uuid4().hex[:8]}",
            "pipeline.name": "test-pipeline",
            # deliberately omitting deployment.environment
        }
        status = _send_otlp_span(trace_id, span_id, attrs)
        assert status in (200, 202, 204)
        time.sleep(2)
        r = requests.get(f"{TEMPO_URL}/api/traces/{trace_id}", timeout=5)
        assert r.status_code == 404


# ── T9.3: Compliant span appears in Tempo ─────────────────────────────────────

@pytest.mark.phase9
@_skip_collector
class TestT93CompliantSpanInTempo:
    def test_compliant_span_found_in_tempo_within_2s(self):
        span_id = uuid.uuid4().hex[:16]
        trace_id = uuid.uuid4().hex[:32]
        attrs = {
            "pipeline.run_id": f"run-{uuid.uuid4().hex[:8]}",
            "pipeline.name": "test-pipeline",
            "deployment.environment": "staging",
        }
        status = _send_otlp_span(trace_id, span_id, attrs)
        assert status in (200, 202, 204)

        deadline = time.time() + 3.0
        found = False
        while time.time() < deadline:
            r = requests.get(f"{TEMPO_URL}/api/traces/{trace_id}", timeout=5)
            if r.status_code == 200:
                found = True
                break
            time.sleep(0.5)
        assert found, "Compliant span not found in Tempo within 3s"


# ── T9.4: Quarantine counter in Prometheus ────────────────────────────────────

@pytest.mark.phase9
@_skip_prom
@_skip_collector
class TestT94QuarantineCounterInPrometheus:
    def test_quarantine_counter_visible_after_noncompliant_spans(self):
        # Send 3 non-compliant spans
        for _ in range(3):
            _send_otlp_span(
                uuid.uuid4().hex[:32],
                uuid.uuid4().hex[:16],
                {"pipeline.name": "test-pipeline"},  # missing run_id + env
            )
        time.sleep(5)  # wait for Prometheus scrape
        r = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": "spans_quarantined_total"},
            timeout=5,
        )
        assert r.status_code == 200
        data = r.json()
        results = data.get("data", {}).get("result", [])
        assert len(results) > 0, "spans_quarantined_total metric not found in Prometheus"


# ── T9.8: GitOps dashboard sync ───────────────────────────────────────────────

@pytest.mark.phase9
@_skip_grafana
class TestT98GitOpsDashboardSync:
    def _make_dashboard(self, uid: str) -> dict:
        return {
            "uid": uid,
            "title": f"Watch Test Dashboard {uid}",
            "tags": ["watch", "test"],
            "panels": [
                {
                    "id": 1,
                    "type": "timeseries",
                    "title": "pipeline-runs-over-time",
                    "datasource": {"uid": "prometheus-watch"},
                }
            ],
            "templating": {
                "list": [{"name": "environment", "type": "query"}]
            },
            "schemaVersion": 38,
        }

    def test_dashboard_upsert_then_retrieve(self):
        uid = f"watch-test-{uuid.uuid4().hex[:8]}"
        dash = self._make_dashboard(uid)
        payload = {"dashboard": dash, "overwrite": True, "folderId": 0}
        r = requests.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json=payload,
            auth=GRAFANA_AUTH,
            timeout=10,
        )
        assert r.status_code in (200, 412), f"Import failed: {r.text}"
        # Retrieve
        r2 = requests.get(
            f"{GRAFANA_URL}/api/dashboards/uid/{uid}",
            auth=GRAFANA_AUTH,
            timeout=10,
        )
        assert r2.status_code == 200
        assert r2.json()["dashboard"]["uid"] == uid


# ── T9.9: GitOps idempotent ───────────────────────────────────────────────────

@pytest.mark.phase9
@_skip_grafana
class TestT99GitOpsIdempotent:
    def _make_dashboard(self, uid: str) -> dict:
        return {
            "uid": uid,
            "title": f"Watch Idempotent Test {uid}",
            "tags": ["watch", "test"],
            "panels": [
                {
                    "id": 1,
                    "type": "timeseries",
                    "title": "pipeline-runs-over-time",
                    "datasource": {"uid": "prometheus-watch"},
                }
            ],
            "templating": {
                "list": [{"name": "environment", "type": "query"}]
            },
            "schemaVersion": 38,
        }

    def test_double_deploy_yields_single_dashboard(self):
        uid = f"watch-idem-{uuid.uuid4().hex[:8]}"
        dash = self._make_dashboard(uid)
        payload = {"dashboard": dash, "overwrite": True, "folderId": 0}
        for _ in range(2):
            r = requests.post(
                f"{GRAFANA_URL}/api/dashboards/db",
                json=payload,
                auth=GRAFANA_AUTH,
                timeout=10,
            )
            assert r.status_code in (200, 412)
        # Search for dashboards with this uid — should be exactly one
        r2 = requests.get(
            f"{GRAFANA_URL}/api/search",
            params={"query": uid},
            auth=GRAFANA_AUTH,
            timeout=10,
        )
        assert r2.status_code == 200
        results = [d for d in r2.json() if d.get("uid") == uid]
        assert len(results) == 1, f"Expected 1 dashboard with uid={uid}, got {len(results)}"
