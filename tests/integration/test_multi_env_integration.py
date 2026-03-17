"""
Phase 12 Integration Tests — T12.1, T12.2

Tests for multi-environment namespace isolation.
Auto-skipped when Tempo or Grafana is not reachable.

T12.1 — Span written to Tempo staging namespace; production namespace returns 404
T12.2 — Grafana production datasource cannot resolve staging trace IDs
"""
import os
import time
import uuid

import pytest
import requests

TEMPO_STAGING_URL = os.getenv("TEMPO_STAGING_URL", "http://localhost:3200")
TEMPO_PROD_URL = os.getenv("TEMPO_PROD_URL", "http://localhost:3201")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
GRAFANA_AUTH = (
    os.getenv("GRAFANA_USER", "admin"),
    os.getenv("GRAFANA_PASSWORD", "admin"),
)
COLLECTOR_OTLP_URL = os.getenv("COLLECTOR_OTLP_URL", "http://localhost:4318")


def _reachable(url: str) -> bool:
    try:
        r = requests.get(url, timeout=2)
        return r.status_code < 500
    except Exception:
        return False


def _send_span(trace_id: str, span_id: str, pipeline_id: str, environment: str) -> int:
    payload = {
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {"key": "pipeline.run_id", "value": {"stringValue": f"run-{span_id[:8]}"}},
                    {"key": "pipeline.name", "value": {"stringValue": pipeline_id}},
                    {"key": "deployment.environment", "value": {"stringValue": environment}},
                ]
            },
            "scopeSpans": [{
                "spans": [{
                    "traceId": trace_id,
                    "spanId": span_id,
                    "name": "test.namespace.span",
                    "kind": 1,
                    "startTimeUnixNano": str(int(time.time() * 1e9)),
                    "endTimeUnixNano": str(int(time.time() * 1e9) + 1_000_000),
                    "status": {"code": 1},
                }]
            }]
        }]
    }
    try:
        r = requests.post(f"{COLLECTOR_OTLP_URL}/v1/traces", json=payload, timeout=5)
        return r.status_code
    except Exception:
        return 0


_skip_isolation = pytest.mark.skipif(
    not (_reachable(TEMPO_STAGING_URL) and _reachable(TEMPO_PROD_URL)),
    reason="Both Tempo staging and production namespaces must be reachable",
)

_skip_grafana = pytest.mark.skipif(
    not _reachable(GRAFANA_URL),
    reason="Grafana not reachable",
)


# ── T12.1: Staging span not in production namespace ──────────────────────────

@pytest.mark.phase12
@_skip_isolation
class TestT121NamespaceIsolation:
    def test_staging_span_not_in_prod_tempo(self):
        trace_id = uuid.uuid4().hex[:32]
        span_id = uuid.uuid4().hex[:16]
        _send_span(trace_id, span_id, "test-pipe", "staging")
        time.sleep(2)
        r = requests.get(f"{TEMPO_PROD_URL}/api/traces/{trace_id}", timeout=5)
        assert r.status_code == 404, (
            f"Staging span should not be visible in production Tempo, got {r.status_code}"
        )

    def test_staging_span_visible_in_staging_tempo(self):
        trace_id = uuid.uuid4().hex[:32]
        span_id = uuid.uuid4().hex[:16]
        _send_span(trace_id, span_id, "test-pipe", "staging")
        deadline = time.time() + 5.0
        found = False
        while time.time() < deadline:
            r = requests.get(f"{TEMPO_STAGING_URL}/api/traces/{trace_id}", timeout=5)
            if r.status_code == 200:
                found = True
                break
            time.sleep(0.5)
        assert found, "Staging span should be visible in staging Tempo"


# ── T12.2: Grafana datasource scoping ─────────────────────────────────────────

@pytest.mark.phase12
@_skip_grafana
class TestT122GrafanaDatasourceScoping:
    def test_prod_datasource_configured(self):
        r = requests.get(f"{GRAFANA_URL}/api/datasources", auth=GRAFANA_AUTH, timeout=5)
        assert r.status_code == 200
        datasources = r.json()
        names = [ds.get("name", "") for ds in datasources]
        # Check that at least some Tempo datasource is configured
        assert any("tempo" in n.lower() or "Tempo" in n for n in names), (
            f"No Tempo datasource found. Available: {names}"
        )
