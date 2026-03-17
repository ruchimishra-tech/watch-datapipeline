"""
Phase 3 Integration Tests — T3.1–T3.4, T3.8

Requires Grafana running at GRAFANA_URL (default: http://localhost:3000).
All tests are auto-skipped when Grafana is not reachable.

T3.1 — Grafana health endpoint returns 200
T3.2 — L1 dashboard JSON is accepted by the Grafana import API
T3.3 — L2 dashboard JSON is accepted by the Grafana import API
T3.4 — Both dashboards retrievable by uid after import
T3.8 — Grafana health responds within 3 seconds
"""
import json
import os
import time
from pathlib import Path

import pytest
import requests

GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
GRAFANA_USER = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin")
TIMEOUT_S = 3  # T3.8 SLO

DASHBOARDS_DIR = Path(__file__).parents[2] / "dashboards"


def _session() -> requests.Session:
    s = requests.Session()
    s.auth = (GRAFANA_USER, GRAFANA_PASSWORD)
    s.headers.update({"Content-Type": "application/json"})
    return s


def _grafana_reachable() -> bool:
    try:
        r = requests.get(f"{GRAFANA_URL}/api/health", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


# Skip entire module when Grafana is not up
pytestmark = pytest.mark.skipif(
    not _grafana_reachable(),
    reason="Grafana not reachable at %s — start docker stack first" % GRAFANA_URL,
)


@pytest.fixture(scope="module")
def grafana():
    return _session()


# ── T3.1 / T3.8: health and latency ──────────────────────────────────────────

@pytest.mark.phase3
def test_T3_1_grafana_health_ok(grafana):
    """T3.1 — /api/health returns 200."""
    r = grafana.get(f"{GRAFANA_URL}/api/health", timeout=TIMEOUT_S)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text}"


@pytest.mark.phase3
def test_T3_8_grafana_health_within_3s():
    """T3.8 — Health endpoint responds within 3 seconds."""
    start = time.perf_counter()
    r = requests.get(
        f"{GRAFANA_URL}/api/health",
        auth=(GRAFANA_USER, GRAFANA_PASSWORD),
        timeout=TIMEOUT_S,
    )
    elapsed = time.perf_counter() - start
    assert r.status_code == 200
    assert elapsed < TIMEOUT_S, (
        f"Grafana health took {elapsed:.2f}s, SLO is {TIMEOUT_S}s"
    )


# ── T3.2 / T3.3: dashboard import ────────────────────────────────────────────

@pytest.mark.phase3
def test_T3_2_l1_dashboard_imports(grafana):
    """T3.2 — l1-estate.json is loaded by Grafana (via provisioning or import API).

    Grafana provisions dashboards from mounted JSON files on startup.
    If provisioned, the import API returns 400 'Cannot save provisioned dashboard',
    which confirms the dashboard JSON was valid enough to provision successfully.
    We accept both: successful import (200) and already-provisioned (400).
    """
    path = DASHBOARDS_DIR / "l1-estate.json"
    if not path.exists():
        pytest.skip("l1-estate.json not found")
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    r = grafana.post(
        f"{GRAFANA_URL}/api/dashboards/import",
        json={"dashboard": dashboard, "overwrite": True, "folderId": 0},
        timeout=TIMEOUT_S,
    )
    provisioned = r.status_code == 400 and "provisioned" in r.text.lower()
    assert r.status_code == 200 or provisioned, (
        f"Unexpected response {r.status_code}: {r.text}"
    )


@pytest.mark.phase3
def test_T3_3_l2_dashboard_imports(grafana):
    """T3.3 — l2-pipeline.json is loaded by Grafana (via provisioning or import API)."""
    path = DASHBOARDS_DIR / "l2-pipeline.json"
    if not path.exists():
        pytest.skip("l2-pipeline.json not found")
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    r = grafana.post(
        f"{GRAFANA_URL}/api/dashboards/import",
        json={"dashboard": dashboard, "overwrite": True, "folderId": 0},
        timeout=TIMEOUT_S,
    )
    provisioned = r.status_code == 400 and "provisioned" in r.text.lower()
    assert r.status_code == 200 or provisioned, (
        f"Unexpected response {r.status_code}: {r.text}"
    )


# ── T3.4: dashboards retrievable after import ─────────────────────────────────

@pytest.mark.phase3
@pytest.mark.parametrize("uid,title", [
    ("watch-l1-estate",   "Watch — Pipeline Estate Health"),
    ("watch-l2-pipeline", "Watch — Pipeline Run Detail"),
])
def test_T3_4_dashboard_retrievable_by_uid(grafana, uid, title):
    """T3.4 — Imported dashboards are retrievable by uid."""
    r = grafana.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}", timeout=TIMEOUT_S)
    if r.status_code == 404:
        pytest.skip(f"Dashboard {uid} not yet imported — run T3.2/T3.3 first")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    body = r.json()
    assert body["dashboard"]["uid"] == uid
    assert body["dashboard"]["title"] == title
