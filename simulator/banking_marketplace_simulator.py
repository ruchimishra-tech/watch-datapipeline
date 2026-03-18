"""
Banking Data Marketplace — Observability Simulator
===================================================
Exposes realistic, scenario-driven Prometheus metrics for an enterprise
bank's Data Marketplace dashboard (marketplace-home.json).

Metric prefix : data_product_*
Labels        : portfolio, domain, product, owner, environment
HTTP endpoint : /metrics on BANKING_METRICS_PORT (default 8097)

Organisation hierarchy
  Enterprise Bank
    ├── Retail Banking
    │     ├── customer-data        → Customer 360
    │     ├── account-management   → Account Aggregation
    │     └── transaction-intel    → Transaction Analytics
    ├── Wealth Management
    │     ├── portfolio-analytics  → Portfolio Performance
    │     └── investment-research  → Investment Analytics
    ├── Commercial & Institutional
    │     ├── treasury             → Treasury Liquidity
    │     ├── corporate-banking    → Corporate Deposits
    │     └── trade                → Trade Finance
    ├── Fraud & Financial Crime
    │     ├── fraud-prevention     → Fraud Detection Engine
    │     └── financial-crime      → AML Monitoring
    │                              → Transaction Risk Scoring
    ├── Payments
    │     └── payments-processing  → Payments Settlement
    └── Risk & Compliance
          └── credit-risk          → Credit Risk Models

Scenarios (time-of-day, UK London timezone)
  • Customer 360          — morning CRM ingestion lag (08:30-10:30 wkday)
  • Portfolio Performance — weekend staleness + market-open/close spikes
  • Treasury Liquidity    — intermittent external Bloomberg feed spikes
  • Fraud Detection       — lunch & evening transaction volume spikes
  • AML Monitoring        — Monday weekly scan + end-of-month regulatory batch
  • Transaction Risk      — 7-day model drift cycle (gradual, then reset)
  • Payments Settlement   — overnight batch failure window (23:00-00:30)
  • Account Aggregation   — EOD volume surge & latency stress
  • Trade Finance         — multi-system latency variability during market hrs
"""
from __future__ import annotations

import logging
import math
import os
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Optional
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("watch.banking_sim")

METRICS_PORT    = int(os.getenv("BANKING_METRICS_PORT", "8097"))
ENVIRONMENT     = os.getenv("WATCH_DEPLOYMENT_ENVIRONMENT", "docker-dev")
UPDATE_INTERVAL = 15                        # seconds, matches Prometheus scrape
LONDON          = ZoneInfo("Europe/London")

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Product:
    """One data product in the marketplace with its observability baselines."""
    portfolio: str
    domain: str
    product: str
    owner: str

    # Steady-state baselines (achieved when no scenario is active)
    base_availability: float        # 0.0 – 1.0
    base_latency_p95_ms: float      # milliseconds
    base_freshness_seconds: float   # seconds since last successful refresh
    base_error_rate: float          # 0.0 – 1.0
    base_volume_rph: float          # records delivered per hour
    base_slo_compliance: float      # rolling-30d compliance ratio, 0.0 – 1.0

    # SLA thresholds — used for health score computation and alert rules
    sla_availability: float = 0.999
    sla_latency_ms:   float = 200.0
    sla_freshness_s:  float = 3600.0
    sla_error_rate:   float = 0.01

    # Optional time-aware scenario modifier
    # Signature: (Product, datetime_london) → dict of metric overrides
    scenario_fn: Optional[Callable] = field(default=None, repr=False)


# ── Metric store ──────────────────────────────────────────────────────────────

_lock    = threading.Lock()
_gauges: dict[str, float] = {}
_counters: dict[str, int] = {}      # SLA breach / DQ violation counters


def _key(name: str, labels: dict[str, str]) -> str:
    lstr = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{lstr}}}"


def _set(name: str, labels: dict, value: float) -> None:
    with _lock:
        _gauges[_key(name, labels)] = value


def _inc(name: str, labels: dict) -> None:
    with _lock:
        k = _key(name, labels)
        _counters[k] = _counters.get(k, 0) + 1


def _render() -> str:
    lines: list[str] = []
    with _lock:
        for k, v in _gauges.items():
            lines.append(f"{k} {v:.6f}")
        for k, v in _counters.items():
            lines.append(f"{k} {v}")
    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            body = _render().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path in ("/health", "/"):
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, *_):
        pass   # silence HTTP access log


# ── Health score ──────────────────────────────────────────────────────────────

def _dim_score(actual: float, sla: float, higher_is_better: bool = False) -> float:
    """Map a single dimension to 0-1 score.

    For lower-is-better dims (latency, freshness, error_rate):
      1.0 when actual ≤ 50% of SLA, degrades linearly, 0.0 when actual ≥ 2× SLA.
    For higher-is-better dims (availability):
      min(1, actual / sla).
    """
    if higher_is_better:
        return min(1.0, actual / sla if sla else 0.0)
    # Lower is better
    if sla <= 0:
        return 1.0
    threshold = sla * 0.5          # starts degrading here
    ceiling   = sla * 2.0          # zero score here
    score = 1.0 - max(0.0, actual - threshold) / max(1e-9, ceiling - threshold)
    return max(0.0, min(1.0, score))


def health_score(avail: float, lat: float, fresh: float, err: float,
                 prod: Product) -> float:
    a = _dim_score(avail, prod.sla_availability, higher_is_better=True)
    l = _dim_score(lat,   prod.sla_latency_ms)
    f = _dim_score(fresh, prod.sla_freshness_s)
    e = _dim_score(err,   prod.sla_error_rate)
    score = a * 0.35 + l * 0.25 + f * 0.25 + e * 0.15
    return round(max(0.0, min(1.0, score)), 4)


# ── Gaussian noise ────────────────────────────────────────────────────────────

def _jitter(value: float, pct: float = 0.03) -> float:
    """Add ±pct Gaussian noise.  Keeps sign and avoids zero for positive values."""
    noisy = value * (1.0 + random.gauss(0, pct))
    return max(0.0, noisy)


# ── Scenario functions ────────────────────────────────────────────────────────
# Each returns a dict of overrides. Only keys present are applied; missing keys
# fall back to product baselines.  Noise is applied AFTER merging.

def _hour(dt: datetime) -> float:
    return dt.hour + dt.minute / 60.0


# ── 1. Customer 360 — morning CRM ingestion lag ───────────────────────────────
# Root cause: Oracle CRM system runs bulk export 08:30 → competes with online
# transactions → export finishes late → downstream enrichment stale by 08:45-09:30.
# Recovery: CRM export completes ~10:30, freshness normalises.
def _scenario_customer_360(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()
    if dow >= 5:                                    # weekends: reduced but stable
        return {"volume_rph": prod.base_volume_rph * 0.25}

    if 8.5 <= h < 10.5:                            # lag window
        t = (h - 8.5) / 2.0                        # 0 → 1 over 2 hours
        severity = math.sin(math.pi * t) ** 2       # bell curve, peak ~09:30
        lag_s    = severity * 1800                  # up to +30 min freshness lag
        avail    = prod.base_availability - severity * 0.003
        return {
            "freshness_seconds": prod.base_freshness_seconds + lag_s,
            "availability":      max(0.98, avail),
            "latency_p95_ms":    prod.base_latency_p95_ms * (1 + severity * 0.4),
        }
    return {}


# ── 2. Account Aggregation — EOD batch surge ──────────────────────────────────
# Root cause: End-of-day account balance reconciliation kicks off 17:30.
# 3× normal record volume floods Snowflake writers → latency spikes.
def _scenario_account_aggregation(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()
    if dow >= 5:
        return {"volume_rph": prod.base_volume_rph * 0.15,
                "freshness_seconds": prod.base_freshness_seconds * 3}

    if 17.5 <= h < 19.5:                           # EOD surge window
        t       = (h - 17.5) / 2.0
        surge   = 1 + math.sin(math.pi * t) ** 2 * 2.2   # up to 3.2× volume
        lat_mul = 1 + math.sin(math.pi * t) ** 2 * 2.0
        return {
            "volume_rph":      prod.base_volume_rph * surge,
            "latency_p95_ms":  prod.base_latency_p95_ms * lat_mul,
            "error_rate":      prod.base_error_rate + (lat_mul - 1) * 0.005,
        }
    return {}


# ── 3. Transaction Analytics — legacy pipeline, chronic SLA miss ──────────────
# Root cause: Full-scan SQL on legacy MSSQL rather than incremental.
# Always slightly over SLA; DQ null spikes on weekends (weekend batch re-run).
def _scenario_transaction_analytics(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()
    if dow >= 5 and 1.0 <= h < 4.0:               # weekend reprocessing window
        return {
            "error_rate":      prod.base_error_rate * 3.5,
            "freshness_seconds": prod.base_freshness_seconds * 5,
            "latency_p95_ms":  prod.base_latency_p95_ms * 1.8,
        }
    return {}


# ── 4. Portfolio Performance — market-hours dependency ────────────────────────
# Root cause: Data sourced from Bloomberg real-time feed (07:00-17:30 UK).
# Outside those hours: data goes stale. Latency spikes at open (07:30), midday
# rebalancing (12:00), and market close (16:30).
def _scenario_portfolio_performance(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()

    if dow >= 5:                                    # weekend: market closed
        return {
            "freshness_seconds": prod.base_freshness_seconds * 48,  # ~2 days stale
            "availability":      0.65,              # degraded-mode only
            "latency_p95_ms":   prod.base_latency_p95_ms * 0.5,     # no real queries
        }

    if h < 7.0 or h >= 17.5:                       # outside market hours
        hours_since_close = max(0, h - 17.5) if h >= 17.5 else h + (24 - 17.5)
        return {
            "freshness_seconds": prod.base_freshness_seconds + hours_since_close * 3600,
            "latency_p95_ms":    prod.base_latency_p95_ms * 0.6,
        }

    # Market open spike (07:30-08:30)
    if 7.5 <= h < 8.5:
        t     = (h - 7.5) / 1.0
        spike = 1 + math.sin(math.pi * t) ** 2 * 3.0
        return {"latency_p95_ms": prod.base_latency_p95_ms * spike,
                "volume_rph":     prod.base_volume_rph * spike * 0.8}

    # Midday rebalancing (12:00-12:30)
    if 12.0 <= h < 12.5:
        return {"latency_p95_ms": prod.base_latency_p95_ms * 1.6}

    # Market close (16:30-17:30) — heaviest processing
    if 16.5 <= h < 17.5:
        t     = (h - 16.5) / 1.0
        spike = 1 + math.sin(math.pi * t) ** 2 * 4.0
        return {"latency_p95_ms": prod.base_latency_p95_ms * spike,
                "volume_rph":     prod.base_volume_rph * 2.5,
                "error_rate":     prod.base_error_rate + (spike - 1) * 0.006}

    return {}


# ── 5. Investment Analytics — end-of-quarter spike ────────────────────────────
# Root cause: Quarterly fund valuation generates 10× normal compute load.
# Simulated as recurring: last 3 days of every month (proxy for EOM/EOQ).
def _scenario_investment_analytics(prod: Product, now: datetime) -> dict:
    h, dow, day = _hour(now), now.weekday(), now.day

    if dow >= 5:
        return {"volume_rph": prod.base_volume_rph * 0.2}

    if day >= 28:                                   # EOM/EOQ computation surge
        severity = (day - 27) / 4.0                # 0.25 → 1.0 over last 4 days
        return {
            "latency_p95_ms":    prod.base_latency_p95_ms * (1 + severity * 3.0),
            "freshness_seconds": prod.base_freshness_seconds * (1 + severity * 2.0),
            "error_rate":        prod.base_error_rate + severity * 0.008,
            "volume_rph":        prod.base_volume_rph * (1 + severity * 1.5),
        }
    return {}


# ── 6. Treasury Liquidity — intermittent Bloomberg/Reuters feed spikes ─────────
# Root cause: External vendor SLA is 99.5% with uncapped latency tails.
# Spikes occur deterministically every 17 minutes (3-min window) so they appear
# consistently in the dashboard — realistic for a vendor with scheduled polling.
# Outside UK market hours: degraded (vendor in maintenance window).
def _scenario_treasury_liquidity(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()

    if dow >= 5:
        return {"availability": 0.75,
                "freshness_seconds": prod.base_freshness_seconds * 12}

    if not (7.0 <= h < 17.5):                      # outside market hours
        return {"availability": 0.88,
                "freshness_seconds": prod.base_freshness_seconds * 6,
                "latency_p95_ms":    prod.base_latency_p95_ms * 0.5}

    # Deterministic spike every 17 minutes (3-min window)
    minute_of_day = now.hour * 60 + now.minute
    spike_cycle   = minute_of_day % 17              # 0 – 16
    if spike_cycle < 3:
        rng          = random.Random(minute_of_day) # deterministic per minute
        spike_factor = rng.uniform(3.0, 10.0)       # vendor tail latency
        timeout      = spike_factor > 6.0
        return {
            "latency_p95_ms": prod.base_latency_p95_ms * spike_factor,
            "error_rate":     prod.base_error_rate + (0.04 if timeout else 0.01),
            "availability":   prod.base_availability - (0.03 if timeout else 0.005),
        }
    return {}


# ── 7. Corporate Deposits — stable but end-of-month reporting load ────────────
def _scenario_corporate_deposits(prod: Product, now: datetime) -> dict:
    day = now.day
    if day >= 28:
        severity = (day - 27) / 4.0
        return {
            "latency_p95_ms":    prod.base_latency_p95_ms * (1 + severity * 1.5),
            "freshness_seconds": prod.base_freshness_seconds * (1 + severity),
        }
    if now.weekday() >= 5:
        return {"volume_rph": prod.base_volume_rph * 0.1}
    return {}


# ── 8. Trade Finance — multi-system integration latency ───────────────────────
# Root cause: SWIFT, correspondent bank APIs, FX rates from ECB — any one can
# add latency tail. Variability is structural, not an incident.
def _scenario_trade_finance(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()
    if dow >= 5:
        return {"availability": 0.80, "volume_rph": prod.base_volume_rph * 0.05}

    # Asia-Pacific trade window (00:00-09:00): SWIFT correspondent banks active
    if h < 9.0:
        return {"latency_p95_ms": prod.base_latency_p95_ms * 1.8,
                "error_rate":     prod.base_error_rate * 1.6}

    # UK/EU window (09:00-17:30): primary window, lowest latency
    if 9.0 <= h < 17.5:
        return {}

    # After London close (17:30+): hand-off to NY, latency rises
    return {"latency_p95_ms": prod.base_latency_p95_ms * 2.2,
            "error_rate":     prod.base_error_rate * 1.4}


# ── 9. Fraud Detection Engine — peak transaction volume latency ───────────────
# Root cause: Synchronous ML inference in the payment auth path.
# Load increases at lunch (commuters buying) and evening (online retail).
# Weekends: elevated Saturday evening fraud attempt spike.
def _scenario_fraud_detection(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()

    def _peak(h_start, h_end, peak_factor):
        if h_start <= h < h_end:
            t     = (h - h_start) / (h_end - h_start)
            spike = 1 + math.sin(math.pi * t) ** 2 * (peak_factor - 1)
            return {
                "latency_p95_ms": prod.base_latency_p95_ms * spike,
                "error_rate":     prod.base_error_rate + max(0, spike - 2.0) * 0.012,
                "volume_rph":     prod.base_volume_rph * spike,
            }
        return None

    if dow == 5:                                    # Saturday evening fraud spike
        if h >= 20.0:
            return {"latency_p95_ms": prod.base_latency_p95_ms * 2.2,
                    "error_rate":     prod.base_error_rate * 2.0,
                    "volume_rph":     prod.base_volume_rph * 1.6}

    if dow < 5:
        for start, end, factor in [(8.0, 9.5, 2.0), (12.0, 13.5, 3.5), (17.0, 19.0, 4.0)]:
            r = _peak(start, end, factor)
            if r:
                return r
    return {}


# ── 10. AML Monitoring — weekly scan + end-of-month regulatory batch ──────────
# Root cause (weekly): Automated transaction screening refreshes every Monday
# 02:00-06:00 UTC — large batch joins with sanctions lists and name databases.
# Root cause (monthly): EOM regulatory filing requires full-history re-scan.
def _scenario_aml_monitoring(prod: Product, now: datetime) -> dict:
    h, dow, day = _hour(now), now.weekday(), now.day

    # EOM regulatory scan (days 28-31, 01:00-08:00) — overrides weekly scan
    if day >= 28 and 1.0 <= h < 8.0:
        severity = (day - 27) / 4.0
        t        = (h - 1.0) / 7.0
        bell     = math.sin(math.pi * t) ** 2
        return {
            "freshness_seconds": prod.base_freshness_seconds + bell * 72000 * severity,  # up to 20h lag
            "latency_p95_ms":    prod.base_latency_p95_ms * (1 + bell * 8 * severity),
            "error_rate":        prod.base_error_rate + bell * 0.015,
            "availability":      prod.base_availability - bell * 0.025,
        }

    # Weekly Monday batch scan
    if dow == 0 and 2.0 <= h < 6.0:
        t    = (h - 2.0) / 4.0
        bell = math.sin(math.pi * t) ** 2
        return {
            "freshness_seconds": prod.base_freshness_seconds + bell * 43200,  # up to +12h
            "latency_p95_ms":    prod.base_latency_p95_ms * (1 + bell * 5),
            "availability":      prod.base_availability - bell * 0.015,
        }

    # Normally: batch, running every 6h
    return {}


# ── 11. Transaction Risk Scoring — 7-day model drift cycle ────────────────────
# Root cause: Feature distribution shift (new merchant categories, geo patterns)
# causes XGBoost model confidence to drift. Every 7 days an auto-retrain runs.
# Visible as: error_rate climbs 0.015 → 0.14 then resets (model swap at midnight).
def _scenario_transaction_risk_scoring(prod: Product, now: datetime) -> dict:
    # 7-day sine cycle — deterministic on wall time, not uptime
    cycle  = (now.timestamp() % (7 * 86400)) / (7 * 86400)  # 0 → 1 over 7 days
    drift  = math.sin(math.pi * cycle) ** 2                  # 0 → 1 → 0

    err    = 0.015 + drift * 0.125                           # 0.015 – 0.14
    lat    = prod.base_latency_p95_ms * (1 + drift * 1.0)   # up to 2× at peak drift
    slo    = prod.base_slo_compliance - drift * 0.085        # SLO compliance degrades

    # Near day-7 reset (cycle > 0.95): brief outage as model reloads
    if cycle > 0.95:
        reload_t = (cycle - 0.95) / 0.05                    # 0 → 1
        return {
            "error_rate":         err + reload_t * 0.03,
            "latency_p95_ms":     lat * (1 + reload_t * 2),
            "availability":       prod.base_availability - reload_t * 0.05,
            "slo_compliance":     max(0.90, slo),
        }

    return {"error_rate": err, "latency_p95_ms": lat, "slo_compliance": slo}


# ── 12. Payments Settlement — overnight batch failure ─────────────────────────
# Root cause: CHAPS/BACS settlement batch runs 22:00-02:00.
# Simulated incident: database lock contention at ~23:00 causes batch to stall.
# Partial recovery (manual intervention): 00:30.  Full recovery by 02:00.
def _scenario_payments_settlement(prod: Product, now: datetime) -> dict:
    h, dow = _hour(now), now.weekday()

    # Settlement window weeknights (Mon-Thu night, i.e. Mon 22:00 → Fri 02:00)
    in_settlement = (dow < 4 and h >= 22.0) or (dow < 5 and h < 2.0)

    if not in_settlement:
        # Daytime: healthy, very low volume (just status APIs)
        return {"volume_rph": prod.base_volume_rph * 0.15}

    # Batch start: 22:00-23:00 — healthy but high volume
    if 22.0 <= h < 23.0:
        return {
            "volume_rph":     prod.base_volume_rph * 8.0,
            "latency_p95_ms": prod.base_latency_p95_ms * 2.0,
        }

    # Batch failure: 23:00-00:30 (hour wraps to <0.5)
    if h >= 23.0 or h < 0.5:
        severity = (h - 23.0) / 1.5 if h >= 23.0 else (h + 1.0) / 1.5
        severity = min(1.0, max(0.0, severity))
        return {
            "availability":      0.70 + severity * 0.10,   # degrades then partial fix
            "error_rate":        0.20 - severity * 0.08,   # peaks 20%, drops to 12%
            "freshness_seconds": prod.base_freshness_seconds * 10,
            "latency_p95_ms":    prod.base_latency_p95_ms * 6,
            "volume_rph":        prod.base_volume_rph * 2,  # retry storms
        }

    # Partial recovery: 00:30-02:00
    t = (h - 0.5) / 1.5                            # 0 → 1
    return {
        "availability":      0.78 + t * (prod.base_availability - 0.78),
        "error_rate":        0.12 - t * (0.12 - prod.base_error_rate),
        "freshness_seconds": prod.base_freshness_seconds * (8 - t * 7),
        "latency_p95_ms":    prod.base_latency_p95_ms * (4 - t * 3),
    }


# ── 13. Credit Risk Models — EOM regulatory reporting ────────────────────────
# Root cause: Basel III / ICAAP risk capital calculations run end-of-month.
# Heavy model ensemble (LGD, PD, EAD) + audit trail generation.
def _scenario_credit_risk_models(prod: Product, now: datetime) -> dict:
    h, dow, day = _hour(now), now.weekday(), now.day

    if dow >= 5:
        return {"volume_rph": prod.base_volume_rph * 0.3}

    if day >= 28:
        severity = min(1.0, (day - 27) / 4.0)
        return {
            "latency_p95_ms":    prod.base_latency_p95_ms * (1 + severity * 4.0),
            "freshness_seconds": prod.base_freshness_seconds * (1 + severity * 3.0),
            "volume_rph":        prod.base_volume_rph * (1 + severity * 2.0),
            "error_rate":        prod.base_error_rate + severity * 0.006,
        }
    return {}


# ── Product catalogue ─────────────────────────────────────────────────────────

PRODUCTS: list[Product] = [

    # ── Retail Banking ────────────────────────────────────────────────────────
    Product(
        portfolio="Retail Banking", domain="Customer Data",
        product="Customer 360",    owner="Retail Platform Team",
        base_availability=0.999,  base_latency_p95_ms=85,
        base_freshness_seconds=300,  base_error_rate=0.002,
        base_volume_rph=150_000,  base_slo_compliance=0.998,
        sla_availability=0.999, sla_latency_ms=200, sla_freshness_s=900, sla_error_rate=0.01,
        scenario_fn=_scenario_customer_360,
    ),
    Product(
        portfolio="Retail Banking", domain="Account Management",
        product="Account Aggregation", owner="Retail Accounts Team",
        base_availability=0.997,  base_latency_p95_ms=120,
        base_freshness_seconds=600,  base_error_rate=0.003,
        base_volume_rph=90_000,   base_slo_compliance=0.995,
        sla_availability=0.999, sla_latency_ms=300, sla_freshness_s=1800, sla_error_rate=0.01,
        scenario_fn=_scenario_account_aggregation,
    ),
    Product(
        portfolio="Retail Banking", domain="Transaction Intelligence",
        product="Transaction Analytics", owner="Retail Analytics Team",
        base_availability=0.994,  base_latency_p95_ms=340,    # chronic SLA miss
        base_freshness_seconds=3200, base_error_rate=0.006,
        base_volume_rph=220_000,  base_slo_compliance=0.982,
        sla_availability=0.999, sla_latency_ms=250, sla_freshness_s=3600, sla_error_rate=0.01,
        scenario_fn=_scenario_transaction_analytics,
    ),

    # ── Wealth Management ─────────────────────────────────────────────────────
    Product(
        portfolio="Wealth Management", domain="Portfolio Analytics",
        product="Portfolio Performance", owner="Wealth Analytics Team",
        base_availability=0.997,  base_latency_p95_ms=180,
        base_freshness_seconds=900,  base_error_rate=0.003,
        base_volume_rph=45_000,   base_slo_compliance=0.994,
        sla_availability=0.999, sla_latency_ms=500, sla_freshness_s=3600, sla_error_rate=0.01,
        scenario_fn=_scenario_portfolio_performance,
    ),
    Product(
        portfolio="Wealth Management", domain="Investment Research",
        product="Investment Analytics", owner="Wealth Research Team",
        base_availability=0.995,  base_latency_p95_ms=250,
        base_freshness_seconds=1800, base_error_rate=0.004,
        base_volume_rph=18_000,   base_slo_compliance=0.991,
        sla_availability=0.999, sla_latency_ms=600, sla_freshness_s=7200, sla_error_rate=0.015,
        scenario_fn=_scenario_investment_analytics,
    ),

    # ── Commercial & Institutional ────────────────────────────────────────────
    Product(
        portfolio="Commercial & Institutional", domain="Treasury",
        product="Treasury Liquidity", owner="C&I Treasury Team",
        base_availability=0.993,  base_latency_p95_ms=210,
        base_freshness_seconds=120,  base_error_rate=0.005,
        base_volume_rph=12_000,   base_slo_compliance=0.987,
        sla_availability=0.995, sla_latency_ms=500, sla_freshness_s=300, sla_error_rate=0.02,
        scenario_fn=_scenario_treasury_liquidity,
    ),
    Product(
        portfolio="Commercial & Institutional", domain="Corporate Banking",
        product="Corporate Deposits", owner="C&I Corporate Team",
        base_availability=0.998,  base_latency_p95_ms=160,
        base_freshness_seconds=1800, base_error_rate=0.002,
        base_volume_rph=8_000,    base_slo_compliance=0.996,
        sla_availability=0.999, sla_latency_ms=400, sla_freshness_s=3600, sla_error_rate=0.01,
        scenario_fn=_scenario_corporate_deposits,
    ),
    Product(
        portfolio="Commercial & Institutional", domain="Trade Finance",
        product="Trade Finance", owner="C&I Trade Team",
        base_availability=0.990,  base_latency_p95_ms=480,
        base_freshness_seconds=2400, base_error_rate=0.012,
        base_volume_rph=5_500,    base_slo_compliance=0.975,
        sla_availability=0.995, sla_latency_ms=1000, sla_freshness_s=7200, sla_error_rate=0.02,
        scenario_fn=_scenario_trade_finance,
    ),

    # ── Fraud & Financial Crime ────────────────────────────────────────────────
    Product(
        portfolio="Fraud & Financial Crime", domain="Fraud Prevention",
        product="Fraud Detection Engine", owner="Fraud Prevention Team",
        base_availability=0.9995, base_latency_p95_ms=42,  # real-time, strict SLA
        base_freshness_seconds=30,   base_error_rate=0.0015,
        base_volume_rph=800_000,  base_slo_compliance=0.999,
        sla_availability=0.9999, sla_latency_ms=100, sla_freshness_s=60, sla_error_rate=0.005,
        scenario_fn=_scenario_fraud_detection,
    ),
    Product(
        portfolio="Fraud & Financial Crime", domain="Financial Crime",
        product="AML Monitoring", owner="FinCrime Team",
        base_availability=0.993,  base_latency_p95_ms=1800,  # batch, higher tolerance
        base_freshness_seconds=21600, base_error_rate=0.006,  # 6h freshness SLA
        base_volume_rph=35_000,   base_slo_compliance=0.988,
        sla_availability=0.990, sla_latency_ms=5000, sla_freshness_s=86400, sla_error_rate=0.02,
        scenario_fn=_scenario_aml_monitoring,
    ),
    Product(
        portfolio="Fraud & Financial Crime", domain="Financial Crime",
        product="Transaction Risk Scoring", owner="Risk Scoring Team",
        base_availability=0.998,  base_latency_p95_ms=38,   # near-real-time scoring
        base_freshness_seconds=45,   base_error_rate=0.015,  # model drift starts here
        base_volume_rph=600_000,  base_slo_compliance=0.995,
        sla_availability=0.999, sla_latency_ms=80, sla_freshness_s=120, sla_error_rate=0.05,
        scenario_fn=_scenario_transaction_risk_scoring,
    ),

    # ── Payments ──────────────────────────────────────────────────────────────
    Product(
        portfolio="Payments", domain="Payments Processing",
        product="Payments Settlement", owner="Payments Ops Team",
        base_availability=0.9995, base_latency_p95_ms=95,
        base_freshness_seconds=180,  base_error_rate=0.001,
        base_volume_rph=500_000,  base_slo_compliance=0.9995,
        sla_availability=0.9999, sla_latency_ms=200, sla_freshness_s=300, sla_error_rate=0.005,
        scenario_fn=_scenario_payments_settlement,
    ),

    # ── Risk & Compliance ─────────────────────────────────────────────────────
    Product(
        portfolio="Risk & Compliance", domain="Credit Risk",
        product="Credit Risk Models", owner="Risk Analytics Team",
        base_availability=0.997,  base_latency_p95_ms=320,
        base_freshness_seconds=7200, base_error_rate=0.003,
        base_volume_rph=25_000,   base_slo_compliance=0.993,
        sla_availability=0.999, sla_latency_ms=1000, sla_freshness_s=14400, sla_error_rate=0.01,
        scenario_fn=_scenario_credit_risk_models,
    ),
]


# ── Metric update loop ────────────────────────────────────────────────────────

def _compute_and_publish(prod: Product, now_london: datetime) -> None:
    """Apply scenario modifiers, add noise, then write all metrics."""
    env = ENVIRONMENT

    # Start from baselines
    avail   = prod.base_availability
    lat     = prod.base_latency_p95_ms
    fresh   = prod.base_freshness_seconds
    err     = prod.base_error_rate
    vol     = prod.base_volume_rph
    slo     = prod.base_slo_compliance

    # Apply scenario overrides
    if prod.scenario_fn:
        overrides = prod.scenario_fn(prod, now_london)
        avail = overrides.get("availability",      avail)
        lat   = overrides.get("latency_p95_ms",    lat)
        fresh = overrides.get("freshness_seconds",  fresh)
        err   = overrides.get("error_rate",         err)
        vol   = overrides.get("volume_rph",         vol)
        slo   = overrides.get("slo_compliance",     slo)

    # Clamp
    avail = max(0.0, min(1.0, avail))
    lat   = max(0.1, lat)
    fresh = max(0.0, fresh)
    err   = max(0.0, min(1.0, err))
    vol   = max(0.0, vol)
    slo   = max(0.0, min(1.0, slo))

    # Add organic noise
    avail = _jitter(avail, 0.001)
    lat   = _jitter(lat,   0.04)
    fresh = _jitter(fresh, 0.02)
    err   = _jitter(err,   0.05)
    vol   = _jitter(vol,   0.03)

    # Re-clamp after noise
    avail = max(0.0, min(1.0, avail))
    err   = max(0.0, min(1.0, err))

    # Derive health score
    hs = health_score(avail, lat, fresh, err, prod)

    labels = {
        "portfolio":   prod.portfolio,
        "domain":      prod.domain,
        "product":     prod.product,
        "owner":       prod.owner,
        "environment": env,
    }

    _set("data_product_availability",           labels, avail)
    _set("data_product_latency_p95_ms",         labels, lat)
    _set("data_product_freshness_seconds",      labels, fresh)
    _set("data_product_error_rate",             labels, err)
    _set("data_product_volume_records_per_hour",labels, vol)
    _set("data_product_slo_compliance_ratio",   labels, slo)
    _set("data_product_health_score",           labels, hs)

    # Increment counters on violations
    if avail < prod.sla_availability or lat > prod.sla_latency_ms or \
       fresh > prod.sla_freshness_s  or err  > prod.sla_error_rate:
        _inc("data_product_sla_breach_total", labels)

    if err > prod.sla_error_rate * 1.5:
        _inc("data_product_dq_violation_total", labels)


def _update_loop() -> None:
    log.info("Metric update loop started (interval=%ds)", UPDATE_INTERVAL)
    while True:
        now_london = datetime.now(LONDON)
        for prod in PRODUCTS:
            try:
                _compute_and_publish(prod, now_london)
            except Exception as exc:
                log.error("Error computing metrics for %s: %s", prod.product, exc)
        time.sleep(UPDATE_INTERVAL)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    log.info("Banking Marketplace Simulator starting")
    log.info("  Products    : %d", len(PRODUCTS))
    log.info("  Portfolios  : %s", sorted({p.portfolio for p in PRODUCTS}))
    log.info("  Environment : %s", ENVIRONMENT)
    log.info("  Metrics     : http://0.0.0.0:%d/metrics", METRICS_PORT)

    # Warm up metrics before HTTP server starts
    now = datetime.now(LONDON)
    for prod in PRODUCTS:
        _compute_and_publish(prod, now)

    # Start update thread
    t = threading.Thread(target=_update_loop, daemon=True, name="metric-updater")
    t.start()

    # Start HTTP server (blocks main thread)
    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    log.info("HTTP server ready on :%d", METRICS_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Simulator stopped.")


if __name__ == "__main__":
    main()
