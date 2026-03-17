"""
Simulator scenario definitions.

Each scenario describes a pipeline's behaviour: phase durations,
failure probability, DQ checks, and run schedule.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PhaseConfig:
    name: str                        # extract | transform | load
    min_seconds: float               # min realistic duration
    max_seconds: float               # max realistic duration
    fail_probability: float = 0.0    # 0.0 = never fails
    rows_min: int = 10_000
    rows_max: int = 1_000_000


@dataclass
class DQConfig:
    column: str
    null_rate_normal: float = 0.001  # normal null rate
    null_rate_spike: float = 0.08    # spike null rate (triggers alert)
    spike_probability: float = 0.0   # chance of a spike per run


@dataclass
class ScenarioConfig:
    pipeline_id: str
    pipeline_name: str
    domain: str
    owner: str
    run_interval_seconds: int        # how often to run
    phases: list[PhaseConfig]
    dq_checks: list[DQConfig] = field(default_factory=list)
    sla_seconds: Optional[int] = None  # None = no SLA
    enabled: bool = True
    description: str = ""


# ── Scenario registry ─────────────────────────────────────────────────────────

SCENARIOS: list[ScenarioConfig] = [

    ScenarioConfig(
        pipeline_id="customer-360-enrichment",
        pipeline_name="Customer 360 Enrichment",
        domain="customer-data",
        owner="team-customer-data",
        run_interval_seconds=120,   # every 2 min in sim (represents hourly in prod)
        sla_seconds=7200,
        description="Happy path: reliable pipeline, always succeeds.",
        phases=[
            PhaseConfig("extract",   min_seconds=8,  max_seconds=15),
            PhaseConfig("transform", min_seconds=20, max_seconds=40),
            PhaseConfig("load",      min_seconds=5,  max_seconds=12),
        ],
        dq_checks=[
            DQConfig("customer_id",  null_rate_normal=0.0,   spike_probability=0.0),
            DQConfig("risk_tier",    null_rate_normal=0.002, spike_probability=0.05,
                     null_rate_spike=0.06),
        ],
    ),

    ScenarioConfig(
        pipeline_id="risk-scoring",
        pipeline_name="Risk Scoring Pipeline",
        domain="risk",
        owner="team-risk-analytics",
        run_interval_seconds=90,
        sla_seconds=3600,
        description="Intermittent failures: fails ~25% of runs in transform.",
        phases=[
            PhaseConfig("extract",   min_seconds=5,  max_seconds=10),
            PhaseConfig("transform", min_seconds=15, max_seconds=35,
                        fail_probability=0.25),
            PhaseConfig("load",      min_seconds=3,  max_seconds=8),
        ],
        dq_checks=[
            DQConfig("score", null_rate_normal=0.001, spike_probability=0.0),
        ],
    ),

    ScenarioConfig(
        pipeline_id="slow-etl",
        pipeline_name="Slow ETL Pipeline",
        domain="data-platform",
        owner="team-platform",
        run_interval_seconds=180,
        sla_seconds=60,             # tight SLA — will regularly breach (60s vs 70-120s)
        description="SLA breacher: runs take longer than declared SLA.",
        phases=[
            PhaseConfig("extract",   min_seconds=25, max_seconds=50),
            PhaseConfig("transform", min_seconds=30, max_seconds=50),
            PhaseConfig("load",      min_seconds=10, max_seconds=20),
        ],
    ),

    ScenarioConfig(
        pipeline_id="raw-ingestion",
        pipeline_name="Raw Data Ingestion",
        domain="data-platform",
        owner="team-platform",
        run_interval_seconds=60,
        description="DQ violations: null spikes trigger DQ critical alert.",
        phases=[
            PhaseConfig("extract",   min_seconds=3,  max_seconds=8,
                        rows_min=500_000, rows_max=5_000_000),
            PhaseConfig("transform", min_seconds=10, max_seconds=25),
            PhaseConfig("load",      min_seconds=4,  max_seconds=10),
        ],
        dq_checks=[
            DQConfig("event_id",    null_rate_normal=0.0,   spike_probability=0.0),
            DQConfig("user_id",     null_rate_normal=0.003, spike_probability=0.15,
                     null_rate_spike=0.12),
            DQConfig("event_type",  null_rate_normal=0.001, spike_probability=0.08,
                     null_rate_spike=0.07),
        ],
    ),

    ScenarioConfig(
        pipeline_id="downstream-enrichment",
        pipeline_name="Downstream Enrichment",
        domain="customer-data",
        owner="team-customer-data",
        run_interval_seconds=150,
        description="Cascade: fails when risk-scoring upstream fails.",
        phases=[
            PhaseConfig("extract",   min_seconds=4,  max_seconds=8),
            PhaseConfig("transform", min_seconds=10, max_seconds=20,
                        fail_probability=0.10),   # some independent failures too
            PhaseConfig("load",      min_seconds=3,  max_seconds=7),
        ],
    ),
]
