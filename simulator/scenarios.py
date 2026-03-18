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
    # Source / target metadata — emitted as span attributes
    source_system: str = "unknown"
    source_table: str = ""
    source_format: str = ""          # parquet, avro, json, jdbc, kafka, s3 …
    source_filter: str = ""          # e.g. "incremental: updated_at > watermark"
    target_system: str = "unknown"
    target_table: str = ""
    target_partition: str = ""       # partition column / key
    target_write_mode: str = ""      # upsert, append, overwrite, merge
    transform_operations: list[str] = field(default_factory=list)  # logical ops list


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
        run_interval_seconds=120,
        sla_seconds=7200,
        description="Happy path: reliable pipeline, always succeeds.",
        phases=[
            PhaseConfig(
                "extract", min_seconds=8, max_seconds=15,
                source_system="oracle", source_table="crm.customers,crm.accounts",
                source_format="jdbc", source_filter="updated_at > :watermark",
            ),
            PhaseConfig(
                "transform", min_seconds=20, max_seconds=40,
                transform_operations=[
                    "join(customers, accounts, on=customer_id)",
                    "enrich(risk_tier from risk_scoring)",
                    "deduplicate(key=customer_id, keep=latest)",
                    "compute(lifetime_value, churn_score)",
                ],
            ),
            PhaseConfig(
                "load", min_seconds=5, max_seconds=12,
                target_system="snowflake", target_table="customer_360.enriched",
                target_partition="ingestion_date", target_write_mode="upsert",
            ),
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
            PhaseConfig(
                "extract", min_seconds=5, max_seconds=10,
                source_system="kafka", source_table="transaction-events",
                source_format="avro", source_filter="consumer_group=cg-risk-scoring",
            ),
            PhaseConfig(
                "transform", min_seconds=15, max_seconds=35,
                fail_probability=0.25,
                transform_operations=[
                    "parse_avro_schema(v2.3)",
                    "feature_engineering(txn_velocity, merchant_category, geo_risk)",
                    "ml_score(model=fraud_xgb_v4, threshold=0.85)",
                    "flag_high_risk(score > 0.85)",
                ],
            ),
            PhaseConfig(
                "load", min_seconds=3, max_seconds=8,
                target_system="redis", target_table="risk_score:{customer_id}",
                target_write_mode="overwrite",
            ),
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
        sla_seconds=60,
        description="SLA breacher: legacy full-load runs take longer than declared SLA.",
        phases=[
            PhaseConfig(
                "extract", min_seconds=25, max_seconds=50,
                source_system="mssql", source_table="legacy.dbo.transactions",
                source_format="jdbc", source_filter="full_load=true",
            ),
            PhaseConfig(
                "transform", min_seconds=30, max_seconds=50,
                transform_operations=[
                    "cast_legacy_types(decimal→float, nvarchar→utf8)",
                    "normalize_dates(format=ISO8601)",
                    "currency_conversion(to=USD, fx_source=ecb_daily)",
                    "apply_business_rules(v12)",
                ],
            ),
            PhaseConfig(
                "load", min_seconds=10, max_seconds=20,
                target_system="redshift", target_table="analytics.transactions",
                target_partition="transaction_date", target_write_mode="append",
            ),
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
            PhaseConfig(
                "extract", min_seconds=3, max_seconds=8,
                rows_min=500_000, rows_max=5_000_000,
                source_system="s3", source_table="s3://data-lake/raw/events/",
                source_format="parquet", source_filter="event_date=:partition_date",
            ),
            PhaseConfig(
                "transform", min_seconds=10, max_seconds=25,
                transform_operations=[
                    "schema_validation(schema=events_v3)",
                    "null_handling(strategy=drop_row, columns=[event_id])",
                    "type_coercion(event_ts→timestamp, amount→decimal(18,4))",
                    "partition_by(event_date, event_type)",
                ],
            ),
            PhaseConfig(
                "load", min_seconds=4, max_seconds=10,
                target_system="hive", target_table="raw.events",
                target_partition="event_date", target_write_mode="overwrite",
            ),
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
            PhaseConfig(
                "extract", min_seconds=4, max_seconds=8,
                source_system="snowflake", source_table="customer_360.enriched",
                source_format="jdbc", source_filter="ingestion_date = :run_date",
            ),
            PhaseConfig(
                "transform", min_seconds=10, max_seconds=20,
                fail_probability=0.10,
                transform_operations=[
                    "aggregate(group_by=segment, metrics=[avg_ltv, churn_rate])",
                    "compute_recommendations(model=collab_filter_v2)",
                    "rank(top_n=10, per=customer_id)",
                ],
            ),
            PhaseConfig(
                "load", min_seconds=3, max_seconds=7,
                target_system="elasticsearch",
                target_table="customer_recommendations_v3",
                target_write_mode="upsert",
            ),
        ],
    ),
]
