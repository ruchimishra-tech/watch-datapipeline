"""
watch_obs — Data Pipeline Observability SDK

Usage:
    from sdk import Pipeline

    with Pipeline(name="customer-360", pipeline_id="customer-360") as run:
        with run.phase("extract", source_system="oracle") as phase:
            phase.record_metric("rows_read", 52000)
        with run.phase("transform") as phase:
            phase.set_attribute("transform.engine", "spark")
        with run.phase("load", target_system="snowflake") as phase:
            phase.record_metric("rows_written", 51800)
"""
from sdk.pipeline import Pipeline
from sdk.phase import Phase

__all__ = ["Pipeline", "Phase"]
