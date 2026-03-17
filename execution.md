# Data Pipeline Observability Platform — Execution Plan

Version: 0.1
Author: Nishant Verma & Ruchi
Purpose: Incremental build plan with testable milestones, derived from Context.md requirements.

---

## How to Read This Document

- Phases are ordered by dependency — each phase builds on the previous.
- Each phase contains **Features** (what to build) and **Tests** (how to know it works before moving on).
- No phase should begin until all tests in the previous phase pass.
- The plan is intentionally granular at Phase 0–3 (foundation matters most) and higher-level from Phase 7 onwards.

---

## Testing Principles

Every feature built in this project must have at least one corresponding automated test. Tests are not optional or deferred — they are written alongside the feature and committed in the same pull request. This test suite is the foundation of the regression pack.

### Automated Test Types

Each test in this document is classified by type:

| Type | Description | Tooling |
|------|-------------|---------|
| **Unit** | Tests a single function or class in isolation. No external services. Fast — runs in milliseconds. | `pytest` |
| **Contract** | Tests that a data structure (span, YAML, JSON) conforms to its declared schema. No runtime services needed. | `jsonschema`, `pydantic` |
| **Integration** | Tests that two or more components work together correctly. Requires the local docker-compose stack to be running. | `pytest` + docker-compose |
| **E2E** | Tests the full pipeline: signal emitted by a real (canary) pipeline flows all the way through to Grafana/Tempo. | `pytest` + docker-compose + Tempo API |

### Regression Pack Structure

All tests live under `tests/` and are tagged so the regression pack can be run selectively:

```
tests/
  unit/                   ← Unit tests (no services needed)
    test_sdk_span.py
    test_schema_validation.py
    test_pii_redaction.py
    test_run_id_generation.py
    ...
  contract/               ← Schema contract tests (no services needed)
    test_span_schema.py
    test_slo_schema.py
    test_contract_schema.py
    ...
  integration/            ← Require docker-compose stack
    test_sdk_to_tempo.py
    test_alerting.py
    test_dq_rules.py
    ...
  e2e/                    ← Full canary pipeline runs
    test_canary_batch.py
    test_canary_legacy.py
    test_canary_streaming.py
    ...
```

Run the full regression pack: `pytest tests/ -v`
Run only unit + contract (no services): `pytest tests/unit tests/contract -v`
Run by phase tag: `pytest tests/ -m "phase0" -v`

### Rule: Every Feature Gets a Test

When adding a feature:
1. Write the test first (or alongside the code) — never after
2. The test must be automated — no manual verification steps
3. Tag the test with its phase: `@pytest.mark.phase0`
4. The test must pass in CI before the PR can merge
5. Once written, the test is never deleted — it becomes a permanent part of the regression pack

---

## Phase 0 — Foundation: Local Dev Stack & Schema Definition

**Goal:** Every engineer can run the full observability stack locally. The canonical span schema exists and is machine-validated.

### Features

#### 0.1 Repository Structure
Set up the monorepo layout:
```
watch-datapipeline/
  schema/                    ← canonical span attribute schema (JSON Schema)
  collector/                 ← ADOT collector config
  sdk/                       ← Python instrumentation SDK
  synthesizer/               ← legacy log → span converter
  infra/                     ← Terraform / docker-compose
  dashboards/                ← Grafana dashboard JSON
  alerts/                    ← Prometheus alert rules
  contracts/                 ← example data contract YAMLs
  slos/                      ← example SLO definition YAMLs
  cli/                       ← obs-cli onboarding tool
  tests/                     ← integration and e2e tests
  .github/workflows/         ← CI pipelines
```

#### 0.2 Local Docker-Compose Stack
Stand up the following services locally via docker-compose:
- **ADOT Collector** — receives spans from SDK, applies PII redaction, routes to Tempo
- **Grafana Tempo** — trace storage backend
- **Prometheus** — metrics storage
- **Grafana** — visualisation (pre-configured with Tempo and Prometheus datasources)
- **Loki** — log aggregation (for later phases)

All services must start with a single `docker-compose up`.

#### 0.3 Canonical Span Schema (JSON Schema)
Define the required attributes for all spans in `schema/span-schema.json`:

Root span (`pipeline.run`) required fields:
- `trace_id`, `span_id`
- `pipeline.run_id` — unique identifier for this execution
- `pipeline.name`, `pipeline.id`
- `pipeline.type` — enum: `batch` | `streaming`
- `deployment.environment` — enum: `dev` | `staging` | `production`
- `pipeline.status` — enum: `running` | `success` | `failed` | `cancelled`

Phase spans (`etl.extract`, `etl.transform`, `etl.load`) required fields:
- All root fields plus: `etl.phase`, `source.type`, `source.system`

---

**Extract phase span — detailed attributes**

What data was read:
- `extract.source.system` — e.g. `oracle`, `s3`, `kafka`, `snowflake`, `postgres`
- `extract.source.database` — database or bucket name
- `extract.source.schema` — schema name (if applicable)
- `extract.source.table` — table or object name being read
- `extract.source.partition` — partition key and value if a partitioned read (e.g. `date=2026-03-16`)
- `extract.source.format` — file format if reading from storage: `parquet`, `csv`, `json`, `orc`, `avro`
- `extract.filter.predicate` — the WHERE clause or filter condition applied (**PII-redacted** if filtering on customer identifiers; stored as `[REDACTED]` if field is PII-classified)
- `extract.filter.date_column` — the column used for date-range filtering
- `extract.filter.date_from` / `extract.filter.date_to` — the date range extracted

What SQL or query was run:
- `extract.query.type` — enum: `full_table` | `incremental` | `snapshot` | `cdc` | `api_call` | `file_glob`
- `extract.query.sql` — the actual SQL statement executed, with literal PII values replaced by bind variable markers (e.g. `WHERE customer_id = ?` not `WHERE customer_id = 12345`). Omitted entirely if the source is a file or API.
- `extract.query.sql_hash` — SHA-256 hash of the normalised SQL (useful for grouping runs using the same query pattern without storing the SQL text)
- `extract.query.incremental_key` — the column used to identify new/changed records (e.g. `updated_at`, `transaction_date`)
- `extract.query.incremental_from` — the watermark value the incremental extract started from
- `extract.query.incremental_to` — the watermark value the incremental extract ended at

Volume and timing:
- `extract.rows_read` — total rows read from source
- `extract.bytes_read` — total bytes read from source
- `extract.files_read` — number of files read (for file-based sources)
- `extract.duration_ms` — time taken for the extract phase in milliseconds
- `extract.source_row_count_at_query_time` — total rows in the source table at the moment of query (enables completeness checks)

Connection and infrastructure:
- `extract.connection.host` — hostname of the source system (no port, no credentials)
- `extract.connection.jdbc_driver` — JDBC driver class name (for JDBC sources)
- `extract.staging.path` — S3/HDFS path where raw extracted data was staged before transformation

---

**Transform phase span — detailed attributes**

What logic was applied:
- `transform.operations[]` — ordered list of named transformation steps applied, e.g.:
  - `filter_invalid_records`
  - `deduplicate_on_account_id`
  - `aggregate_by_account`
  - `join_customer_master`
  - `derive_risk_score`
  - `mask_account_number`
- `transform.logic.description` — free-text human-readable description of what this transform does (populated from code annotation or registration)
- `transform.logic.version` — version identifier of the transformation logic (e.g. git commit SHA of the transform script, or a semantic version if the logic is packaged)

Join and lookup details (one entry per join):
- `transform.joins[].type` — enum: `inner` | `left` | `right` | `cross` | `broadcast`
- `transform.joins[].left_dataset` — name of the left dataset or table
- `transform.joins[].right_dataset` — name of the right dataset or table (the lookup)
- `transform.joins[].join_keys[]` — column names used as join keys
- `transform.joins[].match_rate_pct` — percentage of left-side rows that found a match on the right side
- `transform.joins[].unmatched_rows` — count of rows that did not find a match (useful for detecting reference data gaps)

Aggregation details:
- `transform.aggregations[].function` — enum: `sum` | `count` | `avg` | `min` | `max` | `count_distinct`
- `transform.aggregations[].column` — column being aggregated
- `transform.aggregations[].group_by[]` — columns used in the GROUP BY

Row-level accounting:
- `transform.rows_in` — rows entering the transform phase (should equal `extract.rows_read` for the first transform step)
- `transform.rows_out` — rows exiting the transform phase (feeds into `load.rows_written`)
- `transform.rows_dropped` — rows removed by filter or validation rules
- `transform.rows_dropped_reason` — reason code for the largest drop category (e.g. `null_account_id`, `duplicate_key`, `failed_dq_rule`)
- `transform.rows_quarantined` — rows routed to quarantine rather than dropped
- `transform.rows_derived` — rows created during the transform that did not exist in the source (e.g. from aggregation or explode operations)

Schema:
- `transform.schema_drift_detected` — boolean; true if the input schema differed from the expected schema
- `transform.schema_drift_detail` — description of what changed (e.g. `column 'risk_tier' added`, `column 'account_type' type changed from STRING to INT`)
- `transform.output_schema.columns[]` — list of output column names (not values)
- `transform.output_schema.column_count` — number of columns in the output dataset

Engine and resource:
- `transform.engine` — enum: `spark` | `flink` | `pandas` | `dbt` | `sql` | `python`
- `transform.engine.version` — version of the engine used
- `transform.spark.job_id` — Spark application ID (for cross-referencing Spark history server)
- `transform.spark.stages_total` — number of Spark stages
- `transform.spark.stages_failed` — number of Spark stages that failed (even if retried successfully)
- `transform.spark.shuffle_bytes` — total shuffle data written (indicator of join/aggregation cost)
- `transform.duration_ms` — time taken for the transform phase in milliseconds

---

**Load phase span — detailed attributes**

Where data was written:
- `load.target.system` — e.g. `snowflake`, `bigquery`, `s3`, `redshift`, `delta`, `iceberg`
- `load.target.database` — target database
- `load.target.schema` — target schema
- `load.target.table` — target table or object name
- `load.target.format` — file format if writing to storage: `parquet`, `delta`, `iceberg`, `csv`
- `load.target.path` — S3/HDFS path written to (for file-based targets)
- `load.target.partition_cols[]` — partition columns used when writing (e.g. `[date, region]`)
- `load.target.partition_value` — the specific partition value written in this run (e.g. `2026-03-16`)

How data was written:
- `load.write_mode` — enum: `overwrite` | `append` | `merge` | `upsert` | `insert_overwrite_partition`
- `load.merge_keys[]` — columns used as merge/upsert keys (for `merge` and `upsert` modes)
- `load.snapshot_id` — Iceberg or Delta snapshot ID created by this write (enables artifact-identity stitching for downstream consumers)
- `load.transaction_id` — database transaction ID if the load was transactional

Volume and outcome:
- `load.rows_written` — rows successfully written to target
- `load.rows_rejected` — rows rejected by the target system (e.g. constraint violations)
- `load.bytes_written` — bytes written to target
- `load.files_written` — number of files written (for file-based targets)
- `load.duration_ms` — time taken for the load phase in milliseconds

Pre- and post-load checks:
- `load.pre_check.target_row_count_before` — row count in target table before the load (for change validation)
- `load.post_check.target_row_count_after` — row count in target table after the load
- `load.post_check.row_count_delta` — computed difference (`after - before`); should equal `rows_written` for append mode
- `load.post_check.passed` — boolean; whether post-load row count is within expected range

---

**Step-level spans (within a phase)**

For pipelines that emit at step granularity (native instrumentation, Tier 1), each named operation within a phase emits its own child span:
- `step.name` — human-readable name (e.g. `filter_nulls`, `join_customer_master`, `write_risk_scores`)
- `step.sequence` — integer position within the phase (1-indexed)
- `step.rows_in` / `step.rows_out` — row counts at step boundary
- `step.duration_ms` — time for this step only
- `step.sql` — SQL executed at this step (if applicable; PII bind variables only)
- `step.description` — free-text description from code annotation

---

PII-classified fields (must be redacted at the ADOT collector before storage — never written to Tempo):
- `extract.filter.predicate` — if the source table is PII-classified
- `extract.query.sql` — replaced with bind variable markers before storage
- `sample_bad_values` — from DQ checks; any field tagged `pii: true` in the data contract

#### 0.4 CI Pipeline — Schema Linting
GitHub Actions workflow that runs on every PR:
- Validates any changed YAML/JSON in `schema/` against meta-schema
- Runs span schema against a set of fixture files (valid and invalid samples)
- Fails if a known-valid fixture is rejected or a known-invalid fixture passes

#### 0.5 Synthetic Canary Pipeline
A simple Python script (`tests/canary_pipeline.py`) that:
- Simulates a 3-step batch pipeline (extract → transform → load)
- Emits spans using raw OTLP (before SDK exists)
- Runs against the local docker-compose stack

### Tests for Phase 0

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T0.1 | Integration | `docker-compose up` | All 5 services healthy within 60 seconds |
| T0.2 | Contract | Schema valid fixture | `schema/fixtures/valid_root_span.json` passes JSON Schema validation |
| T0.3 | Contract | Schema invalid fixture — missing run_id | `schema/fixtures/missing_run_id.json` fails validation with error citing `pipeline.run_id` |
| T0.4 | Contract | Schema invalid fixture — bad enum | Span with `pipeline.type: "continuous"` (not in enum) fails validation |
| T0.5 | Contract | CI schema lint | PR removing a required field from `span-schema.json` is blocked by CI |
| T0.6 | E2E | Canary pipeline | Script runs; root span and 3 phase spans appear in Tempo within 30 seconds |
| **Extract span attribute tests** | | | |
| T0.7 | Contract | Extract — full attribute set | Fixture with all extract attributes passes schema validation |
| T0.8 | Contract | Extract — SQL stored safely | Fixture with `extract.query.sql` containing a literal value (e.g. `WHERE id = 123`) fails schema validation — only bind variable form (`WHERE id = ?`) is permitted |
| T0.9 | Contract | Extract — incremental fields | Fixture with `query.type: incremental` but missing `incremental_key` fails validation |
| T0.10 | Contract | Extract — PII field declared | Fixture with `extract.filter.predicate` on a PII-classified source must carry `pii: true` tag; fixture without it fails validation |
| T0.11 | Contract | Extract — partition value | Fixture with `extract.source.partition` not matching `key=value` format fails validation |
| **Transform span attribute tests** | | | |
| T0.12 | Contract | Transform — operations list | Fixture with `transform.operations` as a non-array fails validation |
| T0.13 | Contract | Transform — join type enum | Fixture with `transform.joins[0].type: "anti"` (not in enum) fails validation |
| T0.14 | Contract | Transform — row accounting | Fixture where `rows_in - rows_dropped - rows_quarantined > rows_out` by more than `rows_derived` fails validation (accounting must balance) |
| T0.15 | Contract | Transform — schema drift detail required | Fixture with `schema_drift_detected: true` but no `schema_drift_detail` fails validation |
| T0.16 | Contract | Transform — engine enum | Fixture with `transform.engine: "numpy"` (not in enum) fails validation |
| T0.17 | Contract | Transform — step sequence | Fixture with two step spans sharing the same `step.sequence` integer under the same phase fails validation |
| **Load span attribute tests** | | | |
| T0.18 | Contract | Load — write mode enum | Fixture with `load.write_mode: "truncate_insert"` (not in enum) fails validation |
| T0.19 | Contract | Load — merge keys required for upsert | Fixture with `load.write_mode: merge` but empty `load.merge_keys[]` fails validation |
| T0.20 | Contract | Load — row count reconciliation | Fixture where `post_check.row_count_delta` does not equal `rows_written` for `append` write mode fails validation |
| T0.21 | Contract | Load — snapshot_id present for Iceberg | Fixture with `load.target.format: iceberg` but no `load.snapshot_id` fails validation |
| T0.22 | Contract | Load — post check passed required | Fixture with `load.post_check.passed: false` must carry a `load.post_check.failure_reason`; fixture without it fails validation |

---

## Phase 1 — Python Instrumentation SDK

**Goal:** Pipeline developers can instrument a new pipeline with 5 lines of code. The SDK is non-blocking, schema-compliant, and handles `pipeline_run_id` generation and propagation automatically.

### Features

#### 1.1 SDK Core
Python package `watch_obs` with the following interface:

```python
from watch_obs import Pipeline

with Pipeline(name="customer-360", pipeline_id="cust-360") as run:
    with run.phase("extract", source_system="oracle") as phase:
        phase.record_metric("rows_read", 50000)
    with run.phase("transform") as phase:
        phase.emit_event("schema_drift_detected", severity="warning")
    with run.phase("load", target_system="snowflake") as phase:
        phase.record_metric("rows_written", 49800)
```

Internally the SDK must:
- Generate a `pipeline_run_id` (UUID4) at pipeline start if not provided
- Accept an externally provided `pipeline_run_id` for cross-system propagation
- Emit spans via OTLP to the ADOT collector endpoint (configurable via env var)
- Set all required schema attributes automatically
- Never raise exceptions that propagate to the calling pipeline — all errors are logged internally

#### 1.2 Non-Blocking Emit
Span emission must be asynchronous (background thread or asyncio task). The pipeline must continue executing if the collector is unreachable. If the collector is down:
- Spans are dropped (not buffered indefinitely)
- A warning is logged to stderr
- The pipeline continues normally

#### 1.3 PII Redaction at Collector
ADOT collector processor config that:
- Strips any span attribute listed in the PII field list before forwarding to Tempo
- Replaces stripped values with the string `[REDACTED]`
- Logs a redaction event (without the original value)

#### 1.4 `pipeline_run_id` Propagation Helpers
SDK utility functions to propagate `pipeline_run_id` across system boundaries:
- `run.to_env_vars()` — returns dict suitable for subprocess env injection
- `run.to_http_headers()` — returns dict for HTTP header injection
- `run.to_kafka_headers()` — returns list of Kafka header tuples
- `Pipeline.from_env()` — reconstructs run context from env vars (for child processes)

### Tests for Phase 1

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T1.1 | Integration | SDK happy path | 3-phase pipeline emits 4 spans (1 root + 3 phases), all appear in Tempo with correct attributes |
| T1.2 | Contract | Schema compliance | All emitted spans pass `schema/span-schema.json` validation |
| T1.3 | Unit | Non-blocking — collector down | Collector stopped; pipeline runs to completion; no exception raised; warning logged to stderr |
| T1.4 | Unit | Non-blocking — no buffer growth | Collector down for 60 seconds; SDK memory does not grow (spans are dropped, not queued) |
| T1.5 | Integration | PII redaction | Span emitted with `sample_bad_values="SSN:123-45-6789"`; Tempo stores `[REDACTED]`; original value absent from Tempo API response |
| T1.6 | Integration | run_id propagation via env | Parent pipeline → child subprocess via env vars; child spans carry same `pipeline_run_id` as parent root span |
| T1.7 | Unit | run_id propagation via HTTP headers | `run.to_http_headers()` returns dict with `pipeline_run_id` key; `Pipeline.from_env()` reconstructs correct run context |
| T1.8 | Unit | run_id propagation via Kafka headers | `run.to_kafka_headers()` returns list of tuples; first tuple key is `pipeline_run_id` |
| T1.9 | Integration | External run_id | SDK initialised with externally supplied `pipeline_run_id`; all 4 emitted spans carry that exact ID |
| T1.10 | Unit | Missing collector URL env | Collector URL env var not set; SDK initialises with no-op emitter; no exception raised; pipeline runs without error |
| T1.11 | Unit | run_id uniqueness | Two `Pipeline()` instances created without external run_id; both `run_id` values are different UUID4s |

---

## Phase 2 — Pipeline Execution Observability (Batch)

**Goal:** Every batch pipeline run — whether it succeeds, fails, or retries — produces a complete, queryable trace in Tempo. Support staff can find any run by `pipeline_run_id` or by pipeline name + time range.

### Features

#### 2.1 Full Run Lifecycle Spans
Extend the SDK to emit:
- **Run start event** — emitted immediately when `Pipeline.__enter__` is called
- **Phase completion events** — emitted on phase exit with duration, status, and row counts
- **Run completion event** — emitted on `Pipeline.__exit__` with final status and total duration
- **Failure event** — emitted if an exception propagates through the context manager; includes exception type and message (not stack trace, which may contain PII)

#### 2.2 Retry Tracking
SDK must track and surface retry attempts:
- `run.retry_attempt` attribute (integer, 0-indexed)
- All spans for a retry carry the same `pipeline_run_id` with `retry_attempt` incremented
- First attempt and retry spans are linked in the same trace

#### 2.3 Run State Store (Local)
A simple key-value store (SQLite for local dev, DynamoDB for production) that:
- Records pipeline run state: `run_id → {status, start_time, last_updated, retry_count}`
- Allows the synthesizer (Phase 5) to look up in-progress runs
- TTL: `max_job_duration + 6 hours`; expired entries cleaned up automatically

#### 2.4 Incremental Span Emission for Long-Running Jobs
For jobs running longer than 60 seconds, the SDK must emit intermediate "heartbeat" span updates every 60 seconds so that in-progress runs are visible in the dashboard without waiting for completion.

### Tests for Phase 2

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T2.1 | E2E | Success trace | Run completes; Tempo contains root span + 3 phase spans; root `pipeline.status = success`; all phase durations > 0 |
| T2.2 | E2E | Failure trace | Exception in transform phase; transform span carries `pipeline.status = failed` and `pipeline.failure_reason`; root span status = `failed` |
| T2.3 | Unit | Failure — no stack trace | Exception message stored in span; full Python stack trace string is not present in any span attribute |
| T2.4 | E2E | Retry trace | Pipeline fails then retries; Tempo trace contains spans from both attempts; `retry_attempt = 0` on first, `retry_attempt = 1` on second |
| T2.5 | Unit | Run state store — write | After run completes, SQLite record for `run_id` contains correct `status`, `start_time`, and `duration_ms` |
| T2.6 | Unit | Run state store — TTL | Record inserted with `max_job_duration = 1s`; after `7s` the record is absent (expired) |
| T2.7 | Integration | Long-running heartbeat | Job sleeps 3 minutes; heartbeat spans appear in Tempo at 60s, 120s, and 180s before completion span |
| T2.8 | Integration | Perf — ingestion latency | SDK emits span; Tempo API returns that span within 1 second of emission (timing measured in test) |

---

## Phase 3 — Basic Dashboard (L1 + L2)

**Goal:** Support staff can open a Grafana dashboard, see all pipeline runs for the last 24 hours, filter by status, and click into a failed run to see which step failed and why.

### Features

#### 3.1 L1 — Estate Health Dashboard
Panels (deployed as code in `dashboards/l1-estate.json`):
- Pipeline success rate (last 24h) — single stat
- Total pipeline runs today — single stat
- Failed runs in last 1h — single stat with alert colour threshold
- Pipeline run table: name, last run time, status, duration, environment

#### 3.2 L2 — Pipeline Run Detail Dashboard
Panels (deployed as code in `dashboards/l2-pipeline.json`):
- Run timeline: Gantt-style view of phases (extract → transform → load) with duration
- Phase status table: phase name, start, end, duration, row counts, status
- Failure details panel: visible only when run status = `failed`; shows failure phase, reason, and link to logs

#### 3.3 Environment Isolation
- Every dashboard includes an `$environment` template variable (dropdown: dev / staging / production)
- All Tempo queries are scoped to the selected environment namespace
- Dashboard panels show a visible banner when `production` is selected
- No cross-environment data is retrievable from a single dashboard view

#### 3.4 Dashboard CI Validation
GitHub Actions check that runs on any PR touching `dashboards/`:
- Parses dashboard JSON and asserts mandatory panels are present
- Validates datasource references match deployed datasource names
- Validates `$environment` template variable is declared

### Tests for Phase 3

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T3.1 | E2E | L1 renders after canary run | Canary pipeline completes; Grafana API query for L1 panels returns data with correct pipeline name, status, and duration |
| T3.2 | E2E | L2 Gantt — phase durations | Grafana Tempo query for L2 run detail returns 3 phase spans; each has non-zero duration; spans are in extract → transform → load order |
| T3.3 | E2E | L2 failure panel populated | Canary pipeline fails intentionally; L2 Grafana query returns failure phase name and reason matching the injected exception |
| T3.4 | Integration | Env isolation — staging not in production | Staging canary run stored in Tempo staging namespace; Grafana production datasource query returns 0 spans for that `pipeline_run_id` |
| T3.5 | Contract | Dashboard CI — mandatory panel check | PR removing a mandatory panel from `l1-estate.json`; CI script exits non-zero with message naming the missing panel |
| T3.6 | Contract | Dashboard CI — datasource reference | Dashboard JSON referencing a non-existent datasource name; CI script exits non-zero |
| T3.7 | Contract | Dashboard CI — env variable declared | Dashboard JSON without `$environment` template variable; CI script exits non-zero |
| T3.8 | Integration | Dashboard refresh latency | Grafana API `/api/datasources/proxy` call returns L1 data within 3 seconds (measured in integration test) |

---

## Phase 4 — Alerting

**Goal:** When a pipeline fails or is delayed, the right team is notified within 2 minutes via Slack. Alerts are actionable — they contain enough context to begin investigation without opening a dashboard.

### Features

#### 4.1 Prometheus Alert Rules (`alerts/pipeline-alerts.yaml`)
Rules to define:
- `PipelineRunFailed` — fires when a pipeline run ends with status `failed`
- `PipelineRunDelayed` — fires when a batch pipeline has not started within `expected_start + threshold` window
- `PipelineSLABreached` — fires when pipeline duration exceeds declared SLA
- All rules must carry labels: `severity`, `domain`, `pipeline_id`, `runbook_url`

#### 4.2 Alert Routing
- Slack: all `severity: warning` and `severity: critical` alerts route to a configurable Slack channel
- Email: `severity: critical` alerts additionally send email to pipeline owner
- Alert message template must include: pipeline name, run ID, failure reason, link to L2 dashboard for that run

#### 4.3 Alert Deployment as Code
- Alert rules live in `alerts/` and are applied via Terraform or Prometheus Operator on merge
- CI validates: PromQL syntax, required labels present, `runbook_url` resolves (HTTP 200)
- No manual edits to Prometheus rules via UI

### Tests for Phase 4

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T4.1 | Unit | PromQL syntax — failure rule | `PipelineRunFailed` rule evaluated against mock Prometheus data; fires when `pipeline_status = failed` metric present |
| T4.2 | Unit | PromQL syntax — delay rule | `PipelineRunDelayed` rule fires when no `pipeline_run_started` metric within threshold window |
| T4.3 | Contract | Alert rule CI — invalid PromQL | PR with syntactically invalid PromQL in alert rule; CI promtool check exits non-zero |
| T4.4 | Contract | Alert rule CI — missing required labels | PR with alert rule missing `runbook_url` label; CI exits non-zero naming the missing label |
| T4.5 | Integration | Failure alert fires | Canary pipeline emits `pipeline.status = failed` span; `PipelineRunFailed` Prometheus alert enters `FIRING` state within 2 minutes |
| T4.6 | Integration | Slack notification delivered | Alertmanager webhook mock receives POST with body containing pipeline name, run ID, failure reason, and dashboard URL |
| T4.7 | Integration | Alert resolves | Next canary run succeeds; `PipelineRunFailed` alert transitions to `RESOLVED`; Alertmanager sends resolved notification |
| T4.8 | Integration | Delay alert fires | No pipeline run metric emitted for `expected_start + threshold`; `PipelineRunDelayed` enters `FIRING` state |

---

## Phase 5 — Legacy Pipeline Support (Zero-Instrumentation Mode)

**Goal:** A pipeline that cannot be modified at all still produces observable traces. The Synthesizer reads log output and scheduler events and constructs synthetic spans without touching the pipeline code.

### Features

#### 5.1 Log Scraper Agent
A standalone service (`synthesizer/log_scraper.py`) that:
- Tails a log source (CloudWatch log group, local file, or stdout stream)
- Matches configurable regex patterns to detect pipeline lifecycle events
- Default patterns for common log formats (e.g. Airflow, Spark driver logs, custom formats)
- Emits synthetic spans to ADOT when patterns match

Default pattern config (`synthesizer/patterns/default.yaml`):
```yaml
patterns:
  run_start:   "Pipeline '(?P<name>.+)' started"
  run_success: "Pipeline '(?P<name>.+)' completed successfully"
  run_failed:  "Pipeline '(?P<name>.+)' failed: (?P<reason>.+)"
  phase_start: "Stage '(?P<phase>.+)' starting"
  phase_end:   "Stage '(?P<phase>.+)' completed in (?P<duration_ms>\\d+)ms"
```

#### 5.2 Scheduler Integration
An Airflow listener plugin (`synthesizer/airflow_listener.py`) that:
- Hooks into Airflow DAG run lifecycle events (on_dag_run_created, on_task_instance_success, on_task_instance_failed)
- Emits spans for each DAG run and task instance
- Generates a `pipeline_run_id` from the DAG run ID (deterministic, same ID on retry)

#### 5.3 Phase Map for Stage Classification
A configuration file (`synthesizer/phase_map/{pipeline_id}.yaml`) that maps log stage names to standard ETL phases (`extract`, `transform`, `load`). This allows the synthesizer to label synthetic spans with meaningful phase names even when the log output uses internal stage names.

#### 5.4 Synthesizer Resilience
- Synthesizer failure must not affect the pipeline being observed
- If the log source is unavailable, the synthesizer waits and retries with backoff
- Partial traces (e.g. start event captured but end event missed due to crash) are stored with status `unknown` and TTL-expired after `max_job_duration + 6h`

### Tests for Phase 5

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T5.1 | Unit | Log pattern match — run start | Log line `"Pipeline 'test-pipe' started"` fed to scraper; scraper emits root span with `pipeline.name = test-pipe` and `pipeline.status = running` |
| T5.2 | Unit | Log pattern match — run success | Log line `"Pipeline 'test-pipe' completed successfully"` fed to scraper; scraper emits span with `pipeline.status = success` |
| T5.3 | Unit | Log pattern match — run failed | Log line `"Pipeline 'test-pipe' failed: NullPointerException"` fed to scraper; span carries `pipeline.failure_reason = NullPointerException` |
| T5.4 | Unit | Log pattern match — phase timing | Phase start and end log lines fed to scraper; synthesized phase span duration matches difference between timestamps in logs |
| T5.5 | Unit | Phase map lookup | Stage name `"process_accounts"` present in phase map; synthesized span carries `etl.phase = transform` |
| T5.6 | Unit | Phase map fallback | Stage name not in phase map; synthesized span carries `etl.phase = unknown`; no error raised |
| T5.7 | Integration | Log scrape full run | Un-instrumented script writes full lifecycle logs; Tempo contains root span + phase spans with correct attributes and timing |
| T5.8 | Integration | Airflow listener — DAG success | Airflow DAG run completes; spans appear in Tempo with `pipeline_run_id` derived deterministically from DAG run ID |
| T5.9 | Integration | Airflow listener — task failure | Airflow task fails; task span in Tempo carries `pipeline.status = failed` and task name |
| T5.10 | Integration | Synthesizer crash recovery | Synthesizer killed after start span emitted; restarted; does not emit duplicate start span; resumes observation |
| T5.11 | Integration | Partial trace TTL | Start span emitted; end span never arrives; after configured TTL, run state store record is absent |

---

## Phase 6 — Streaming Pipeline Observability

**Goal:** Continuously running Kafka-based pipelines are monitored via consumer lag and throughput metrics. A separate streaming dashboard surfaces health without confusing batch run traces with streaming health metrics.

### Features

#### 6.1 Kafka Consumer Metrics Collection
A metrics exporter that collects per-consumer-group metrics from Kafka and exposes them to Prometheus:
- `kafka_consumer_lag_sum` — total lag across all partitions for a consumer group
- `kafka_consumer_throughput_msgs_per_sec` — messages consumed per second
- `kafka_consumer_error_rate` — proportion of messages that resulted in processing errors
- `kafka_end_to_end_latency_ms` — time from message produce to message processed (requires producer timestamp)

All metrics carry labels: `consumer_group`, `topic`, `pipeline_id`, `deployment.environment`

#### 6.2 Streaming Alert Rules
Prometheus rules:
- `StreamingConsumerLagHigh` — consumer lag exceeds configurable threshold for >5 minutes
- `StreamingConsumerDown` — no messages consumed in last 10 minutes for an active topic
- `StreamingErrorRateHigh` — error rate exceeds 1% for >5 minutes

#### 6.3 L2 Streaming Dashboard
A separate dashboard (`dashboards/l2-streaming.json`) with panels:
- Consumer lag over time (per consumer group)
- Throughput over time
- Error rate over time
- End-to-end latency percentiles (p50, p95, p99)

Pipeline registration must declare `pipeline.type: streaming` for these metrics to be surfaced. The batch L2 dashboard is not shown for streaming pipelines.

### Tests for Phase 6

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T6.1 | Integration | Lag metric scraped | Test Kafka consumer with artificially held lag; `kafka_consumer_lag_sum` in Prometheus matches expected lag value within ±5 messages |
| T6.2 | Integration | Throughput metric scraped | Consumer processing 100 msg/s; `kafka_consumer_throughput_msgs_per_sec` gauge reads between 95–105 |
| T6.3 | Unit | Lag alert PromQL — fires | Rule evaluated against mock data where lag > threshold for 6 minutes; alert enters `FIRING` |
| T6.4 | Unit | Lag alert PromQL — does not fire early | Lag > threshold for only 4 minutes; alert remains `PENDING`, not `FIRING` |
| T6.5 | Integration | Consumer down alert | Consumer stopped for 11 minutes; `StreamingConsumerDown` alert fires; Alertmanager mock receives notification |
| T6.6 | Integration | Error rate alert | Consumer error rate injected at 2%; `StreamingErrorRateHigh` alert fires after 6 minutes |
| T6.7 | Integration | Streaming dashboard renders | Prometheus returns lag and throughput metrics; Grafana API query for streaming L2 panels returns data |
| T6.8 | Integration | Env label isolation | Staging consumer group metrics carry `deployment.environment = staging`; Prometheus query scoped to `production` returns no data for those metrics |

---

## Phase 7 — Data Quality Observability

**Goal:** Data quality failures (nulls, schema drift, volume anomalies, freshness breaches) are surfaced as events on pipeline spans and trigger targeted alerts to the owning team.

### Features

#### 7.1 DQ Rule Engine
A library (`sdk/dq.py`) that evaluates rules against a dataframe or result set and emits DQ events as span events:

```python
with run.phase("transform") as phase:
    dq = phase.data_quality()
    dq.check_null_rate("account_id", max_pct=0.01)    # fails if >1% null
    dq.check_volume(min_rows=1000, max_rows=100000)   # fails if outside range
    dq.check_freshness(max_age_hours=25)              # fails if data too old
    dq.check_schema(expected_schema)                  # fails if schema changed
    dq.evaluate()  # emits span events for each violation
```

DQ event attributes:
- `dq.rule_id`, `dq.column`, `dq.severity` (critical | warning | pass)
- `dq.failed_row_count`, `dq.failure_rate_pct`
- `dq.action_taken` (rows_dropped | quarantined | pipeline_failed)
- No raw data values stored — only statistics

#### 7.2 DQ Actions
Configurable per-rule actions on violation:
- `warn` — emit event, continue pipeline, no alert
- `quarantine` — route failing rows to quarantine location, continue with clean rows
- `fail` — fail the pipeline immediately

#### 7.3 DQ Alerts
Prometheus-style alerts derived from DQ span events:
- `DataQualityCriticalViolation` — any DQ check with severity `critical` triggers an alert
- Alert includes: pipeline name, rule ID, column, failure rate, action taken

#### 7.4 DQ Dashboard Panel
Add a "Data Health" panel to the L2 dashboard showing DQ check results per run:
- Table: rule, column, result (pass/warn/fail), failure rate
- Colour-coded by severity

### Tests for Phase 7

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T7.1 | Unit | Null rate check — pass | Dataset with 0.5% nulls against 1% threshold; `dq.severity = pass`; no alert emitted |
| T7.2 | Unit | Null rate check — warn | Dataset with 5% nulls against 1% threshold; `dq.severity = warning`; `dq.failure_rate_pct = 5.0` |
| T7.3 | Unit | Volume check — below min | Dataset with 500 rows; min threshold 1000; `dq.severity = critical`; `dq.action_taken = pipeline_failed`; pipeline raises exception |
| T7.4 | Unit | Volume check — above max | Dataset with 200000 rows; max threshold 100000; `dq.severity = critical` |
| T7.5 | Unit | Schema drift — column removed | Expected schema has column `risk_tier`; actual dataset does not; `schema_drift_detected = true`; `schema_drift_detail` names the missing column |
| T7.6 | Unit | Schema drift — column added | Unexpected column present in dataset; `schema_drift_detected = true`; `schema_drift_detail` names the added column |
| T7.7 | Unit | Freshness check — breach | Dataset timestamp 30 hours ago; max 25 hours; `dq.severity = warning`; DQ event emitted |
| T7.8 | Unit | Quarantine action | 2% nulls with `action: quarantine`; `dq.action_taken = quarantined`; `dq.rows_quarantined = expected_count`; pipeline does not fail |
| T7.9 | Unit | No PII in DQ events | DQ event attributes inspected; no raw data values present; only `dq.failed_row_count` and `dq.failure_rate_pct` stored |
| T7.10 | Integration | DQ events appear in Tempo | Pipeline with DQ violations runs; span in Tempo carries DQ span events with correct `dq.rule_id` and `dq.severity` |
| T7.11 | Integration | DQ critical alert fires | DQ critical violation emitted; `DataQualityCriticalViolation` Prometheus alert enters `FIRING`; Alertmanager receives notification with rule ID and column name |

---

## Phase 8 — SLOs, Error Budgets, and Data Contracts

**Goal:** Every shared pipeline has an SLO and a data contract. Breaches notify consumers directly. Error budget depletion blocks deployments.

### Features

#### 8.1 SLO Definition Schema
YAML schema for SLO definitions (`schema/slo-schema.json`). Example:
```yaml
pipeline_id: customer-360-enrichment
owner: team-customer-data
slos:
  freshness:
    sli: pipeline:freshness_sli:good
    target: 99.5         # percentage
    window: 28d
    sla_seconds: 7200
error_budget:
  freeze_threshold_pct: 10
```

#### 8.2 SLI Recording Rules
Standard Prometheus recording rules (owned centrally, not by teams) that compute SLIs from pipeline run metrics:
- `pipeline:freshness_sli:good` — ratio of runs where data arrived within `sla_seconds`
- `pipeline:success_rate_sli:good` — ratio of runs that completed with status `success`
- `pipeline:latency_sli:good` — ratio of runs that completed within declared duration SLA
- `pipeline:completeness_sli:good` — ratio of runs where row count was within declared range

#### 8.3 Error Budget Tracking
- Error budget consumption computed from SLI recording rules
- Budget dashboard panel added to L1 (per-pipeline error budget remaining %)
- Burn rate alerts at configurable thresholds (fast burn = page, slow burn = ticket)

#### 8.4 Deployment Gate (CI)
A CI check (`ci/error_budget_gate.py`) that:
- Is called as a CI step in team repository PR pipelines
- Fetches current error budget for the pipeline being changed
- Blocks the PR if budget is below `freeze_threshold_pct`
- Requires team lead approval to override

#### 8.5 Data Contract Schema
YAML schema for data contracts (`schema/contract-schema.json`). Contract declares:
- Output schema with evolution policy per column
- Volume range (min/max rows)
- Freshness SLA reference
- DQ rules with max failure rates
- Downstream consumer teams and criticality ratings

#### 8.6 Contract Violation Notifications
When a DQ check tied to a data contract fails:
- The producing pipeline's team is notified (as normal)
- **Each downstream consumer team listed in the contract is also notified directly**
- Notification includes: contract ID, violated rule, affected pipeline, run ID, link to trace

#### 8.7 Schema Evolution CI Gate
CI check that detects when a PR modifies a job writing to a contracted dataset:
- Parses changed job code / schema definition to detect column removals or type changes
- Cross-references the data contract for that dataset
- Automatically adds consumer team leads as PR reviewers if their contract is affected
- PR cannot merge until all required consumer approvals are obtained

### Tests for Phase 8

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T8.1 | Contract | SLO YAML — valid | Valid SLO YAML passes `slo-schema.json` validation |
| T8.2 | Contract | SLO YAML — missing target | SLO YAML without `target` field fails validation with error citing `target` |
| T8.3 | Contract | SLO YAML — target out of range | SLO with `target: 100.1` fails validation (max 99.99) |
| T8.4 | Contract | Data contract YAML — valid | Valid contract YAML passes `contract-schema.json` validation |
| T8.5 | Contract | Data contract YAML — missing consumer criticality | Contract with consumer entry missing `criticality` field fails validation |
| T8.6 | Unit | SLI compute — success rate | Mock Prometheus with 4 success and 1 failure metric; recording rule evaluates to `0.80` |
| T8.7 | Unit | SLI compute — freshness | 3 runs within SLA, 1 run 10 minutes late; freshness SLI evaluates to `0.75` |
| T8.8 | Unit | Error budget gate — blocks | Budget at 8%; gate script called with `freeze_threshold = 10`; exits non-zero with message |
| T8.9 | Unit | Error budget gate — allows | Budget at 50%; gate script exits 0; PR proceeds |
| T8.10 | Unit | Error budget gate — override label | Budget at 8%; PR has `override-error-budget` label and team lead approval; gate exits 0 |
| T8.11 | Integration | Contract violation — consumer notified | DQ failure on contracted dataset; Alertmanager mock receives two notifications: one for producer team, one for consumer team |
| T8.12 | Integration | Contract violation notification content | Consumer notification contains: contract ID, violated rule, pipeline name, run ID, trace link |
| T8.13 | Unit | Schema evolution gate — column removal detected | PR diff removing column `risk_tier` from job schema; gate script identifies column as removed and outputs consumer team names requiring approval |
| T8.14 | Unit | Schema evolution gate — nullable add permitted | PR diff adding nullable column; gate script exits 0 without requiring consumer approvals |

---

## Phase 9 — Federation & CI Enforcement Gates

**Goal:** The platform's non-negotiable contract is enforced mechanically. Non-compliant spans are rejected before storage. Non-compliant configuration is rejected before merge. Teams cannot accidentally bypass policy.

### Features

#### 9.1 Span Compliance Gate (ADOT Processor)
Add a validation processor to the ADOT collector pipeline that:
- Checks each incoming span for required attributes (`pipeline.run_id`, `pipeline.name`, `deployment.environment`)
- Routes non-compliant spans to a quarantine log (not Tempo) with reason recorded
- Emits a Prometheus metric `spans_quarantined_total` with label `reason`
- Compliant spans pass through normally

#### 9.2 CI Schema Validation Gate (Teams)
A reusable GitHub Actions job (`ci/validate-observability-config.yaml`) that team repos import:
- Validates topology YAML, policy YAML, SLO YAML, and data contract YAML against central schemas
- Blocks PR if any file fails validation
- Posts schema validation summary as a PR comment

#### 9.3 Span Compliance Test (Staging Gate)
CI step that runs after a changed pipeline is deployed to staging:
- Triggers a test run of the changed pipeline
- Asserts `pipeline_run_id` is present on all emitted spans
- Asserts all required span attributes are present and correctly typed
- Asserts no PII attributes reach Tempo unredacted
- Blocks promotion to production if any assertion fails

#### 9.4 GitOps Config Sync
A sync mechanism that applies team-owned config (alert rules, dashboards) to the central platform on merge:
- On merge to team repo `main`, a GitHub Action calls the Grafana API to upsert dashboards
- Alert rules are applied via Prometheus rule file update
- No manual config changes via UI are permitted (enforced by Grafana org-level setting)

### Tests for Phase 9

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T9.1 | Integration | Span quarantine — missing run_id | Span without `pipeline_run_id` sent to collector; Tempo API returns 0 results for that span_id; quarantine log contains entry with `reason = missing_run_id` |
| T9.2 | Integration | Span quarantine — missing environment | Span without `deployment.environment` sent to collector; quarantined with `reason = missing_environment` |
| T9.3 | Integration | Compliant span passes through | Span with all required attributes sent; Tempo API returns span with correct attributes within 2 seconds |
| T9.4 | Integration | Quarantine counter increments | 10 non-compliant spans sent; `spans_quarantined_total` Prometheus counter = 10 |
| T9.5 | Contract | Config CI gate — invalid SLO | PR with SLO YAML missing `target`; CI exits non-zero; PR comment contains validation error text |
| T9.6 | Contract | Config CI gate — unknown strategy | Policy YAML referencing strategy not in central catalogue; CI exits non-zero |
| T9.7 | Integration | Staging span compliance gate | Staging canary run with PII field unredacted in span; staging gate script exits non-zero; deployment to production blocked |
| T9.8 | Integration | GitOps dashboard sync | Dashboard JSON committed to team repo `main`; within 2 minutes, Grafana API GET for that dashboard returns the updated JSON |
| T9.9 | Integration | GitOps idempotent | Same dashboard JSON deployed twice; second deployment does not create a duplicate; Grafana API returns exactly one dashboard with that `uid` |

---

## Phase 10 — Self-Service Onboarding (obs-cli)

**Goal:** A pipeline team can register a new pipeline and be fully onboarded — with spans flowing and validated — in under 2 hours, without raising any ticket to the central platform team.

### Features

#### 10.1 CLI Tool (`cli/obs-cli`)
A Python CLI tool with the following commands:

```
obs-cli pipeline register   # prompts for pipeline_id, domain, owner, tier; creates registration record
obs-cli topology init       # generates skeleton topology YAML for the pipeline
obs-cli slo init            # generates SLO YAML with defaults based on pipeline schedule
obs-cli contract init       # generates data contract YAML for each declared output dataset
obs-cli validate            # validates all generated files against central schemas (local, no network needed)
obs-cli verify              # after first staging run, queries Tempo and asserts spans flowing correctly
```

#### 10.2 Registration Record
`obs-cli pipeline register` creates a file `observability/pipeline-registration.yaml` in the team repo:
```yaml
pipeline_id: customer-360-enrichment
domain: customer-data
owner: team-customer-data
instrumentation_tier: 1   # 1=native, 2=light-touch, 3=zero-instrumentation
```

#### 10.3 Skeleton File Generation
`obs-cli topology init` and `obs-cli slo init` generate valid, schema-compliant YAML with placeholder values and inline comments explaining each field. Files are written to `observability/topology/`, `observability/slos/`, `observability/contracts/`.

#### 10.4 Local Validation
`obs-cli validate` runs entirely offline — it bundles the central schemas and validates all files in `observability/` against them. This gives instant feedback before committing, preventing CI failures.

#### 10.5 Span Flow Verification
`obs-cli verify --pipeline-id customer-360-enrichment --since 30m` queries Tempo and asserts:
- At least one span with `pipeline_run_id` set exists in the last 30 minutes
- Root span carries all required attributes
- No spans for this pipeline appear in the quarantine log

### Tests for Phase 10

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T10.1 | Unit | Register — valid pipeline_id | `obs-cli pipeline register --pipeline-id cust-360` creates `pipeline-registration.yaml` with correct content |
| T10.2 | Unit | Register — invalid pipeline_id with spaces | `obs-cli pipeline register --pipeline-id "cust 360"` exits non-zero with error message citing invalid character |
| T10.3 | Unit | Register — duplicate pipeline_id | Running register for an already-registered `pipeline_id`; CLI exits non-zero with `already registered` message |
| T10.4 | Unit | Skeleton SLO file generated | `obs-cli slo init` creates `observability/slos/{pipeline_id}.yaml`; file passes `slo-schema.json` validation |
| T10.5 | Unit | Skeleton contract file generated | `obs-cli contract init` creates `observability/contracts/{pipeline_id}.yaml`; file passes `contract-schema.json` validation |
| T10.6 | Unit | Local validate — valid files | All generated skeleton files pass `obs-cli validate`; command exits 0; output lists files checked |
| T10.7 | Unit | Local validate — offline | `obs-cli validate` run with `HTTPS_PROXY` disabled and DNS blocked; exits 0 (uses bundled schemas, no network call) |
| T10.8 | Unit | Local validate — invalid file | Hand-edited SLO YAML with missing `target`; `obs-cli validate` exits non-zero; output names the file and field |
| T10.9 | Integration | Verify — spans found | After instrumented staging run; `obs-cli verify --pipeline-id cust-360 --since 30m` exits 0; output confirms span count and run_id |
| T10.10 | Integration | Verify — no spans found | No run executed; `obs-cli verify --pipeline-id cust-360 --since 30m` exits non-zero; output says "no spans found in Tempo for this pipeline in the last 30 minutes" |
| T10.11 | Integration | Verify — quarantined spans detected | Pipeline emitting non-compliant spans; `obs-cli verify` exits non-zero; output warns that spans were quarantined and names the reason |

---

## Phase 11 — Infrastructure Observability

**Goal:** Infrastructure health metrics (CPU, memory, disk, container health) for the machines running data pipelines are visible in Grafana alongside pipeline run data, so support staff can correlate a pipeline failure with an infrastructure event.

### Features

#### 11.1 Kubernetes Metrics
Deploy a Prometheus Node Exporter and kube-state-metrics (or equivalent) to collect:
- Node CPU and memory utilisation
- Pod restart counts and OOM kills
- Persistent volume usage

#### 11.2 Spark / Glue Job Metrics
A Spark metrics listener that exports to Prometheus:
- Driver and executor JVM heap usage
- GC time
- Task duration distribution (p50, p95)
- Stage-level record counts

#### 11.3 Infrastructure Dashboard
L3 dashboard (`dashboards/l3-infra.json`) with panels:
- CPU and memory utilisation heatmap (by node / by pipeline)
- Pod health table
- Spark executor utilisation per active job
- Correlatable time axis with L2 pipeline run timeline

#### 11.4 Infrastructure Alerts
Prometheus rules:
- `InfrastructureCPUHigh` — node CPU >85% for >10 minutes
- `PodOOMKilled` — any pod OOM-killed
- `SparkExecutorHighGC` — GC time >20% of task time

### Tests for Phase 11

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T11.1 | Integration | K8s CPU metric scraped | Node exporter running against test cluster; `node_cpu_seconds_total` metric appears in Prometheus with `node` label |
| T11.2 | Integration | K8s memory metric scraped | `node_memory_MemAvailable_bytes` metric present in Prometheus for each node in test cluster |
| T11.3 | Integration | Pod OOM metric scraped | `kube_pod_container_status_last_terminated_reason{reason="OOMKilled"}` appears in Prometheus after OOM event |
| T11.4 | Integration | Spark heap metric scraped | Test Spark job runs; `spark_executor_jvm_heap_used_bytes` metric appears in Prometheus with `spark_app_id` label |
| T11.5 | Integration | Spark GC metric scraped | `spark_executor_jvm_gc_time_ms` metric present in Prometheus after Spark job completes |
| T11.6 | Unit | CPU alert PromQL — fires at threshold | Mock Prometheus data with CPU > 85% for 11 minutes; `InfrastructureCPUHigh` enters `FIRING` |
| T11.7 | Unit | CPU alert PromQL — does not fire below threshold | CPU at 80% for 11 minutes; alert remains inactive |
| T11.8 | Integration | L3 dashboard time axis | Grafana API query for L3 dashboard during period of both infrastructure metrics and pipeline run; both data series share the same time range |

---

## Phase 12 — Multi-Environment Hardening & Cost Controls

**Goal:** Production data is provably isolated from staging. Observability infrastructure does not generate runaway costs.

### Features

#### 12.1 Tempo Namespace Enforcement
- Staging and production Tempo tenants are separate namespaces
- Grafana datasources for staging and production are configured separately
- The `$environment` template variable in Grafana drives which datasource is queried — not a filter applied to a shared datasource
- No cross-namespace query is possible through the Grafana UI

#### 12.2 Cardinality Guard in ADOT
ADOT collector metric pipeline processor that:
- Strips `pipeline_run_id` from all metric data points before forwarding to Prometheus
- Strips any label with cardinality >1000 unique values (measured over a 10-minute window)
- Emits a counter `metrics_labels_stripped_total` for monitoring

#### 12.3 Span Sampling for High-Volume Pipelines
Configurable tail-based sampling in ADOT:
- Pipelines declared as high-volume (`sampling.success_rate: 0.1`) have 10% of success spans sampled
- Error spans are always retained at 100% regardless of sampling config
- Sampling config is per `pipeline_id` in the central platform config

#### 12.4 Tiered Storage Lifecycle Rules
Tempo and Loki storage lifecycle configuration:
- Hot tier: last 7 days (fast SSD / high-performance object storage)
- Warm tier: 7–30 days (standard object storage)
- Cold tier: 30–365 days (infrequent access / Glacier equivalent)
- Automated lifecycle rule transitions with no manual intervention required
- Data beyond 365 days deleted unless a regulatory retention override is declared for the pipeline

#### 12.5 Cost Visibility
A Grafana dashboard panel (added to L1) showing estimated observability infrastructure cost:
- Storage cost estimate (based on data volume × tier pricing)
- Ingest cost estimate (based on span count × collector pricing)
- Breakdown by domain/team where feasible

### Tests for Phase 12

| # | Type | Test | Pass Condition |
|---|------|------|----------------|
| T12.1 | Integration | Env namespace isolation | Span written to Tempo staging namespace; Tempo production namespace API query for that `trace_id` returns 404 |
| T12.2 | Integration | Grafana datasource scoping | Grafana production datasource configuration points to production Tempo namespace; API call with staging `trace_id` returns empty result |
| T12.3 | Integration | Cardinality guard — run_id stripped | Span with `pipeline_run_id` emitted as a metric attribute; Prometheus `/api/v1/label/__names__` does not contain `pipeline_run_id` |
| T12.4 | Integration | Cardinality guard counter | High-cardinality label stripped by ADOT; `metrics_labels_stripped_total` counter increments in Prometheus |
| T12.5 | Integration | Sampling — success spans sampled | High-volume pipeline config `sampling.success_rate: 0.1`; 100 success spans emitted; between 5–20 appear in Tempo (statistical range) |
| T12.6 | Integration | Sampling — error spans not sampled | Same pipeline; 10 error spans emitted; all 10 appear in Tempo (100% retention for errors) |
| T12.7 | Integration | Tiered storage — hot tier queryable | Span written 1 day ago; Tempo API query returns span within 2 seconds |
| T12.8 | Integration | Tiered storage — warm tier queryable | Span written 8 days ago (moved to warm tier by lifecycle rule); Tempo API query returns span (latency may be higher) |
| T12.9 | Integration | Lifecycle TTL — data deleted | Span written with timestamp > 365 days ago (injected via backdating); lifecycle rule deletes it; Tempo API returns 404 for that `trace_id` |
| T12.10 | Unit | Lifecycle override — data retained | Pipeline with `regulatory_retention_override: true`; lifecycle rule script excludes that pipeline's data from deletion; span remains queryable |

---

## Milestone Summary

| Phase | What You Can Do After Completion | Approximate Scope |
|-------|----------------------------------|-------------------|
| 0 | Run the full stack locally; validate span schema | Foundation |
| 1 | Instrument a new pipeline with 5 lines of code | SDK |
| 2 | See complete batch pipeline traces in Tempo | Core observability |
| 3 | Support staff can find and investigate any run in Grafana | Dashboard |
| 4 | Team is alerted within 2 minutes of a failure | Alerting |
| 5 | Legacy pipelines are observable without code changes | Legacy support |
| 6 | Streaming pipelines are monitored via lag and throughput | Streaming |
| 7 | Data quality violations are detected and surfaced | Data quality |
| 8 | SLOs, error budgets, and data contracts are enforced | Reliability contracts |
| 9 | Non-compliant spans and config are mechanically rejected | Federation enforcement |
| 10 | Any team can onboard a new pipeline without central team help | Self-service |
| 11 | Infrastructure metrics correlate with pipeline traces | Infra observability |
| 12 | Production is isolated from staging; costs are controlled | Hardening |

---

## Definition of Done (Per Phase)

A phase is complete when:
1. All feature code is merged to `main` via pull request
2. All tests listed for that phase pass in CI (not just locally)
3. The canary pipeline (`tests/canary_pipeline.py`) still passes end-to-end after the phase is merged
4. The L1 dashboard renders without error in the staging environment
5. No new Prometheus alerts are firing that were not firing before the phase began
