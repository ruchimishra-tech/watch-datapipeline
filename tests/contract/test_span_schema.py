"""
Contract tests for the canonical span schema (schema/span-schema.json).
Tests T0.2 – T0.22 from execution.md Phase 0.

Run: pytest tests/contract/test_span_schema.py -v
No services required — pure offline schema validation.
"""
import json
import pathlib
import pytest
import jsonschema

ROOT = pathlib.Path(__file__).parents[2]
SCHEMA_PATH = ROOT / "schema" / "span-schema.json"
FIXTURES = ROOT / "schema" / "fixtures"


@pytest.fixture(scope="module")
def schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def validate(schema, data):
    """Returns None if valid, raises jsonschema.ValidationError if not."""
    validator = jsonschema.Draft7Validator(schema)
    errors = list(validator.iter_errors(data))
    return errors


# ── Valid fixtures ─────────────────────────────────────────────────────────

@pytest.mark.phase0
def test_T0_2_valid_root_span(schema):
    """T0.2 — valid root span passes schema validation."""
    with open(FIXTURES / "valid_root_span.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors == [], f"Unexpected validation errors: {errors}"


@pytest.mark.phase0
def test_valid_extract_span(schema):
    """Valid extract span with all required fields passes."""
    with open(FIXTURES / "valid_extract_span.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors == [], f"Unexpected validation errors: {errors}"


@pytest.mark.phase0
def test_valid_transform_span(schema):
    """Valid transform span passes."""
    with open(FIXTURES / "valid_transform_span.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors == [], f"Unexpected validation errors: {errors}"


@pytest.mark.phase0
def test_valid_load_span(schema):
    """Valid load span passes."""
    with open(FIXTURES / "valid_load_span.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors == [], f"Unexpected validation errors: {errors}"


# ── Root span invalid fixtures ─────────────────────────────────────────────

@pytest.mark.phase0
def test_T0_3_missing_run_id(schema):
    """T0.3 — span without pipeline.run_id fails validation."""
    with open(FIXTURES / "invalid_missing_run_id.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors, "Expected validation errors for missing pipeline.run_id but got none"


@pytest.mark.phase0
def test_T0_4_bad_env_enum(schema):
    """T0.4 — span with deployment.environment not in enum fails."""
    with open(FIXTURES / "invalid_bad_env_enum.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors, "Expected validation errors for invalid environment value"


@pytest.mark.phase0
def test_failed_span_requires_failure_reason(schema):
    """T0.x — span with status=failed but no failure_reason fails."""
    with open(FIXTURES / "invalid_failed_no_reason.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors, "Expected validation error: failed status requires pipeline.failure_reason"


@pytest.mark.phase0
def test_pipeline_id_must_be_lowercase_hyphenated(schema):
    """pipeline.id with spaces or uppercase must fail."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "My Pipeline",   # spaces — invalid
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "success"
    }
    errors = validate(schema, data)
    assert errors, "pipeline.id with spaces should fail validation"


# ── Extract span invalid fixtures ──────────────────────────────────────────

@pytest.mark.phase0
def test_T0_8_sql_with_literal_value(schema):
    """T0.8 — SQL with literal string value (not bind variable) fails."""
    with open(FIXTURES / "invalid_sql_with_literal.json") as f:
        data = json.load(f)
    errors = validate(schema, data)
    assert errors, "SQL with literal values should fail — only bind variable markers (?) permitted"


@pytest.mark.phase0
def test_T0_9_incremental_without_key(schema):
    """T0.9 — incremental query type without incremental_key fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "running",
        "etl.phase": "extract",
        "extract.source.system": "postgres",
        "extract.query.type": "incremental",  # requires incremental_key
        "extract.rows_read": 1000
        # missing: extract.query.incremental_key
    }
    errors = validate(schema, data)
    assert errors, "incremental query type without incremental_key should fail"


@pytest.mark.phase0
def test_T0_11_invalid_partition_format(schema):
    """T0.11 — partition not matching key=value format fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "running",
        "etl.phase": "extract",
        "extract.source.system": "s3",
        "extract.source.partition": "2026-03-16",  # missing key= prefix
        "extract.query.type": "full_table",
        "extract.rows_read": 100
    }
    errors = validate(schema, data)
    assert errors, "Partition without key=value format should fail"


@pytest.mark.phase0
def test_extract_source_system_enum(schema):
    """extract.source.system value not in enum fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "running",
        "etl.phase": "extract",
        "extract.source.system": "sap-hana",   # not in enum
        "extract.query.type": "full_table",
        "extract.rows_read": 100
    }
    errors = validate(schema, data)
    assert errors, "Unknown source system should fail enum validation"


# ── Transform span invalid fixtures ───────────────────────────────────────

@pytest.mark.phase0
def test_T0_12_operations_not_array(schema):
    """T0.12 — transform.operations as non-array fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "running",
        "etl.phase": "transform",
        "transform.operations": "filter_then_join",  # should be an array
        "transform.rows_in": 1000,
        "transform.rows_out": 900,
        "transform.engine": "spark"
    }
    errors = validate(schema, data)
    assert errors, "transform.operations as string should fail — must be array"


@pytest.mark.phase0
def test_T0_13_invalid_join_type(schema):
    """T0.13 — join type not in enum fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "running",
        "etl.phase": "transform",
        "transform.joins": [
            {
                "type": "anti",   # not in enum
                "left_dataset": "a",
                "right_dataset": "b",
                "join_keys": ["id"]
            }
        ],
        "transform.rows_in": 1000,
        "transform.rows_out": 900,
        "transform.engine": "pandas"
    }
    errors = validate(schema, data)
    assert errors, "Join type 'anti' is not in the enum and should fail"


@pytest.mark.phase0
def test_T0_15_schema_drift_without_detail(schema):
    """T0.15 — schema_drift_detected=true without schema_drift_detail fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "running",
        "etl.phase": "transform",
        "transform.schema_drift_detected": True,
        # missing: transform.schema_drift_detail
        "transform.rows_in": 100,
        "transform.rows_out": 100,
        "transform.engine": "spark"
    }
    errors = validate(schema, data)
    assert errors, "schema_drift_detected=true without schema_drift_detail should fail"


@pytest.mark.phase0
def test_T0_16_invalid_engine_enum(schema):
    """T0.16 — transform.engine value not in enum fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "running",
        "etl.phase": "transform",
        "transform.rows_in": 1000,
        "transform.rows_out": 900,
        "transform.engine": "numpy"   # not in enum
    }
    errors = validate(schema, data)
    assert errors, "Engine 'numpy' is not in enum and should fail"


# ── Load span invalid fixtures ─────────────────────────────────────────────

@pytest.mark.phase0
def test_T0_18_invalid_write_mode(schema):
    """T0.18 — write_mode not in enum fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "success",
        "etl.phase": "load",
        "load.target.system": "snowflake",
        "load.write_mode": "truncate_insert",   # not in enum
        "load.rows_written": 1000
    }
    errors = validate(schema, data)
    assert errors, "write_mode 'truncate_insert' is not in enum and should fail"


@pytest.mark.phase0
def test_T0_19_merge_without_merge_keys(schema):
    """T0.19 — write_mode=merge without merge_keys fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "success",
        "etl.phase": "load",
        "load.target.system": "snowflake",
        "load.write_mode": "merge",
        "load.merge_keys": [],   # empty — must have at least one key
        "load.rows_written": 1000
    }
    errors = validate(schema, data)
    assert errors, "merge write_mode with empty merge_keys should fail"


@pytest.mark.phase0
def test_T0_21_iceberg_without_snapshot_id(schema):
    """T0.21 — iceberg target format without snapshot_id fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "success",
        "etl.phase": "load",
        "load.target.system": "s3",
        "load.target.format": "iceberg",
        # missing: load.snapshot_id
        "load.write_mode": "append",
        "load.rows_written": 500
    }
    errors = validate(schema, data)
    assert errors, "iceberg format without snapshot_id should fail"


@pytest.mark.phase0
def test_T0_22_post_check_failed_without_reason(schema):
    """T0.22 — post_check.passed=false without failure_reason fails."""
    data = {
        "pipeline.run_id": "550e8400-e29b-41d4-a716-446655440000",
        "pipeline.name": "Test",
        "pipeline.id": "test-pipeline",
        "pipeline.type": "batch",
        "deployment.environment": "dev",
        "pipeline.status": "success",
        "etl.phase": "load",
        "load.target.system": "snowflake",
        "load.write_mode": "append",
        "load.rows_written": 500,
        "load.post_check.passed": False
        # missing: load.post_check.failure_reason
    }
    errors = validate(schema, data)
    assert errors, "post_check.passed=false without failure_reason should fail"
