"""
generate_lineage.py
--------------------
Generates a static OpenLineage NDJSON file for customer_orders_pipeline.py.

Lineage is code/design-based — not runtime. It describes what the job reads,
what it writes, and the column-level transformation logic, as declared in the
pipeline code. No Spark context is required to run this script.

Output: ../output/customer_orders_lineage.ndjson
        (one JSON object per line — POST each line to Marquez /api/v1/lineage)
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PRODUCER = "https://github.com/ruchimishra-tech/watch-datapipeline/spark_lineage_experiment"
OPENLINEAGE_SPEC = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

JOB_NAMESPACE = "spark://data-platform"
JOB_NAME      = "customer_orders_pipeline"

POSTGRES_NS   = "postgres://prod-db.internal:5432"
S3_NS         = "s3://data-lake"

# Fixed run ID for static/design-time lineage (not a real execution UUID)
STATIC_RUN_ID = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{JOB_NAMESPACE}/{JOB_NAME}/static-v1"))

NOMINAL_TIME  = "2024-01-01T00:00:00Z"   # representative design-time timestamp
EVENT_TIME    = datetime.now(timezone.utc).isoformat()

OUTPUT_PATH   = Path(__file__).parent.parent / "output" / "customer_orders_lineage.ndjson"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def facet_meta(schema_url: str) -> dict:
    return {"_producer": PRODUCER, "_schemaURL": schema_url}


def schema_facet(fields: list[dict]) -> dict:
    return {
        **facet_meta("https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json"),
        "fields": fields,
    }


def datasource_facet(name: str, uri: str) -> dict:
    return {
        **facet_meta("https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json"),
        "name": name,
        "uri": uri,
    }


def column_lineage_facet(fields: dict) -> dict:
    return {
        **facet_meta("https://openlineage.io/spec/facets/1-0-0/ColumnLineageDatasetFacet.json"),
        "fields": fields,
    }


def input_field(namespace: str, dataset: str, field: str,
                transform_type: str = "IDENTITY",
                description: str = "") -> dict:
    entry = {
        "namespace": namespace,
        "name": dataset,
        "field": field,
        "transformationType": transform_type,
    }
    if description:
        entry["transformationDescription"] = description
    return entry


# ---------------------------------------------------------------------------
# Input Dataset Definitions
# ---------------------------------------------------------------------------

# --- public.customers -------------------------------------------------------
DS_CUSTOMERS = {
    "namespace": POSTGRES_NS,
    "name": "retail.public.customers",
    "facets": {
        "schema": schema_facet([
            {"name": "customer_id",  "type": "INTEGER",   "description": "Primary key"},
            {"name": "first_name",   "type": "VARCHAR",   "description": "Customer first name"},
            {"name": "last_name",    "type": "VARCHAR",   "description": "Customer last name"},
            {"name": "email",        "type": "VARCHAR",   "description": "Contact email — PII"},
            {"name": "region",       "type": "VARCHAR",   "description": "Sales region"},
            {"name": "status",       "type": "VARCHAR",   "description": "ACTIVE | INACTIVE | SUSPENDED"},
            {"name": "created_at",   "type": "TIMESTAMP", "description": "Record creation time"},
            {"name": "updated_at",   "type": "TIMESTAMP", "description": "Last update time"},
        ]),
        "dataSource": datasource_facet(
            name="postgres-retail-prod",
            uri=f"{POSTGRES_NS}/retail"
        ),
        "documentation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json"),
            "description": "Master customer table. Only ACTIVE records are used by this pipeline.",
        },
    },
}

# --- public.orders ----------------------------------------------------------
DS_ORDERS = {
    "namespace": POSTGRES_NS,
    "name": "retail.public.orders",
    "facets": {
        "schema": schema_facet([
            {"name": "order_id",    "type": "INTEGER",       "description": "Primary key"},
            {"name": "customer_id", "type": "INTEGER",       "description": "FK -> customers.customer_id"},
            {"name": "order_date",  "type": "DATE",          "description": "Date order was placed"},
            {"name": "order_total", "type": "DECIMAL(10,2)", "description": "Pre-tax order total"},
            {"name": "status",      "type": "VARCHAR",       "description": "Order lifecycle status"},
            {"name": "created_at",  "type": "TIMESTAMP",     "description": "Record creation time"},
        ]),
        "dataSource": datasource_facet(
            name="postgres-retail-prod",
            uri=f"{POSTGRES_NS}/retail"
        ),
        "documentation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json"),
            "description": "All customer orders. Joined to order_items for line-level aggregation.",
        },
    },
}

# --- public.order_items -----------------------------------------------------
DS_ORDER_ITEMS = {
    "namespace": POSTGRES_NS,
    "name": "retail.public.order_items",
    "facets": {
        "schema": schema_facet([
            {"name": "item_id",    "type": "INTEGER",       "description": "Primary key"},
            {"name": "order_id",   "type": "INTEGER",       "description": "FK -> orders.order_id"},
            {"name": "product_id", "type": "INTEGER",       "description": "FK -> product_catalog.product_id"},
            {"name": "quantity",   "type": "INTEGER",       "description": "Units ordered"},
            {"name": "unit_price", "type": "DECIMAL(10,2)", "description": "Price per unit at time of order"},
        ]),
        "dataSource": datasource_facet(
            name="postgres-retail-prod",
            uri=f"{POSTGRES_NS}/retail"
        ),
        "documentation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json"),
            "description": "Line items per order. Source for quantity and revenue aggregations.",
        },
    },
}

# --- s3: product_catalog ----------------------------------------------------
DS_PRODUCT_CATALOG = {
    "namespace": S3_NS,
    "name": "raw/product_catalog",
    "facets": {
        "schema": schema_facet([
            {"name": "product_id",   "type": "INTEGER",       "description": "Primary key"},
            {"name": "product_name", "type": "VARCHAR",       "description": "Display name"},
            {"name": "category",     "type": "VARCHAR",       "description": "Product category"},
            {"name": "brand",        "type": "VARCHAR",       "description": "Brand name"},
            {"name": "unit_cost",    "type": "DECIMAL(10,2)", "description": "Wholesale cost"},
        ]),
        "dataSource": datasource_facet(
            name="s3-data-lake-raw",
            uri="s3://data-lake/raw/product_catalog/"
        ),
        "documentation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json"),
            "description": "Product reference data. Parquet files partitioned by ingestion date. Used to enrich order items with product name and category.",
        },
        "storage": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json"),
            "storageLayer": "S3",
            "fileFormat": "PARQUET",
        },
    },
}


# ---------------------------------------------------------------------------
# Output Dataset Definitions
# ---------------------------------------------------------------------------

# Column lineage for customer_order_summary
SUMMARY_COLUMN_LINEAGE = column_lineage_facet({
    "customer_id": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.customers", "customer_id"),
        ],
    },
    "customer_name": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.customers", "first_name",
                        "TRANSFORMATION",
                        "Concatenated first_name and last_name with space separator via concat_ws"),
            input_field(POSTGRES_NS, "retail.public.customers", "last_name",
                        "TRANSFORMATION",
                        "Concatenated first_name and last_name with space separator via concat_ws"),
        ],
    },
    "email": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.customers", "email"),
        ],
    },
    "region": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.customers", "region"),
        ],
    },
    "total_orders": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.orders", "order_id",
                        "AGGREGATION",
                        "COUNT DISTINCT of order_id per customer_id"),
        ],
    },
    "total_items": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.order_items", "quantity",
                        "AGGREGATION",
                        "SUM of quantity per customer_id across all orders"),
        ],
    },
    "total_spend": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.order_items", "quantity",
                        "AGGREGATION",
                        "SUM of quantity * unit_price (line_amount) per customer_id"),
            input_field(POSTGRES_NS, "retail.public.order_items", "unit_price",
                        "AGGREGATION",
                        "SUM of quantity * unit_price (line_amount) per customer_id"),
        ],
    },
    "avg_order_value": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.orders", "order_total",
                        "AGGREGATION",
                        "AVG of order_total per customer_id"),
        ],
    },
    "first_order_date": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.orders", "order_date",
                        "AGGREGATION", "MIN of order_date per customer_id"),
        ],
    },
    "last_order_date": {
        "inputFields": [
            input_field(POSTGRES_NS, "retail.public.orders", "order_date",
                        "AGGREGATION", "MAX of order_date per customer_id"),
        ],
    },
    "top_product_name": {
        "inputFields": [
            input_field(S3_NS, "raw/product_catalog", "product_name",
                        "TRANSFORMATION",
                        "Product with highest total quantity ordered by customer; resolved via RANK window function over (customer_id, product_id) ordered by product_total_qty DESC"),
            input_field(POSTGRES_NS, "retail.public.order_items", "quantity",
                        "TRANSFORMATION",
                        "Aggregated per (customer_id, product_id) to determine top product"),
        ],
    },
    "spend_rank_in_region": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "total_spend",
                        "TRANSFORMATION",
                        "RANK window function partitioned by region, ordered by total_spend DESC"),
            input_field(POSTGRES_NS, "retail.public.customers", "region",
                        "TRANSFORMATION",
                        "Partition key for the spend ranking window"),
        ],
    },
    "pipeline_run_date": {
        "inputFields": [],
        "transformationType": "CONSTANT",
        "transformationDescription": "current_date() at pipeline execution time — no source field",
    },
})

DS_CUSTOMER_ORDER_SUMMARY = {
    "namespace": S3_NS,
    "name": "curated/customer_order_summary",
    "facets": {
        "schema": schema_facet([
            {"name": "customer_id",         "type": "INTEGER",        "description": "Customer primary key"},
            {"name": "customer_name",        "type": "VARCHAR",        "description": "Full name — PII"},
            {"name": "email",                "type": "VARCHAR",        "description": "Email address — PII"},
            {"name": "region",               "type": "VARCHAR",        "description": "Sales region (partition key)"},
            {"name": "total_orders",         "type": "INTEGER",        "description": "Count of distinct orders"},
            {"name": "total_items",          "type": "INTEGER",        "description": "Sum of all items ordered"},
            {"name": "total_spend",          "type": "DECIMAL(12,2)",  "description": "Sum of all line amounts"},
            {"name": "avg_order_value",      "type": "DECIMAL(10,2)",  "description": "Average order total"},
            {"name": "first_order_date",     "type": "DATE",           "description": "Date of first order"},
            {"name": "last_order_date",      "type": "DATE",           "description": "Date of most recent order"},
            {"name": "top_product_name",     "type": "VARCHAR",        "description": "Most ordered product by quantity"},
            {"name": "spend_rank_in_region", "type": "INTEGER",        "description": "Spend rank within sales region"},
            {"name": "pipeline_run_date",    "type": "DATE",           "description": "Date this record was generated"},
        ]),
        "dataSource": datasource_facet(
            name="s3-data-lake-curated",
            uri="s3://data-lake/curated/customer_order_summary/"
        ),
        "columnLineage": SUMMARY_COLUMN_LINEAGE,
        "storage": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json"),
            "storageLayer": "S3",
            "fileFormat": "PARQUET",
            "partitionedBy": ["region"],
        },
        "documentation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json"),
            "description": "Curated per-customer order summary with spend aggregations, top product, and regional spend rank. Partitioned by region. Overwritten on each pipeline run.",
        },
        "ownership": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/OwnershipDatasetFacet.json"),
            "owners": [{"name": "team:data-platform", "type": "TEAM"}],
        },
    },
}

# Column lineage for reporting.customer_metrics
METRICS_COLUMN_LINEAGE = column_lineage_facet({
    "customer_id": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "customer_id"),
        ],
    },
    "customer_name": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "customer_name"),
        ],
    },
    "region": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "region"),
        ],
    },
    "total_spend": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "total_spend"),
        ],
    },
    "spend_rank_in_region": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "spend_rank_in_region"),
        ],
    },
    "total_orders": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "total_orders"),
        ],
    },
    "last_order_date": {
        "inputFields": [
            input_field(S3_NS, "curated/customer_order_summary", "last_order_date"),
        ],
    },
    "updated_at": {
        "inputFields": [],
        "transformationType": "CONSTANT",
        "transformationDescription": "current_timestamp() at pipeline execution time",
    },
})

DS_CUSTOMER_METRICS = {
    "namespace": POSTGRES_NS,
    "name": "reporting.customer_metrics",
    "facets": {
        "schema": schema_facet([
            {"name": "customer_id",         "type": "INTEGER",       "description": "Customer primary key"},
            {"name": "customer_name",        "type": "VARCHAR",       "description": "Full name — PII"},
            {"name": "region",               "type": "VARCHAR",       "description": "Sales region"},
            {"name": "total_spend",          "type": "DECIMAL(12,2)", "description": "Lifetime spend"},
            {"name": "spend_rank_in_region", "type": "INTEGER",       "description": "Spend rank within region"},
            {"name": "total_orders",         "type": "INTEGER",       "description": "Total order count"},
            {"name": "last_order_date",      "type": "DATE",          "description": "Most recent order date"},
            {"name": "updated_at",           "type": "TIMESTAMP",     "description": "Pipeline write timestamp"},
        ]),
        "dataSource": datasource_facet(
            name="postgres-reporting-prod",
            uri=f"{POSTGRES_NS}/reporting"
        ),
        "columnLineage": METRICS_COLUMN_LINEAGE,
        "documentation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json"),
            "description": "Reporting-layer customer metrics table. Full overwrite on each pipeline run. Consumed by BI dashboards and downstream reporting pipelines.",
        },
        "ownership": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/OwnershipDatasetFacet.json"),
            "owners": [{"name": "team:data-platform", "type": "TEAM"}],
        },
    },
}


# ---------------------------------------------------------------------------
# Job Definition
# ---------------------------------------------------------------------------
JOB = {
    "namespace": JOB_NAMESPACE,
    "name": JOB_NAME,
    "facets": {
        "documentation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json"),
            "description": (
                "PySpark ETL pipeline that aggregates customer order history from PostgreSQL "
                "and S3 product catalog into a curated customer summary dataset. "
                "Applies window-based spend ranking per region and identifies top products per customer."
            ),
        },
        "sourceCodeLocation": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json"),
            "type": "git",
            "url": "https://github.com/ruchimishra-tech/watch-datapipeline",
            "repoUrl": "https://github.com/ruchimishra-tech/watch-datapipeline.git",
            "path": "spark_lineage_experiment/pyspark_job/customer_orders_pipeline.py",
            "version": "main",
        },
        "ownership": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json"),
            "owners": [
                {"name": "team:data-platform", "type": "TEAM"},
                {"name": "user:ruchi",          "type": "MAINTAINER"},
            ],
        },
        "sql": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json"),
            "query": (
                "SELECT c.customer_id, "
                "  concat_ws(' ', c.first_name, c.last_name) AS customer_name, "
                "  c.email, c.region, "
                "  COUNT(DISTINCT o.order_id) AS total_orders, "
                "  SUM(oi.quantity) AS total_items, "
                "  SUM(oi.quantity * oi.unit_price) AS total_spend, "
                "  AVG(o.order_total) AS avg_order_value, "
                "  MIN(o.order_date) AS first_order_date, "
                "  MAX(o.order_date) AS last_order_date, "
                "  RANK() OVER (PARTITION BY c.region ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS spend_rank_in_region "
                "FROM public.customers c "
                "  INNER JOIN public.orders o ON c.customer_id = o.customer_id "
                "  INNER JOIN public.order_items oi ON o.order_id = oi.order_id "
                "  LEFT JOIN product_catalog p ON oi.product_id = p.product_id "
                "WHERE c.status = 'ACTIVE' "
                "GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.region"
            ),
        },
    },
}


# ---------------------------------------------------------------------------
# Run Definition
# ---------------------------------------------------------------------------
RUN = {
    "runId": STATIC_RUN_ID,
    "facets": {
        "nominalTime": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json"),
            "nominalStartTime": NOMINAL_TIME,
            "nominalEndTime":   NOMINAL_TIME,
        },
        "processing_engine": {
            **facet_meta("https://openlineage.io/spec/facets/1-0-0/ProcessingEngineRunFacet.json"),
            "version": "3.5.0",
            "name": "Apache Spark",
            "openlineageAdapterVersion": "static-codegen-v1",
        },
    },
}


# ---------------------------------------------------------------------------
# Build Events
# ---------------------------------------------------------------------------
def build_start_event() -> dict:
    return {
        "eventType": "START",
        "eventTime": EVENT_TIME,
        "run": RUN,
        "job": JOB,
        "inputs": [DS_CUSTOMERS, DS_ORDERS, DS_ORDER_ITEMS, DS_PRODUCT_CATALOG],
        "outputs": [],
        "producer": PRODUCER,
        "schemaURL": OPENLINEAGE_SPEC,
    }


def build_complete_event() -> dict:
    return {
        "eventType": "COMPLETE",
        "eventTime": EVENT_TIME,
        "run": RUN,
        "job": JOB,
        "inputs": [DS_CUSTOMERS, DS_ORDERS, DS_ORDER_ITEMS, DS_PRODUCT_CATALOG],
        "outputs": [DS_CUSTOMER_ORDER_SUMMARY, DS_CUSTOMER_METRICS],
        "producer": PRODUCER,
        "schemaURL": OPENLINEAGE_SPEC,
    }


# ---------------------------------------------------------------------------
# Write NDJSON
# ---------------------------------------------------------------------------
def generate():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    events = [build_start_event(), build_complete_event()]

    with open(OUTPUT_PATH, "w") as f:
        for event in events:
            f.write(json.dumps(event, default=str) + "\n")

    print(f"[OK] Lineage file written: {OUTPUT_PATH}")
    print(f"     Events  : {len(events)}")
    print(f"     Run ID  : {STATIC_RUN_ID}")
    print(f"     Job     : {JOB_NAMESPACE}/{JOB_NAME}")
    print(f"     Inputs  : {len(events[1]['inputs'])} datasets")
    print(f"     Outputs : {len(events[1]['outputs'])} datasets")
    print()
    print("To import into Marquez:")
    print("  while IFS= read -r line; do")
    print(f'    curl -s -X POST http://localhost:5000/api/v1/lineage \\')
    print(f'      -H "Content-Type: application/json" \\')
    print(f'      -d "$line"; echo; done < {OUTPUT_PATH}')


if __name__ == "__main__":
    generate()
