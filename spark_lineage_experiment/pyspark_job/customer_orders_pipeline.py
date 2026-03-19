"""
customer_orders_pipeline.py
----------------------------
PySpark ETL job: Customer Orders Summary Pipeline

Sources:
  - PostgreSQL (JDBC): retail.public.customers
  - PostgreSQL (JDBC): retail.public.orders
  - PostgreSQL (JDBC): retail.public.order_items
  - S3 Parquet:        s3://data-lake/raw/product_catalog/

Transformations:
  - Filter active customers
  - Join orders -> customers -> order_items -> product_catalog
  - Aggregate spend, order count, item count per customer
  - Identify top product per customer (window function)
  - Rank customers by total_spend within region (window function)

Outputs:
  - S3 Parquet:   s3://data-lake/curated/customer_order_summary/
  - PostgreSQL:   reporting.customer_metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DecimalType, DateType, TimestampType
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
JDBC_URL = "jdbc:postgresql://prod-db.internal:5432/retail"
JDBC_REPORTING_URL = "jdbc:postgresql://prod-db.internal:5432/reporting"
JDBC_PROPS = {
    "user": "etl_user",
    "password": "{{ secret:etl_db_password }}",
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000",
}

S3_PRODUCT_CATALOG = "s3://data-lake/raw/product_catalog/"
S3_OUTPUT_SUMMARY = "s3://data-lake/curated/customer_order_summary/"
REPORTING_TABLE = "customer_metrics"


# ---------------------------------------------------------------------------
# Schemas (explicit — used for static lineage derivation)
# ---------------------------------------------------------------------------
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",  IntegerType(),   False),
    StructField("first_name",   StringType(),    False),
    StructField("last_name",    StringType(),    False),
    StructField("email",        StringType(),    True),
    StructField("region",       StringType(),    True),
    StructField("status",       StringType(),    False),
    StructField("created_at",   TimestampType(), True),
    StructField("updated_at",   TimestampType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id",     IntegerType(),      False),
    StructField("customer_id",  IntegerType(),      False),
    StructField("order_date",   DateType(),         False),
    StructField("order_total",  DecimalType(10, 2), True),
    StructField("status",       StringType(),       False),
    StructField("created_at",   TimestampType(),    True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("item_id",      IntegerType(),      False),
    StructField("order_id",     IntegerType(),      False),
    StructField("product_id",   IntegerType(),      False),
    StructField("quantity",     IntegerType(),      False),
    StructField("unit_price",   DecimalType(10, 2), False),
])

PRODUCT_CATALOG_SCHEMA = StructType([
    StructField("product_id",   IntegerType(),      False),
    StructField("product_name", StringType(),       False),
    StructField("category",     StringType(),       True),
    StructField("brand",        StringType(),       True),
    StructField("unit_cost",    DecimalType(10, 2), True),
])


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def extract_customers(spark: SparkSession):
    return (
        spark.read
        .schema(CUSTOMERS_SCHEMA)
        .jdbc(JDBC_URL, "public.customers", properties=JDBC_PROPS)
    )


def extract_orders(spark: SparkSession):
    return (
        spark.read
        .schema(ORDERS_SCHEMA)
        .jdbc(JDBC_URL, "public.orders", properties=JDBC_PROPS)
    )


def extract_order_items(spark: SparkSession):
    return (
        spark.read
        .schema(ORDER_ITEMS_SCHEMA)
        .jdbc(JDBC_URL, "public.order_items", properties=JDBC_PROPS)
    )


def extract_product_catalog(spark: SparkSession):
    return (
        spark.read
        .schema(PRODUCT_CATALOG_SCHEMA)
        .parquet(S3_PRODUCT_CATALOG)
    )


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def transform(customers, orders, order_items, product_catalog):

    # 1. Filter active customers only
    active_customers = customers.filter(F.col("status") == "ACTIVE")

    # 2. Enrich order_items with product name
    items_with_product = order_items.join(
        product_catalog.select("product_id", "product_name", "category"),
        on="product_id",
        how="left"
    )

    # 3. Calculate line amount per item
    items_with_amount = items_with_product.withColumn(
        "line_amount", F.col("quantity") * F.col("unit_price")
    )

    # 4. Join orders -> order_items
    orders_enriched = orders.join(items_with_amount, on="order_id", how="inner")

    # 5. Identify top product per customer (most ordered by quantity)
    product_window = Window.partitionBy("customer_id", "product_id")
    product_qty = orders_enriched.withColumn(
        "product_total_qty", F.sum("quantity").over(product_window)
    )
    top_product_window = Window.partitionBy("customer_id").orderBy(
        F.col("product_total_qty").desc()
    )
    top_product = (
        product_qty
        .withColumn("product_rank", F.rank().over(top_product_window))
        .filter(F.col("product_rank") == 1)
        .select("customer_id", F.col("product_name").alias("top_product_name"))
        .dropDuplicates(["customer_id"])
    )

    # 6. Aggregate per customer
    customer_agg = (
        orders_enriched
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_items"),
            F.sum("line_amount").cast(DecimalType(12, 2)).alias("total_spend"),
            F.avg("order_total").cast(DecimalType(10, 2)).alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    # 7. Join aggregates back to active customers
    customer_summary = (
        active_customers
        .join(customer_agg, on="customer_id", how="inner")
        .join(top_product, on="customer_id", how="left")
        .select(
            "customer_id",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
            "email",
            "region",
            "total_orders",
            "total_items",
            "total_spend",
            "avg_order_value",
            "first_order_date",
            "last_order_date",
            "top_product_name",
        )
    )

    # 8. Rank customers by total_spend within region
    region_window = Window.partitionBy("region").orderBy(F.col("total_spend").desc())
    customer_ranked = customer_summary.withColumn(
        "spend_rank_in_region", F.rank().over(region_window)
    ).withColumn(
        "pipeline_run_date", F.current_date()
    )

    return customer_ranked


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load_to_s3(df):
    (
        df.write
        .mode("overwrite")
        .partitionBy("region")
        .parquet(S3_OUTPUT_SUMMARY)
    )


def load_to_reporting_db(df):
    reporting_df = df.select(
        "customer_id",
        "customer_name",
        "region",
        "total_spend",
        "spend_rank_in_region",
        "total_orders",
        "last_order_date",
        F.current_timestamp().alias("updated_at"),
    )
    (
        reporting_df.write
        .mode("overwrite")
        .jdbc(JDBC_REPORTING_URL, REPORTING_TABLE, properties=JDBC_PROPS)
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder
        .appName("customer_orders_pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    customers     = extract_customers(spark)
    orders        = extract_orders(spark)
    order_items   = extract_order_items(spark)
    product_catalog = extract_product_catalog(spark)

    summary = transform(customers, orders, order_items, product_catalog)

    summary.cache()
    load_to_s3(summary)
    load_to_reporting_db(summary)
    summary.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
