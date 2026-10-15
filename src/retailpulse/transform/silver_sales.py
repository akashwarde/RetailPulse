"""PySpark Silver transformation pipeline for retail sales data."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

if TYPE_CHECKING:
    from minio import Minio
    from psycopg2.extensions import connection as PgConnection

from retailpulse.transform.preparation import prepare, session, from_bronze
from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.lake import (
    build_lake_path,
    get_minio_client,
    read_parquet,
    write_parquet,
)
from retailpulse.common.logging import get_logger
from retailpulse.validation import (
    SALES_RULES,
    ValidationRule,
    calculate_quality_summary,
    get_quality_threshold,
    quality_gate,
    validate,
    write_quality_summary,
    write_quarantine,
)

logger = get_logger("transform.silver_sales")

# Standard schema definition for Silver sales records
SILVER_SALES_COLUMNS = [
    "order_id",
    "order_item_id",
    "customer_id",
    "store_id",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_timestamp",
    "order_date",
    "order_status",
    "payment_method",
    "_processed_at",
]


def transform_sales(
    df: DataFrame,
    dedupe_keys: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Apply standardization, type casting, string normalization, and deduplication to sales records.

    Steps applied:
    1. Cast IDs to LongType and quantity to IntegerType.
    2. Standardize unit_price to DoubleType (rounded to 2 decimal places).
    3. Compute derived total_amount = round(quantity * unit_price, 2).
    4. Cast order_timestamp to TimestampType and extract order_date (DateType).
    5. Strip whitespace and uppercase order_status and payment_method.
    6. Deduplicate on business keys, preserving the most recent record by order_timestamp.
    7. Append _processed_at audit timestamp.

    Args:
        df: Input DataFrame containing validated sales records.
        dedupe_keys: Business key columns for deduplication. Defaults to
                     ['order_id', 'order_item_id'] if present, else ['order_id', 'product_id'].

    Returns:
        DataFrame: Transformed, cleansed Silver sales records.
    """
    if df.isEmpty():
        logger.info("Input sales DataFrame is empty; returning empty Silver DataFrame.")
        # Return empty DataFrame with correct silver schema
        empty_df = (
            df.withColumn("order_id", F.lit(0).cast(LongType()))
            .withColumn("order_item_id", F.lit(0).cast(LongType()))
            .withColumn("customer_id", F.lit(0).cast(LongType()))
            .withColumn("store_id", F.lit(0).cast(LongType()))
            .withColumn("product_id", F.lit(0).cast(LongType()))
            .withColumn("quantity", F.lit(0).cast(IntegerType()))
            .withColumn("unit_price", F.lit(0.0).cast(DoubleType()))
            .withColumn("total_amount", F.lit(0.0).cast(DoubleType()))
            .withColumn("order_timestamp", F.current_timestamp())
            .withColumn("order_date", F.current_date())
            .withColumn("order_status", F.lit("").cast(StringType()))
            .withColumn("payment_method", F.lit("").cast(StringType()))
            .withColumn("_processed_at", F.current_timestamp())
            .filter("1=0")
        )
        return empty_df.select(SILVER_SALES_COLUMNS)

    # 1. Type casting and column transformations
    transformed = (
        df.withColumn("order_id", F.col("order_id").cast(LongType()))
        .withColumn(
            "order_item_id",
            F.col("order_item_id").cast(LongType())
            if "order_item_id" in df.columns
            else F.lit(None).cast(LongType()),
        )
        .withColumn("customer_id", F.col("customer_id").cast(LongType()))
        .withColumn("store_id", F.col("store_id").cast(LongType()))
        .withColumn("product_id", F.col("product_id").cast(LongType()))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn(
            "unit_price",
            F.round(F.col("unit_price").cast(DoubleType()), 2),
        )
        .withColumn(
            "total_amount",
            F.round(
                F.col("quantity").cast(DoubleType())
                * F.col("unit_price").cast(DoubleType()),
                2,
            ),
        )
        .withColumn(
            "order_timestamp",
            F.to_timestamp(F.col("order_timestamp")),
        )
        .withColumn("order_date", F.to_date(F.to_timestamp(F.col("order_timestamp"))))
        .withColumn(
            "order_status",
            F.upper(F.trim(F.col("order_status"))),
        )
        .withColumn(
            "payment_method",
            F.upper(F.trim(F.col("payment_method"))),
        )
        .withColumn("_processed_at", F.current_timestamp())
    )

    # 2. Determine deduplication key
    if dedupe_keys:
        keys = list(dedupe_keys)
    elif "order_item_id" in df.columns:
        keys = ["order_id", "order_item_id"]
    else:
        keys = ["order_id", "product_id"]

    # 3. Deduplicate on business keys, keeping latest record by order_timestamp
    window_spec = Window.partitionBy([F.col(k) for k in keys]).orderBy(
        F.col("order_timestamp").desc()
    )
    deduped = (
        transformed.withColumn("__rp_rn", F.row_number().over(window_spec))
        .filter(F.col("__rp_rn") == 1)
        .drop("__rp_rn")
    )

    return deduped.select(SILVER_SALES_COLUMNS)


def run_silver_sales(
    run_date: str,
    spark: Optional[SparkSession] = None,
    config: Optional[AppConfig] = None,
    client: Optional[Minio] = None,
    conn: Optional[PgConnection] = None,
    rules: Optional[List[ValidationRule]] = None,
    batch_id: Optional[str] = None,
    source_df: Optional[DataFrame] = None,
) -> Dict[str, Any]:
    """Execute the end-to-end Bronze -> Silver transformation job for sales data.

    Workflow:
    1. Read Bronze sales Parquet for the specified run_date partition.
    2. Validate input records using SALES_RULES.
    3. Route invalid records to MinIO quarantine and PostgreSQL dq.quarantine.
    4. Calculate and persist data quality metrics to dq.summary.
    5. Evaluate the quality gate threshold (raises QualityGateError if reject_pct > 5%).
    6. Transform and deduplicate valid records to the Silver sales schema.
    7. Overwrite the Silver Parquet partition for the given run_date in MinIO.

    Args:
        run_date: Date string partition in YYYY-MM-DD format.
        spark: Optional active SparkSession.
        config: Optional AppConfig instance.
        client: Optional MinIO client.
        conn: Optional PostgreSQL connection.
        rules: Optional validation rules override (defaults to SALES_RULES).
        batch_id: Optional batch identifier.
        source_df: Optional in-memory DataFrame (bypasses MinIO read for testing).

    Returns:
        Dict[str, Any]: Execution summary statistics.
    """
    logger.info("Starting Bronze->Silver sales transformation for date '%s'", run_date)
    cfg = config or get_config()
    minio_client = client or get_minio_client(cfg)
    active_batch_id = batch_id or f"sales_{run_date}"
    validation_rules = rules if rules is not None else SALES_RULES

    # 1. Initialize SparkSession
    spark_session = spark or session("SilverSales")

    # 2. Read Bronze partition
    if source_df is not None:
        bronze_df = source_df
    else:
        bronze_path = build_lake_path("bronze", "sales", dt=run_date, filename="data.parquet")
        logger.info("Reading Bronze sales from s3://%s/%s", cfg.minio.bucket, bronze_path)
        bronze_table = read_parquet(minio_client, cfg.minio.bucket, bronze_path)
        bronze_df = from_bronze(spark_session, bronze_table)

    total_records = bronze_df.count()
    logger.info("Read %d records from Bronze sales partition '%s'", total_records, run_date)

    # 3. Call validation engine
    bronze_df = prepare(bronze_df, "sales")
    if source_df is None:
        masters = read_parquet(minio_client, cfg.minio.bucket, build_lake_path("silver", "products", run_date))
        validation_rules = [*validation_rules, ValidationRule.referential("product_id",
            ref_values=[str(r["product_id"]) for r in masters.to_pylist()])]
    good_df, bad_df = validate(bronze_df, validation_rules)
    good_count = good_df.count()
    bad_count = bad_df.count()
    logger.info(
        "Validation completed: %d valid records, %d invalid records",
        good_count,
        bad_count,
    )

    # 4. Quarantine invalid records
    quarantine_count = write_quarantine(
        bad_df=bad_df,
        source="sales",
        batch_id=active_batch_id,
        dt=run_date,
        client=minio_client,
        conn=conn,
        config=cfg,
    )
    logger.info("Quarantined %d rejected sales records", quarantine_count)

    # 5. Calculate and persist Data Quality summary
    summary = calculate_quality_summary(
        bad_df=bad_df,
        total=total_records,
        source="sales",
        batch_id=active_batch_id,
    )
    write_quality_summary(
        source="sales",
        batch_id=active_batch_id,
        total=total_records,
        accepted=good_count,
        rejected=bad_count,
        per_rule_counts=summary.per_rule_counts,
        conn=conn,
        config=cfg,
    )

    # 6. Evaluate Quality Gate (default 5.0% threshold)
    quality_gate(summary, threshold=get_quality_threshold())

    # 7. Apply Silver transformations
    silver_df = transform_sales(good_df)
    silver_count = silver_df.count()
    logger.info(
        "Transformed %d records into Silver sales schema (after deduplication)",
        silver_count,
    )

    # 8. Write to Silver lake partition (overwrite partition date)
    silver_path = build_lake_path("silver", "sales", dt=run_date, filename="data.parquet")
    silver_pdf = silver_df.toPandas()
    silver_table = pa.Table.from_pandas(silver_pdf)
    write_parquet(minio_client, cfg.minio.bucket, silver_path, silver_table)
    logger.info(
        "Successfully wrote %d Silver sales records to s3://%s/%s",
        silver_count,
        cfg.minio.bucket,
        silver_path,
    )

    return {
        "run_date": run_date,
        "batch_id": active_batch_id,
        "total_records": total_records,
        "accepted_records": good_count,
        "rejected_records": bad_count,
        "silver_records": silver_count,
        "status": "SUCCESS",
    }


def main(run_date: str) -> None:
    """Airflow callable entrypoint for Silver sales ETL job.

    Args:
        run_date: Execution partition date in 'YYYY-MM-DD' format.
    """
    logger.info("Invoking Silver sales ETL main for run_date=%s", run_date)
    result = run_silver_sales(run_date=run_date)
    logger.info("Silver sales ETL main finished successfully: %s", result)
