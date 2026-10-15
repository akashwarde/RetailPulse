"""PySpark Silver transformation pipeline for customer platform data."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    TimestampType,
)

if TYPE_CHECKING:
    from minio import Minio
    from psycopg2.extensions import connection as PgConnection

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.lake import (
    build_lake_path,
    get_minio_client,
    read_parquet,
    write_parquet,
)
from retailpulse.common.logging import get_logger
from retailpulse.validation import (
    CUSTOMERS_RULES,
    ValidationRule,
    calculate_quality_summary,
    quality_gate,
    validate,
    write_quality_summary,
    write_quarantine,
)

logger = get_logger("transform.silver_customers")

# Standard schema definition for Silver customer records
SILVER_CUSTOMERS_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "state",
    "signup_date",
    "_processed_at",
]


def transform_customers(
    df: DataFrame,
    dedupe_keys: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Apply type casting, string normalization, date standardization, and deduplication to customers.

    Steps applied:
    1. Cast customer_id to LongType.
    2. Normalize first_name, last_name, city, and state (trim whitespace, title-case).
    3. Normalize email (trim whitespace, lowercase).
    4. Standardize signup_date to DateType.
    5. Deduplicate on customer_id, keeping the latest record by signup_date.
    6. Append _processed_at audit timestamp.

    Args:
        df: Input DataFrame containing validated customer records.
        dedupe_keys: Business key columns for deduplication (defaults to ['customer_id']).

    Returns:
        DataFrame: Transformed, cleansed Silver customer records.
    """
    if df.rdd.isEmpty():
        logger.info("Input customer DataFrame is empty; returning empty Silver DataFrame.")
        empty_df = (
            df.withColumn("customer_id", F.lit(0).cast(LongType()))
            .withColumn("first_name", F.lit("").cast(StringType()))
            .withColumn("last_name", F.lit("").cast(StringType()))
            .withColumn("email", F.lit("").cast(StringType()))
            .withColumn("city", F.lit("").cast(StringType()))
            .withColumn("state", F.lit("").cast(StringType()))
            .withColumn("signup_date", F.current_date())
            .withColumn("_processed_at", F.current_timestamp())
            .filter("1=0")
        )
        return empty_df.select(SILVER_CUSTOMERS_COLUMNS)

    has_last_name = "last_name" in df.columns
    has_state = "state" in df.columns

    parsed_signup_date = F.coalesce(
        F.to_date(F.col("signup_date"), "yyyy-MM-dd"),
        F.to_date(F.col("signup_date"), "dd/MM/yyyy"),
        F.to_date(F.col("signup_date"), "MM/dd/yyyy"),
        F.to_date(F.col("signup_date")),
    )

    transformed = (
        df.withColumn("customer_id", F.col("customer_id").cast(LongType()))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn(
            "last_name",
            F.initcap(F.trim(F.col("last_name"))) if has_last_name else F.lit(None).cast(StringType()),
        )
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn(
            "state",
            F.initcap(F.trim(F.col("state"))) if has_state else F.lit(None).cast(StringType()),
        )
        .withColumn("signup_date", parsed_signup_date)
        .withColumn("_processed_at", F.current_timestamp())
    )

    # Deduplicate on customer_id, keeping the latest by signup_date
    keys = list(dedupe_keys) if dedupe_keys else ["customer_id"]
    window_spec = Window.partitionBy([F.col(k) for k in keys]).orderBy(
        F.col("signup_date").desc()
    )
    deduped = (
        transformed.withColumn("__rp_rn", F.row_number().over(window_spec))
        .filter(F.col("__rp_rn") == 1)
        .drop("__rp_rn")
    )

    return deduped.select(SILVER_CUSTOMERS_COLUMNS)


def run_silver_customers(
    run_date: str,
    spark: Optional[SparkSession] = None,
    config: Optional[AppConfig] = None,
    client: Optional[Minio] = None,
    conn: Optional[PgConnection] = None,
    rules: Optional[List[ValidationRule]] = None,
    batch_id: Optional[str] = None,
    source_df: Optional[DataFrame] = None,
) -> Dict[str, Any]:
    """Execute the end-to-end Bronze -> Silver transformation job for customer data.

    Workflow:
    1. Read Bronze customer Parquet for run_date partition.
    2. Validate input records using CUSTOMERS_RULES.
    3. Route invalid records to MinIO quarantine and PostgreSQL dq.quarantine.
    4. Calculate and persist data quality metrics to dq.summary.
    5. Evaluate the quality gate threshold (raises QualityGateError if reject_pct > 5%).
    6. Transform and deduplicate valid records to the Silver customer schema.
    7. Overwrite the Silver Parquet partition for the given run_date in MinIO.

    Args:
        run_date: Date string partition in YYYY-MM-DD format.
        spark: Optional active SparkSession.
        config: Optional AppConfig instance.
        client: Optional MinIO client.
        conn: Optional PostgreSQL connection.
        rules: Optional validation rules override (defaults to CUSTOMERS_RULES).
        batch_id: Optional batch identifier.
        source_df: Optional in-memory DataFrame (bypasses MinIO read for testing).

    Returns:
        Dict[str, Any]: Execution summary statistics.
    """
    logger.info("Starting Bronze->Silver customer transformation for date '%s'", run_date)
    cfg = config or get_config()
    minio_client = client or get_minio_client(cfg)
    active_batch_id = batch_id or f"customers_{run_date}_{int(time.time())}"
    validation_rules = rules or CUSTOMERS_RULES

    spark_session = spark or SparkSession.builder.appName("SilverCustomers").getOrCreate()

    if source_df is not None:
        bronze_df = source_df
    else:
        bronze_path = build_lake_path("bronze", "customers", dt=run_date, filename="data.parquet")
        logger.info("Reading Bronze customers from s3://%s/%s", cfg.minio.bucket, bronze_path)
        bronze_table = read_parquet(minio_client, cfg.minio.bucket, bronze_path)
        bronze_df = spark_session.createDataFrame(bronze_table.to_pandas())

    total_records = bronze_df.count()
    logger.info("Read %d records from Bronze customers partition '%s'", total_records, run_date)

    # Validation
    good_df, bad_df = validate(bronze_df, validation_rules)
    good_count = good_df.count()
    bad_count = bad_df.count()
    logger.info(
        "Validation completed: %d valid records, %d invalid records",
        good_count,
        bad_count,
    )

    # Quarantine invalid records
    if bad_count > 0:
        quarantine_count = write_quarantine(
            bad_df=bad_df,
            source="customers",
            batch_id=active_batch_id,
            dt=run_date,
            client=minio_client,
            conn=conn,
            config=cfg,
        )
        logger.info("Quarantined %d rejected customer records", quarantine_count)

    # Quality summary
    summary = calculate_quality_summary(
        bad_df=bad_df,
        total=total_records,
        source="customers",
        batch_id=active_batch_id,
    )
    write_quality_summary(
        source="customers",
        batch_id=active_batch_id,
        total=total_records,
        accepted=good_count,
        rejected=bad_count,
        per_rule_counts=summary.per_rule_counts,
        conn=conn,
        config=cfg,
    )

    # Quality Gate (default 5.0% threshold)
    quality_gate(summary, threshold=5.0)

    # Silver transformation
    silver_df = transform_customers(good_df)
    silver_count = silver_df.count()
    logger.info(
        "Transformed %d records into Silver customer schema (after deduplication)",
        silver_count,
    )

    # Write Silver Parquet
    silver_path = build_lake_path("silver", "customers", dt=run_date, filename="data.parquet")
    silver_pdf = silver_df.toPandas()
    silver_table = pa.Table.from_pandas(silver_pdf)
    write_parquet(minio_client, cfg.minio.bucket, silver_path, silver_table)
    logger.info(
        "Successfully wrote %d Silver customer records to s3://%s/%s",
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
    """Airflow callable entrypoint for Silver customer ETL job.

    Args:
        run_date: Execution partition date in 'YYYY-MM-DD' format.
    """
    logger.info("Invoking Silver customer ETL main for run_date=%s", run_date)
    result = run_silver_customers(run_date=run_date)
    logger.info("Silver customer ETL main finished successfully: %s", result)
