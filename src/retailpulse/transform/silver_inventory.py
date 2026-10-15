"""PySpark Silver transformation pipeline for retail inventory data."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
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
    INVENTORY_RULES,
    ValidationRule,
    calculate_quality_summary,
    quality_gate,
    validate,
    write_quality_summary,
    write_quarantine,
)

logger = get_logger("transform.silver_inventory")

# Standard schema definition for Silver inventory records
SILVER_INVENTORY_COLUMNS = [
    "product_id",
    "store_id",
    "stock_qty",
    "snapshot_date",
    "_processed_at",
]


def transform_inventory(
    df: DataFrame,
    dedupe_keys: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Apply type casting, date normalization, and deduplication to inventory records.

    Steps applied:
    1. Cast product_id and store_id to LongType.
    2. Cast stock_qty to IntegerType.
    3. Standardize snapshot_date to DateType (handling ISO and common date formats).
    4. Deduplicate on composite business key (product_id, store_id, snapshot_date).
    5. Append _processed_at audit timestamp.

    Args:
        df: Input DataFrame containing validated inventory records.
        dedupe_keys: Business key columns for deduplication. Defaults to
                     ['product_id', 'store_id', 'snapshot_date'].

    Returns:
        DataFrame: Transformed, cleansed Silver inventory records.
    """
    df = prepare(df, "inventory")
    if df.isEmpty():
        logger.info("Input inventory DataFrame is empty; returning empty Silver DataFrame.")
        empty_df = (
            df.withColumn("product_id", F.lit(0).cast(LongType()))
            .withColumn("store_id", F.lit(0).cast(LongType()))
            .withColumn("stock_qty", F.lit(0).cast(IntegerType()))
            .withColumn("snapshot_date", F.current_date())
            .withColumn("_processed_at", F.current_timestamp())
            .filter("1=0")
        )
        return empty_df.select(SILVER_INVENTORY_COLUMNS)

    # Standardize snapshot_date across possible string/date formats
    parsed_date = F.coalesce(
        F.to_date(F.col("snapshot_date"), "yyyy-MM-dd"),
        F.to_date(F.col("snapshot_date"), "dd/MM/yyyy"),
        F.to_date(F.col("snapshot_date"), "MM/dd/yyyy"),
        F.to_date(F.col("snapshot_date")),
    )

    transformed = (
        df.withColumn("product_id", F.col("product_id").cast(LongType()))
        .withColumn("store_id", F.col("store_id").cast(LongType()))
        .withColumn("stock_qty", F.col("stock_qty").cast(IntegerType()))
        .withColumn("snapshot_date", parsed_date)
        .withColumn("_processed_at", F.current_timestamp())
    )

    # Deduplicate on business keys
    keys = list(dedupe_keys) if dedupe_keys else ["product_id", "store_id", "snapshot_date"]
    window_spec = Window.partitionBy([F.col(k) for k in keys]).orderBy(
        *([F.col("_ingested_at").desc_nulls_last()] if "_ingested_at" in transformed.columns else []),
        F.to_json(F.struct(*[F.col(c) for c in df.columns])).asc(),
    )
    deduped = (
        transformed.withColumn("__rp_rn", F.row_number().over(window_spec))
        .filter(F.col("__rp_rn") == 1)
        .drop("__rp_rn")
    )

    return deduped.select(SILVER_INVENTORY_COLUMNS)


def run_silver_inventory(
    run_date: str,
    spark: Optional[SparkSession] = None,
    config: Optional[AppConfig] = None,
    client: Optional[Minio] = None,
    conn: Optional[PgConnection] = None,
    rules: Optional[List[ValidationRule]] = None,
    batch_id: Optional[str] = None,
    source_df: Optional[DataFrame] = None,
) -> Dict[str, Any]:
    """Execute the end-to-end Bronze -> Silver transformation job for inventory data.

    Workflow:
    1. Read Bronze inventory Parquet for run_date partition.
    2. Validate input records using INVENTORY_RULES.
    3. Route invalid records to MinIO quarantine and PostgreSQL dq.quarantine.
    4. Calculate and persist data quality metrics to dq.summary.
    5. Evaluate the quality gate threshold (raises QualityGateError if reject_pct > 5%).
    6. Transform and deduplicate valid records to the Silver inventory schema.
    7. Overwrite the Silver Parquet partition for the given run_date in MinIO.

    Args:
        run_date: Date string partition in YYYY-MM-DD format.
        spark: Optional active SparkSession.
        config: Optional AppConfig instance.
        client: Optional MinIO client.
        conn: Optional PostgreSQL connection.
        rules: Optional validation rules override (defaults to INVENTORY_RULES).
        batch_id: Optional batch identifier.
        source_df: Optional in-memory DataFrame (bypasses MinIO read for testing).

    Returns:
        Dict[str, Any]: Execution summary statistics.
    """
    logger.info("Starting Bronze->Silver inventory transformation for date '%s'", run_date)
    cfg = config or get_config()
    minio_client = client or get_minio_client(cfg)
    active_batch_id = batch_id or f"inventory_{run_date}"
    validation_rules = rules if rules is not None else INVENTORY_RULES

    spark_session = spark or session("SilverInventory")

    if source_df is not None:
        bronze_df = source_df
    else:
        bronze_path = build_lake_path("bronze", "inventory", dt=run_date, filename="data.parquet")
        logger.info("Reading Bronze inventory from s3://%s/%s", cfg.minio.bucket, bronze_path)
        bronze_table = read_parquet(minio_client, cfg.minio.bucket, bronze_path)
        bronze_df = from_bronze(spark_session, bronze_table)

    total_records = bronze_df.count()
    logger.info("Read %d records from Bronze inventory partition '%s'", total_records, run_date)

    # Validation
    bronze_df = prepare(bronze_df, "inventory")
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

    # Quarantine invalid records
    quarantine_count = write_quarantine(
        bad_df=bad_df,
        source="inventory",
        batch_id=active_batch_id,
        dt=run_date,
        client=minio_client,
        conn=conn,
        config=cfg,
    )
    logger.info("Quarantined %d rejected inventory records", quarantine_count)

    # Quality summary
    summary = calculate_quality_summary(
        bad_df=bad_df,
        total=total_records,
        source="inventory",
        batch_id=active_batch_id,
    )
    write_quality_summary(
        source="inventory",
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
    silver_df = transform_inventory(good_df)
    silver_count = silver_df.count()
    logger.info(
        "Transformed %d records into Silver inventory schema (after deduplication)",
        silver_count,
    )

    # Write Silver Parquet
    silver_path = build_lake_path("silver", "inventory", dt=run_date, filename="data.parquet")
    silver_pdf = silver_df.toPandas()
    silver_table = pa.Table.from_pandas(silver_pdf)
    write_parquet(minio_client, cfg.minio.bucket, silver_path, silver_table)
    logger.info(
        "Successfully wrote %d Silver inventory records to s3://%s/%s",
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
    """Airflow callable entrypoint for Silver inventory ETL job.

    Args:
        run_date: Execution partition date in 'YYYY-MM-DD' format.
    """
    logger.info("Invoking Silver inventory ETL main for run_date=%s", run_date)
    result = run_silver_inventory(run_date=run_date)
    logger.info("Silver inventory ETL main finished successfully: %s", result)
