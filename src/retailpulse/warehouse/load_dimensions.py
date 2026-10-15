"""Warehouse dimension loading pipeline for RetailPulse.

Loads Silver datasets from MinIO into the PostgreSQL star schema dimension tables
(dw.dim_date, dw.dim_product, dw.dim_customer, dw.dim_store).

Architecture & Design Choices:
------------------------------
1. Slowly Changing Dimensions (SCD):
   - Type-1 SCD (Upsert in place):
     Implemented via PostgreSQL `INSERT INTO ... ON CONFLICT (business_key) DO UPDATE SET ...`.
     Keeps the current state of dimensions without historical tracking, preserving
     surrogate keys and simplifying dimensional joins.
   - What Type-2 SCD would add:
     Type-2 SCD would add `valid_from TIMESTAMP`, `valid_to TIMESTAMP`, and
     `is_current BOOLEAN` columns. On update, the prior row is closed
     (`is_current = FALSE, valid_to = CURRENT_TIMESTAMP`) and a new record is inserted
     with a new surrogate key. This enables point-in-time historical reporting
     (e.g., product price or customer segment at the exact moment of purchase).

2. Idempotency:
   - All dimension loads use `ON CONFLICT (business_key) DO UPDATE` or
     `ON CONFLICT (date_key) DO NOTHING`.
   - Running the load multiple times for the same date or overlapping batches
     will never duplicate records or change surrogate primary keys.

3. Schema & Column Mappings:
   - dim_product: Maps Silver `product_name` -> `dw.dim_product.name`.
   - dim_customer: Maps `customer_id`, `first_name`, `last_name`, `email`,
     `city`, `state`, and optional `segment` (default: 'Standard').
   - dim_store: Maps `store_id`, `store_name`, `city`, `state`, and `store_type`
     (default: 'Physical').
   - dim_date: Consistent with DDL (pre-populates 2-year range 2024-01-01 to 2025-12-31).
     Provides dynamic date extension helper for dates outside this range.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

import pyarrow as pa
import psycopg2
from psycopg2.extras import execute_batch

if TYPE_CHECKING:
    from minio import Minio
    from psycopg2.extensions import connection as PgConnection

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.db import get_connection, get_db_connection
from retailpulse.common.lake import (
    build_lake_path,
    get_minio_client,
    read_parquet,
)
from retailpulse.common.logging import get_logger

logger = get_logger("warehouse.load_dimensions")


@dataclass(frozen=True)
class DimensionLoadResult:
    """Summary metrics of a dimension loading execution."""

    dimension: str
    total_records: int
    loaded: int
    skipped: int
    source_path: Optional[str] = None


def load_dim_date(
    conn: PgConnection,
    start_date: str = "2024-01-01",
    end_date: str = "2025-12-31",
) -> DimensionLoadResult:
    """Ensure the date dimension is populated for the specified range idempotently.

    Uses PostgreSQL generate_series with ON CONFLICT (date_key) DO NOTHING,
    consistent with sql/ddl/03_dw_schema.sql.

    Args:
        conn: Open psycopg2 database connection.
        start_date: Start date string (YYYY-MM-DD), default '2024-01-01'.
        end_date: End date string (YYYY-MM-DD), default '2025-12-31'.

    Returns:
        DimensionLoadResult: Counts of dates populated.
    """
    logger.info("Loading dim_date from %s to %s", start_date, end_date)
    query = """
    INSERT INTO dw.dim_date (
        date_key, full_date, day, month, month_name, quarter, year, day_name, is_weekend
    )
    SELECT
        TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
        d::DATE AS full_date,
        EXTRACT(DAY FROM d)::INT AS day,
        EXTRACT(MONTH FROM d)::INT AS month,
        TRIM(TO_CHAR(d, 'Month')) AS month_name,
        EXTRACT(QUARTER FROM d)::INT AS quarter,
        EXTRACT(YEAR FROM d)::INT AS year,
        TRIM(TO_CHAR(d, 'Day')) AS day_name,
        (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
    FROM generate_series(%s::DATE, %s::DATE, '1 day'::INTERVAL) AS d
    ON CONFLICT (date_key) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date))
        rows_inserted = cur.rowcount
        conn.commit()

    logger.info("dim_date load complete: %d new dates inserted", rows_inserted)
    return DimensionLoadResult(
        dimension="dim_date",
        total_records=rows_inserted,
        loaded=rows_inserted,
        skipped=0,
        source_path="generate_series",
    )


def ensure_date_key(conn: PgConnection, target_date: Union[str, date, datetime]) -> int:
    """Ensure a specific date exists in dw.dim_date, inserting it if absent.

    Handles dates that may occur beyond the initial 2-year range.

    Args:
        conn: Open psycopg2 database connection.
        target_date: Date to verify/insert.

    Returns:
        int: The integer date_key (YYYYMMDD).
    """
    if isinstance(target_date, str):
        parsed = datetime.strptime(target_date[:10], "%Y-%m-%d").date()
    elif isinstance(target_date, datetime):
        parsed = target_date.date()
    else:
        parsed = target_date

    date_str = parsed.isoformat()
    date_key = int(parsed.strftime("%Y%m%d"))

    query = """
    INSERT INTO dw.dim_date (
        date_key, full_date, day, month, month_name, quarter, year, day_name, is_weekend
    )
    VALUES (
        %s, %s::DATE,
        EXTRACT(DAY FROM %s::DATE)::INT,
        EXTRACT(MONTH FROM %s::DATE)::INT,
        TRIM(TO_CHAR(%s::DATE, 'Month')),
        EXTRACT(QUARTER FROM %s::DATE)::INT,
        EXTRACT(YEAR FROM %s::DATE)::INT,
        TRIM(TO_CHAR(%s::DATE, 'Day')),
        (EXTRACT(ISODOW FROM %s::DATE) IN (6, 7))
    )
    ON CONFLICT (date_key) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                date_key,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
                date_str,
            ),
        )
        conn.commit()

    return date_key


def load_dim_product(
    conn: PgConnection,
    data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]] = None,
    run_date: Optional[str] = None,
    minio_client: Optional[Minio] = None,
    bucket: Optional[str] = None,
) -> DimensionLoadResult:
    """Upsert products into dw.dim_product (Type-1 SCD).

    Maps Silver column `product_name` to warehouse column `dw.dim_product.name`.
    Preserves surrogate `product_key` on update.

    Args:
        conn: Open psycopg2 connection.
        data: Optional in-memory PyArrow Table or list of dicts.
        run_date: Date partition to read from MinIO if data is not provided.
        minio_client: Optional MinIO client for lake reads.
        bucket: Lake bucket name.

    Returns:
        DimensionLoadResult: Metrics on records read, loaded, and skipped.
    """
    source_path = None
    records: List[Dict[str, Any]] = []

    if data is not None:
        if isinstance(data, pa.Table):
            records = data.to_pylist()
        else:
            records = list(data)
        source_path = "in_memory"
    elif run_date is not None and minio_client is not None and bucket is not None:
        source_path = build_lake_path("silver", "products", run_date)
        try:
            table = read_parquet(minio_client, bucket, source_path)
            records = table.to_pylist()
        except Exception as exc:
            logger.warning(
                "Could not read Silver products from %s: %s", source_path, exc
            )
            return DimensionLoadResult(
                dimension="dim_product",
                total_records=0,
                loaded=0,
                skipped=0,
                source_path=source_path,
            )

    total_records = len(records)
    valid_params: List[tuple] = []
    skipped = 0

    for r in records:
        product_id = r.get("product_id")
        name = r.get("product_name") or r.get("name")
        category = r.get("category")
        price = r.get("unit_price") if r.get("unit_price") is not None else r.get("price")

        # Validate mandatory dimension fields
        if product_id is None or name is None or category is None or price is None:
            logger.warning(
                "Skipping invalid product record missing required fields: %s", r
            )
            skipped += 1
            continue

        try:
            pid = int(product_id)
            p_price = float(price)
            if p_price < 0:
                logger.warning("Skipping product with negative price: %s", r)
                skipped += 1
                continue
        except (ValueError, TypeError):
            logger.warning("Skipping product with malformed numeric fields: %s", r)
            skipped += 1
            continue

        valid_params.append((pid, str(name).strip(), str(category).strip(), p_price))

    if valid_params:
        upsert_query = """
        INSERT INTO dw.dim_product (product_id, name, category, price, created_at, updated_at)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (product_id) DO UPDATE SET
            name = EXCLUDED.name,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            updated_at = CURRENT_TIMESTAMP;
        """
        with conn.cursor() as cur:
            execute_batch(cur, upsert_query, valid_params, page_size=500)
            conn.commit()

    loaded = len(valid_params)
    logger.info(
        "dim_product load complete: %d loaded, %d skipped of %d total",
        loaded,
        skipped,
        total_records,
    )
    return DimensionLoadResult(
        dimension="dim_product",
        total_records=total_records,
        loaded=loaded,
        skipped=skipped,
        source_path=source_path,
    )


def load_dim_customer(
    conn: PgConnection,
    data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]] = None,
    run_date: Optional[str] = None,
    minio_client: Optional[Minio] = None,
    bucket: Optional[str] = None,
) -> DimensionLoadResult:
    """Upsert customers into dw.dim_customer (Type-1 SCD).

    Preserves surrogate `customer_key` on conflict.

    Args:
        conn: Open psycopg2 connection.
        data: Optional in-memory PyArrow Table or list of dicts.
        run_date: Date partition to read from MinIO if data is not provided.
        minio_client: Optional MinIO client for lake reads.
        bucket: Lake bucket name.

    Returns:
        DimensionLoadResult: Metrics on records read, loaded, and skipped.
    """
    source_path = None
    records: List[Dict[str, Any]] = []

    if data is not None:
        if isinstance(data, pa.Table):
            records = data.to_pylist()
        else:
            records = list(data)
        source_path = "in_memory"
    elif run_date is not None and minio_client is not None and bucket is not None:
        source_path = build_lake_path("silver", "customers", run_date)
        try:
            table = read_parquet(minio_client, bucket, source_path)
            records = table.to_pylist()
        except Exception as exc:
            logger.warning(
                "Could not read Silver customers from %s: %s", source_path, exc
            )
            return DimensionLoadResult(
                dimension="dim_customer",
                total_records=0,
                loaded=0,
                skipped=0,
                source_path=source_path,
            )

    total_records = len(records)
    valid_params: List[tuple] = []
    skipped = 0

    for r in records:
        customer_id = r.get("customer_id")
        if customer_id is None:
            logger.warning("Skipping customer record without customer_id: %s", r)
            skipped += 1
            continue

        try:
            cid = int(customer_id)
        except (ValueError, TypeError):
            logger.warning("Skipping customer with non-integer customer_id: %s", r)
            skipped += 1
            continue

        first_name = str(r.get("first_name") or "").strip() or None
        last_name = str(r.get("last_name") or "").strip() or None
        email = str(r.get("email") or "").strip() or None
        city = str(r.get("city") or "").strip() or None
        state = str(r.get("state") or "").strip() or None
        segment = str(r.get("segment") or "Standard").strip()

        valid_params.append((cid, first_name, last_name, email, city, state, segment))

    if valid_params:
        upsert_query = """
        INSERT INTO dw.dim_customer (
            customer_id, first_name, last_name, email, city, state, segment, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (customer_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            segment = EXCLUDED.segment,
            updated_at = CURRENT_TIMESTAMP;
        """
        with conn.cursor() as cur:
            execute_batch(cur, upsert_query, valid_params, page_size=500)
            conn.commit()

    loaded = len(valid_params)
    logger.info(
        "dim_customer load complete: %d loaded, %d skipped of %d total",
        loaded,
        skipped,
        total_records,
    )
    return DimensionLoadResult(
        dimension="dim_customer",
        total_records=total_records,
        loaded=loaded,
        skipped=skipped,
        source_path=source_path,
    )


def load_dim_store(
    conn: PgConnection,
    data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]] = None,
    run_date: Optional[str] = None,
    minio_client: Optional[Minio] = None,
    bucket: Optional[str] = None,
) -> DimensionLoadResult:
    """Upsert stores into dw.dim_store (Type-1 SCD).

    Preserves surrogate `store_key` on conflict.

    Args:
        conn: Open psycopg2 connection.
        data: Optional in-memory PyArrow Table or list of dicts.
        run_date: Date partition to read from MinIO if data is not provided.
        minio_client: Optional MinIO client for lake reads.
        bucket: Lake bucket name.

    Returns:
        DimensionLoadResult: Metrics on records read, loaded, and skipped.
    """
    source_path = None
    records: List[Dict[str, Any]] = []

    if data is not None:
        if isinstance(data, pa.Table):
            records = data.to_pylist()
        else:
            records = list(data)
        source_path = "in_memory"
    elif run_date is not None and minio_client is not None and bucket is not None:
        source_path = build_lake_path("silver", "stores", run_date)
        try:
            table = read_parquet(minio_client, bucket, source_path)
            records = table.to_pylist()
        except Exception as exc:
            logger.warning(
                "Could not read Silver stores from %s: %s", source_path, exc
            )
            return DimensionLoadResult(
                dimension="dim_store",
                total_records=0,
                loaded=0,
                skipped=0,
                source_path=source_path,
            )

    total_records = len(records)
    valid_params: List[tuple] = []
    skipped = 0

    for r in records:
        store_id = r.get("store_id")
        city = r.get("city")

        if store_id is None or city is None:
            logger.warning(
                "Skipping store record missing store_id or city: %s", r
            )
            skipped += 1
            continue

        try:
            sid = int(store_id)
        except (ValueError, TypeError):
            logger.warning("Skipping store with invalid store_id: %s", r)
            skipped += 1
            continue

        store_name = str(r.get("store_name") or f"Store {sid}").strip()
        s_city = str(city).strip()
        state = str(r.get("state") or "").strip() or None
        store_type = str(r.get("store_type") or r.get("type") or "Physical").strip()

        valid_params.append((sid, store_name, s_city, state, store_type))

    if valid_params:
        upsert_query = """
        INSERT INTO dw.dim_store (
            store_id, store_name, city, state, store_type, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (store_id) DO UPDATE SET
            store_name = EXCLUDED.store_name,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            store_type = EXCLUDED.store_type,
            updated_at = CURRENT_TIMESTAMP;
        """
        with conn.cursor() as cur:
            execute_batch(cur, upsert_query, valid_params, page_size=500)
            conn.commit()

    loaded = len(valid_params)
    logger.info(
        "dim_store load complete: %d loaded, %d skipped of %d total",
        loaded,
        skipped,
        total_records,
    )
    return DimensionLoadResult(
        dimension="dim_store",
        total_records=total_records,
        loaded=loaded,
        skipped=skipped,
        source_path=source_path,
    )


def load_dimensions(
    run_date: str,
    dimensions: Optional[Sequence[str]] = None,
    config: Optional[AppConfig] = None,
    minio_client: Optional[Minio] = None,
    conn: Optional[PgConnection] = None,
) -> Dict[str, DimensionLoadResult]:
    """Orchestrate loading of warehouse dimensions for a given run date.

    Loads dim_date, dim_product, dim_customer, and dim_store.
    Idempotent: Re-running for the same date does not duplicate records.

    Args:
        run_date: Partition date in YYYY-MM-DD format.
        dimensions: Optional list of dimensions to load (defaults to all four).
        config: Application configuration.
        minio_client: Optional MinIO client.
        conn: Optional active PostgreSQL connection to warehouse_db.

    Returns:
        Dict[str, DimensionLoadResult]: Results keyed by dimension table name.
    """
    if config is None:
        config = get_config()
    if minio_client is None:
        minio_client = get_minio_client(config)

    target_dims = set(dimensions) if dimensions else {"dim_date", "dim_product", "dim_customer", "dim_store"}
    results: Dict[str, DimensionLoadResult] = {}
    bucket = config.minio.bucket

    def _execute(connection: PgConnection) -> None:
        # 1. Date Dimension
        if "dim_date" in target_dims:
            # Ensure base range is loaded
            results["dim_date"] = load_dim_date(connection)
            # Ensure specific run_date is present (in case run_date is outside base range)
            ensure_date_key(connection, run_date)

        # 2. Product Dimension
        if "dim_product" in target_dims:
            results["dim_product"] = load_dim_product(
                conn=connection,
                run_date=run_date,
                minio_client=minio_client,
                bucket=bucket,
            )

        # 3. Customer Dimension
        if "dim_customer" in target_dims:
            results["dim_customer"] = load_dim_customer(
                conn=connection,
                run_date=run_date,
                minio_client=minio_client,
                bucket=bucket,
            )

        # 4. Store Dimension
        if "dim_store" in target_dims:
            results["dim_store"] = load_dim_store(
                conn=connection,
                run_date=run_date,
                minio_client=minio_client,
                bucket=bucket,
            )

    if conn is not None:
        _execute(conn)
    else:
        with get_db_connection(database="warehouse", config=config) as connection:
            _execute(connection)

    logger.info("load_dimensions finished for run_date=%s: %s", run_date, results)
    return results


def main(run_date: str) -> Dict[str, DimensionLoadResult]:
    """CLI/Airflow entrypoint for loading all dimensions for a logical run date.

    Args:
        run_date: Logical execution date (YYYY-MM-DD).

    Returns:
        Dict[str, DimensionLoadResult]: Summary results.
    """
    logger.info("Executing warehouse dimension load entrypoint for run_date=%s", run_date)
    return load_dimensions(run_date=run_date)
