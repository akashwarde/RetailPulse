"""Warehouse fact loading pipeline for RetailPulse.

Loads Silver sales and inventory records from MinIO into the PostgreSQL star schema
fact tables (dw.fact_sales, dw.fact_inventory).

Architecture & Design Choices:
------------------------------
1. Surrogate Key Resolution:
   - Look up surrogate keys from dimension tables:
     - product_key from dw.dim_product(product_id)
     - customer_key from dw.dim_customer(customer_id)
     - store_key from dw.dim_store(store_id) (nullable for online orders, mandatory for in-store)
     - date_key from dw.dim_date(full_date)
   - Rows failing dimension lookups are skipped, logged as missing dimensions,
     and counted in the load result.

2. Dynamic Date Dimension Extension:
   - For every distinct date present in incoming transactions or snapshots,
     ensure_date_key is invoked to guarantee that dates outside the baseline
     2024–2025 range exist in dw.dim_date prior to fact loading.

3. Idempotency (Delete-and-Reload):
   - fact_sales: Uses atomic delete-and-reload by date_key and channel so rerunning
     a batch replaces existing facts without duplication or cross-channel interference.
   - fact_inventory: Uses upsert matching
     CONSTRAINT uq_fact_inventory UNIQUE (date_key, product_key, store_key).

4. Robust Transaction Handling & Error Isolation:
   - All database modifications execute in transaction blocks.
   - On database error, conn.rollback() is explicitly executed before re-raising.
   - Missing lake partitions (NoSuchKey) are handled gracefully, while unexpected
     I/O or authentication errors are re-raised.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import pyarrow as pa
import psycopg2
from minio.error import S3Error
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
from retailpulse.warehouse.load_dimensions import ensure_date_key

logger = get_logger("warehouse.load_facts")


@dataclass(frozen=True)
class FactLoadResult:
    """Summary metrics of a fact table loading execution."""

    fact_table: str
    total_records: int
    loaded: int
    skipped: int
    quarantined: int
    source_path: Optional[str] = None


def fetch_dimension_lookups(conn: PgConnection) -> Dict[str, Dict[int, int]]:
    """Fetch dimension surrogate key lookup maps from PostgreSQL.

    Returns:
        Dict[str, Dict[int, int]]: Dictionaries for products, customers, and stores.
        - 'products': {product_id: product_key}
        - 'customers': {customer_id: customer_key}
        - 'stores': {store_id: store_key}
    """
    products: Dict[int, int] = {}
    customers: Dict[int, int] = {}
    stores: Dict[int, int] = {}

    with conn.cursor() as cur:
        cur.execute("SELECT product_id, product_key FROM dw.dim_product;")
        for pid, pkey in cur.fetchall():
            products[int(pid)] = int(pkey)

        cur.execute("SELECT customer_id, customer_key FROM dw.dim_customer;")
        for cid, ckey in cur.fetchall():
            customers[int(cid)] = int(ckey)

        cur.execute("SELECT store_id, store_key FROM dw.dim_store;")
        for sid, skey in cur.fetchall():
            stores[int(sid)] = int(skey)

    logger.debug(
        "Loaded dimension lookup caches: %d products, %d customers, %d stores",
        len(products),
        len(customers),
        len(stores),
    )
    return {
        "products": products,
        "customers": customers,
        "stores": stores,
    }


def _parse_date_and_timestamp(raw_val: Any) -> Tuple[Optional[date], Optional[datetime]]:
    """Extract normalized date and naive UTC timestamp from datetime representations."""
    if raw_val is None:
        return None, None

    if isinstance(raw_val, datetime):
        if raw_val.tzinfo is not None:
            raw_val = raw_val.astimezone(timezone.utc).replace(tzinfo=None)
        return raw_val.date(), raw_val

    if isinstance(raw_val, date):
        return raw_val, datetime.combine(raw_val, datetime.min.time())

    val_str = str(raw_val).strip()
    if not val_str:
        return None, None

    # Handle ISO formats like 2025-01-15 14:30:00, 2025-01-15T14:30:00, with or without tz
    try:
        dt = datetime.fromisoformat(val_str.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.date(), dt
    except ValueError:
        pass

    try:
        dt = datetime.strptime(val_str[:19], "%Y-%m-%d %H:%M:%S")
        return dt.date(), dt
    except ValueError:
        pass

    try:
        d = datetime.strptime(val_str[:10], "%Y-%m-%d").date()
        return d, datetime.combine(d, datetime.min.time())
    except ValueError:
        return None, None


def _is_missing_lake_object(exc: Exception) -> bool:
    """Check whether an exception indicates a missing object or empty partition."""
    if isinstance(exc, S3Error) and exc.code in ("NoSuchKey", "NoSuchBucket"):
        return True
    exc_msg = str(exc).lower()
    return "nosuchkey" in exc_msg or "not found" in exc_msg or "does not exist" in exc_msg


def load_fact_sales(
    conn: PgConnection,
    data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]] = None,
    run_date: Optional[str] = None,
    minio_client: Optional[Minio] = None,
    bucket: Optional[str] = None,
    channel: Optional[str] = None,
) -> FactLoadResult:
    """Load Silver sales records into dw.fact_sales.

    Idempotent: Uses atomic delete-and-reload by date_key and channel.
    Resolves surrogate keys for date, product, customer, and store.

    Args:
        conn: Open psycopg2 database connection.
        data: Optional in-memory PyArrow Table or list of dicts.
        run_date: Partition date (YYYY-MM-DD) to read from MinIO if data not provided.
        minio_client: Optional MinIO client for lake reads.
        bucket: Lake bucket name.
        channel: Optional channel override ('in_store' or 'online').

    Returns:
        FactLoadResult: Metrics on loaded, skipped, and quarantined records.
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
        source_path = build_lake_path("silver", "sales", run_date)
        try:
            table = read_parquet(minio_client, bucket, source_path)
            records = table.to_pylist()
        except Exception as exc:
            if _is_missing_lake_object(exc):
                logger.warning("Silver sales partition not found at %s: %s", source_path, exc)
                return FactLoadResult(
                    fact_table="fact_sales",
                    total_records=0,
                    loaded=0,
                    skipped=0,
                    quarantined=0,
                    source_path=source_path,
                )
            logger.error("Unexpected error reading Silver sales from %s: %s", source_path, exc)
            raise

    total_records = len(records)
    if total_records == 0:
        logger.info("No sales records to load.")
        return FactLoadResult(
            fact_table="fact_sales",
            total_records=0,
            loaded=0,
            skipped=0,
            quarantined=0,
            source_path=source_path,
        )

    # 1. Fetch dimension lookups
    lookups = fetch_dimension_lookups(conn)
    prod_map = lookups["products"]
    cust_map = lookups["customers"]
    store_map = lookups["stores"]

    # 2. Resolve distinct dates and ensure date dimension entries exist
    dates_to_ensure: Set[date] = set()
    for r in records:
        ts_val = r.get("order_timestamp") or r.get("order_date") or run_date
        d_val, _ = _parse_date_and_timestamp(ts_val)
        if d_val:
            dates_to_ensure.add(d_val)

    if run_date:
        run_d, _ = _parse_date_and_timestamp(run_date)
        if run_d:
            dates_to_ensure.add(run_d)

    for d in dates_to_ensure:
        ensure_date_key(conn, d)

    # 3. Process, validate, and resolve surrogate keys
    valid_params: List[tuple] = []
    affected_date_keys: Set[int] = set()
    skipped = 0

    for r in records:
        order_id = r.get("order_id")
        product_id = r.get("product_id")
        customer_id = r.get("customer_id")
        store_id = r.get("store_id")
        quantity = r.get("quantity")
        unit_price = r.get("unit_price")
        total_amount = r.get("total_amount")
        order_item_id = r.get("order_item_id")
        row_channel = channel or r.get("channel") or ("online" if store_id is None else "in_store")

        # Mandatory natural keys check
        if order_id is None or product_id is None or customer_id is None:
            logger.warning("Skipping sales record missing mandatory IDs: %s", r)
            skipped += 1
            continue

        raw_ts = r.get("order_timestamp") or r.get("order_date") or run_date
        order_d, order_ts = _parse_date_and_timestamp(raw_ts)
        if order_d is None or order_ts is None:
            logger.warning("Skipping sales record with invalid timestamp/date: %s", r)
            skipped += 1
            continue

        date_key = int(order_d.strftime("%Y%m%d"))

        # Look up surrogate keys
        try:
            pid = int(product_id)
            cid = int(customer_id)
            product_key = prod_map.get(pid)
            customer_key = cust_map.get(cid)
        except (ValueError, TypeError):
            logger.warning("Skipping sales record with non-integer natural keys: %s", r)
            skipped += 1
            continue

        if product_key is None:
            logger.warning("Product ID %d not found in dw.dim_product; skipping row: %s", pid, r)
            skipped += 1
            continue

        if customer_key is None:
            logger.warning("Customer ID %d not found in dw.dim_customer; skipping row: %s", cid, r)
            skipped += 1
            continue

        store_key = None
        if store_id is not None:
            try:
                sid = int(store_id)
                store_key = store_map.get(sid)
                if store_key is None and row_channel == "in_store":
                    logger.warning("Store ID %d not found in dw.dim_store for in-store sale; skipping: %s", sid, r)
                    skipped += 1
                    continue
            except (ValueError, TypeError):
                if row_channel == "in_store":
                    logger.warning("Invalid store_id %s for in-store sale; skipping: %s", store_id, r)
                    skipped += 1
                    continue
        elif row_channel == "in_store":
            # In-store sale must have a valid physical store
            logger.warning("In-store sales record missing store_id; skipping: %s", r)
            skipped += 1
            continue

        # Validate numeric measures (protecting against NaN / Inf / negative values)
        try:
            qty = int(quantity)
            u_price = float(unit_price) if unit_price is not None else 0.0
            if total_amount is not None:
                rev = round(float(total_amount), 2)
            else:
                rev = round(qty * u_price, 2)

            if (
                qty <= 0
                or u_price < 0
                or rev < 0
                or math.isnan(u_price)
                or math.isinf(u_price)
                or math.isnan(rev)
                or math.isinf(rev)
            ):
                logger.warning(
                    "Skipping sales record with invalid numeric measures (qty=%d, price=%s, rev=%s): %s",
                    qty,
                    u_price,
                    rev,
                    r,
                )
                skipped += 1
                continue
        except (ValueError, TypeError):
            logger.warning("Skipping sales record with non-numeric measures: %s", r)
            skipped += 1
            continue

        item_id = int(order_item_id) if order_item_id is not None else None
        valid_params.append((
            int(order_id),
            item_id,
            date_key,
            product_key,
            customer_key,
            store_key,
            qty,
            u_price,
            rev,
            row_channel,
            order_ts,
        ))
        affected_date_keys.add(date_key)

    # 4. Idempotent delete-and-reload in a single atomic transaction block
    if valid_params:
        try:
            with conn.cursor() as cur:
                # Delete existing sales for the affected date keys and distinct channels present in batch
                delete_query = """
                DELETE FROM dw.fact_sales
                WHERE date_key = ANY(%s) AND channel = %s;
                """
                distinct_channels = set(p[9] for p in valid_params)
                for ch in distinct_channels:
                    cur.execute(delete_query, (list(affected_date_keys), ch))
                    logger.debug(
                        "Deleted existing fact_sales for date_keys=%s, channel=%s (rows=%d)",
                        affected_date_keys,
                        ch,
                        cur.rowcount,
                    )

                insert_query = """
                INSERT INTO dw.fact_sales (
                    order_id, order_item_id, date_key, product_key, customer_key, store_key,
                    quantity, unit_price, revenue, channel, order_timestamp, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                """
                execute_batch(cur, insert_query, valid_params, page_size=500)
                conn.commit()
        except Exception as exc:
            conn.rollback()
            logger.error("Database error during fact_sales load, rolled back: %s", exc)
            raise

    loaded = len(valid_params)
    logger.info(
        "fact_sales load complete: %d loaded, %d skipped of %d total",
        loaded,
        skipped,
        total_records,
    )
    return FactLoadResult(
        fact_table="fact_sales",
        total_records=total_records,
        loaded=loaded,
        skipped=skipped,
        quarantined=skipped,
        source_path=source_path,
    )


def load_fact_inventory(
    conn: PgConnection,
    data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]] = None,
    run_date: Optional[str] = None,
    minio_client: Optional[Minio] = None,
    bucket: Optional[str] = None,
) -> FactLoadResult:
    """Load Silver inventory records into dw.fact_inventory.

    Idempotent: Uses upsert matching CONSTRAINT uq_fact_inventory UNIQUE (date_key, product_key, store_key).
    Resolves surrogate keys for date, product, and store.

    Args:
        conn: Open psycopg2 database connection.
        data: Optional in-memory PyArrow Table or list of dicts.
        run_date: Partition date (YYYY-MM-DD) to read from MinIO if data not provided.
        minio_client: Optional MinIO client for lake reads.
        bucket: Lake bucket name.

    Returns:
        FactLoadResult: Metrics on loaded, skipped, and quarantined records.
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
        source_path = build_lake_path("silver", "inventory", run_date)
        try:
            table = read_parquet(minio_client, bucket, source_path)
            records = table.to_pylist()
        except Exception as exc:
            if _is_missing_lake_object(exc):
                logger.warning("Silver inventory partition not found at %s: %s", source_path, exc)
                return FactLoadResult(
                    fact_table="fact_inventory",
                    total_records=0,
                    loaded=0,
                    skipped=0,
                    quarantined=0,
                    source_path=source_path,
                )
            logger.error("Unexpected error reading Silver inventory from %s: %s", source_path, exc)
            raise

    total_records = len(records)
    if total_records == 0:
        logger.info("No inventory records to load.")
        return FactLoadResult(
            fact_table="fact_inventory",
            total_records=0,
            loaded=0,
            skipped=0,
            quarantined=0,
            source_path=source_path,
        )

    # 1. Fetch dimension lookups
    lookups = fetch_dimension_lookups(conn)
    prod_map = lookups["products"]
    store_map = lookups["stores"]

    # 2. Resolve distinct dates and ensure date dimension entries exist
    dates_to_ensure: Set[date] = set()
    for r in records:
        d_raw = r.get("snapshot_date") or run_date
        d_val, _ = _parse_date_and_timestamp(d_raw)
        if d_val:
            dates_to_ensure.add(d_val)

    if run_date:
        run_d, _ = _parse_date_and_timestamp(run_date)
        if run_d:
            dates_to_ensure.add(run_d)

    for d in dates_to_ensure:
        ensure_date_key(conn, d)

    # 3. Process, validate, and resolve surrogate keys
    valid_params: List[tuple] = []
    skipped = 0

    for r in records:
        product_id = r.get("product_id")
        store_id = r.get("store_id")
        stock_qty = r.get("stock_qty")

        if product_id is None or store_id is None or stock_qty is None:
            logger.warning("Skipping inventory record missing mandatory fields: %s", r)
            skipped += 1
            continue

        d_raw = r.get("snapshot_date") or run_date
        snap_d, _ = _parse_date_and_timestamp(d_raw)
        if snap_d is None:
            logger.warning("Skipping inventory record with invalid snapshot_date: %s", r)
            skipped += 1
            continue

        date_key = int(snap_d.strftime("%Y%m%d"))

        try:
            pid = int(product_id)
            sid = int(store_id)
            qty = int(stock_qty)
        except (ValueError, TypeError):
            logger.warning("Skipping inventory record with non-integer fields: %s", r)
            skipped += 1
            continue

        if qty < 0:
            logger.warning("Skipping inventory record with negative stock_qty (%d): %s", qty, r)
            skipped += 1
            continue

        product_key = prod_map.get(pid)
        store_key = store_map.get(sid)

        if product_key is None:
            logger.warning("Product ID %d not found in dw.dim_product; skipping inventory: %s", pid, r)
            skipped += 1
            continue

        if store_key is None:
            logger.warning("Store ID %d not found in dw.dim_store; skipping inventory: %s", sid, r)
            skipped += 1
            continue

        valid_params.append((date_key, product_key, store_key, qty))

    # 4. Idempotent upsert matching CONSTRAINT uq_fact_inventory UNIQUE (date_key, product_key, store_key)
    if valid_params:
        try:
            with conn.cursor() as cur:
                upsert_query = """
                INSERT INTO dw.fact_inventory (
                    date_key, product_key, store_key, stock_qty, created_at
                )
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (date_key, product_key, store_key) DO UPDATE SET
                    stock_qty = EXCLUDED.stock_qty,
                    created_at = CURRENT_TIMESTAMP;
                """
                execute_batch(cur, upsert_query, valid_params, page_size=500)
                conn.commit()
        except Exception as exc:
            conn.rollback()
            logger.error("Database error during fact_inventory load, rolled back: %s", exc)
            raise

    loaded = len(valid_params)
    logger.info(
        "fact_inventory load complete: %d loaded, %d skipped of %d total",
        loaded,
        skipped,
        total_records,
    )
    return FactLoadResult(
        fact_table="fact_inventory",
        total_records=total_records,
        loaded=loaded,
        skipped=skipped,
        quarantined=skipped,
        source_path=source_path,
    )


def load_facts(
    run_date: str,
    facts: Optional[Sequence[str]] = None,
    config: Optional[AppConfig] = None,
    minio_client: Optional[Minio] = None,
    conn: Optional[PgConnection] = None,
) -> Dict[str, FactLoadResult]:
    """Orchestrate loading of warehouse fact tables for a given run date.

    Loads fact_sales and fact_inventory.
    Idempotent: Re-running for the same date deletes/updates existing partition facts without duplicates.

    Args:
        run_date: Partition date in YYYY-MM-DD format.
        facts: Optional list of facts to load (defaults to both 'fact_sales' and 'fact_inventory').
        config: Application configuration.
        minio_client: Optional MinIO client.
        conn: Optional active PostgreSQL connection to warehouse_db.

    Returns:
        Dict[str, FactLoadResult]: Results keyed by fact table name.
    """
    if config is None:
        config = get_config()
    if minio_client is None:
        minio_client = get_minio_client(config)

    target_facts = set(facts) if facts else {"fact_sales", "fact_inventory"}
    results: Dict[str, FactLoadResult] = {}
    bucket = config.minio.bucket

    def _execute(connection: PgConnection) -> None:
        # 1. Sales Fact Table
        if "fact_sales" in target_facts:
            results["fact_sales"] = load_fact_sales(
                conn=connection,
                run_date=run_date,
                minio_client=minio_client,
                bucket=bucket,
            )

        # 2. Inventory Fact Table
        if "fact_inventory" in target_facts:
            results["fact_inventory"] = load_fact_inventory(
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

    logger.info("load_facts finished for run_date=%s: %s", run_date, results)
    return results


def main(run_date: str) -> Dict[str, FactLoadResult]:
    """CLI/Airflow entrypoint for loading all fact tables for a logical run date.

    Args:
        run_date: Logical execution date (YYYY-MM-DD).

    Returns:
        Dict[str, FactLoadResult]: Summary results.
    """
    logger.info("Executing warehouse fact load entrypoint for run_date=%s", run_date)
    return load_facts(run_date=run_date)
