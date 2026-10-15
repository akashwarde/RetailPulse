"""Warehouse loads with date replacement for batch snapshots and event-id
replay protection for online increments. Rejections are persisted in the same
transaction as accepted facts; missing source partitions fail explicitly.
"""

from __future__ import annotations

import math
from decimal import Decimal
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set, Tuple, Union

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
from retailpulse.common.records import persist_rejections
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
        d = date.fromisoformat(val_str)
        return d, datetime.combine(d, datetime.min.time())
    except ValueError:
        return None, None


def _records(source: str, data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]],
             run_date: Optional[str], minio_client: Optional[Minio],
             bucket: Optional[str]) -> Tuple[List[Dict[str, Any]], str]:
    """Read an explicitly supplied snapshot; missing required objects fail."""
    if data is not None:
        return (data.to_pylist() if isinstance(data, pa.Table) else list(data)), "in_memory"
    if run_date is None or minio_client is None or bucket is None:
        raise ValueError("Provide data or run_date, minio_client and bucket")
    path = build_lake_path("silver", source, run_date)
    return read_parquet(minio_client, bucket, path).to_pylist(), path


def _integer(value: Any, name: str) -> int:
    number = Decimal(str(value))
    if not number.is_finite() or number != number.to_integral_value():
        raise ValueError(f"{name} must be an integer")
    return int(number)


def _lookup(mapping: Dict[int, int], value: Any, name: str) -> int:
    key = mapping.get(_integer(value, name))
    if key is None:
        raise ValueError(f"missing dimension: {name}={value}")
    return key


def _rejection(row: Dict[str, Any], reason: Exception) -> Dict[str, Any]:
    # JSONB rejects NaN/Inf. Keep malformed values as strings in raw payload.
    clean = {k: str(v) if isinstance(v, float) and not math.isfinite(v) else v for k, v in row.items()}
    return dict(rule_failed="warehouse:invalid_record", reason=str(reason), raw_record=clean)


def load_fact_sales(
    conn: PgConnection, data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]] = None,
    run_date: Optional[str] = None, minio_client: Optional[Minio] = None,
    bucket: Optional[str] = None, channel: Optional[str] = None, batch_id: Optional[str] = None,
) -> FactLoadResult:
    """Replace complete in-store dates; upsert online increments by event_id.

    Online rows must include event_id. They never delete any date partition.
    Rejections and accepted facts are written in the same database transaction.
    """
    records, source_path = _records("sales", data, run_date, minio_client, bucket)
    lookups = fetch_dimension_lookups(conn) if records else {}
    valid, rejected, physical_dates = [], [], set()
    seen = set()
    for row in records:
        try:
            ch = channel or row.get("channel") or ("online" if row.get("store_id") is None else "in_store")
            if ch not in ("in_store", "online"):
                raise ValueError("Invalid channel")
            event_id = row.get("event_id")
            if ch == "online" and (not isinstance(event_id, str) or not event_id.strip()):
                raise ValueError("Online events require event_id")
            event_id = event_id.strip() if ch == "online" else None
            order_id = _integer(row.get("order_id"), "order_id")
            item_id = _integer(row["order_item_id"], "order_item_id") if row.get("order_item_id") is not None else None
            day, timestamp = _parse_date_and_timestamp(row.get("order_timestamp") or row.get("event_time") or row.get("order_date") or run_date)
            if day is None:
                raise ValueError("Invalid sales timestamp")
            if ch == "in_store" and run_date and day.isoformat() != run_date:
                raise ValueError("In-store row is outside its snapshot date")
            product = _lookup(lookups["products"], row.get("product_id"), "product_id")
            customer = _lookup(lookups["customers"], row.get("customer_id"), "customer_id")
            store = _lookup(lookups["stores"], row.get("store_id"), "store_id") if ch == "in_store" else None
            qty = _integer(row.get("quantity"), "quantity")
            price = Decimal(str(row.get("unit_price")))
            if qty <= 0 or not price.is_finite() or price <= 0:
                raise ValueError("Quantity and price must be finite and positive")
            revenue = (price * qty).quantize(Decimal("0.01"))
            if row.get("total_amount") is not None:
                supplied = Decimal(str(row["total_amount"]))
                if not supplied.is_finite() or supplied.quantize(Decimal("0.01")) != revenue:
                    raise ValueError("Revenue does not match quantity * price")
            identity = (ch, event_id) if ch == "online" else (ch, order_id, item_id)
            if identity in seen:
                continue  # replayed event/line inside this input
            seen.add(identity)
            date_key = int(day.strftime("%Y%m%d"))
            ensure_date_key(conn, day)
            valid.append((order_id, item_id, date_key, product, customer, store,
                          qty, price, revenue, ch, timestamp, event_id))
            if ch == "in_store":
                physical_dates.add(date_key)
        except (ValueError, TypeError, ArithmeticError) as exc:
            rejected.append(_rejection(row, exc))
    only_online = bool(records) and all(channel == "online" or r.get("channel") == "online" or r.get("event_id") for r in records)
    if run_date and channel != "online" and not only_online:
        # A corrected empty full-day snapshot must remove old physical facts.
        physical_dates.add(int(date.fromisoformat(run_date).strftime("%Y%m%d")))
    identity = batch_id or f"warehouse_sales_{run_date or 'input'}"
    incremental = channel == "online" or any(r.get("channel") == "online" or r.get("event_id") for r in records)
    try:
        with conn.cursor() as cur:
            if physical_dates:
                cur.execute("DELETE FROM dw.fact_sales WHERE date_key = ANY(%s) AND channel = %s",
                            (sorted(physical_dates), "in_store"))
            if valid:
                execute_batch(cur, """
                    INSERT INTO dw.fact_sales (order_id, order_item_id, date_key, product_key,
                        customer_key, store_key, quantity, unit_price, revenue, channel,
                        order_timestamp, event_id, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                    ON CONFLICT (event_id) DO NOTHING
                """, valid, page_size=500)
        persist_rejections(conn, "warehouse_sales", identity, rejected, replace=not incremental)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return FactLoadResult("fact_sales", len(records), len(valid), len(rejected), len(rejected), source_path)


def load_fact_inventory(
    conn: PgConnection, data: Optional[Union[pa.Table, Sequence[Dict[str, Any]]]] = None,
    run_date: Optional[str] = None, minio_client: Optional[Minio] = None,
    bucket: Optional[str] = None, batch_id: Optional[str] = None,
) -> FactLoadResult:
    """Replace full inventory snapshots, including corrected empty snapshots."""
    records, source_path = _records("inventory", data, run_date, minio_client, bucket)
    lookups = fetch_dimension_lookups(conn) if records else {}
    valid, rejected, dates = [], [], set()
    seen = set()
    if run_date:
        dates.add(int(date.fromisoformat(run_date).strftime("%Y%m%d")))
    for row in records:
        try:
            day, _ = _parse_date_and_timestamp(row.get("snapshot_date") or run_date)
            if day is None or (run_date and day.isoformat() != run_date):
                raise ValueError("Invalid inventory snapshot date")
            qty = _integer(row.get("stock_qty"), "stock_qty")
            if qty < 0:
                raise ValueError("Stock cannot be negative")
            product = _lookup(lookups["products"], row.get("product_id"), "product_id")
            store = _lookup(lookups["stores"], row.get("store_id"), "store_id")
            key = int(day.strftime("%Y%m%d"))
            ensure_date_key(conn, day)
            if (key, product, store) not in seen:
                valid.append((key, product, store, qty))
                seen.add((key, product, store))
            dates.add(key)
        except (ValueError, TypeError, ArithmeticError) as exc:
            rejected.append(_rejection(row, exc))
    try:
        with conn.cursor() as cur:
            if dates:
                cur.execute("DELETE FROM dw.fact_inventory WHERE date_key = ANY(%s)", (sorted(dates),))
            if valid:
                execute_batch(cur, """
                    INSERT INTO dw.fact_inventory (date_key, product_key, store_key, stock_qty)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (date_key, product_key, store_key) DO UPDATE SET stock_qty=EXCLUDED.stock_qty
                """, valid, page_size=500)
        persist_rejections(conn, "warehouse_inventory", batch_id or f"warehouse_inventory_{run_date or 'input'}", rejected)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return FactLoadResult("fact_inventory", len(records), len(valid), len(rejected), len(rejected), source_path)


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
