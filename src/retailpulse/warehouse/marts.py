"""Warehouse data marts and analytical reporting interface for RetailPulse.

Provides access to analytical mart views:
- dw.mart_daily_revenue: Sales and revenue trends over time
- dw.mart_top_products: Product performance, revenue ranking, and category breakdown
- dw.mart_low_stock: Real-time inventory depletion and low-stock alerts
- dw.mart_dq_summary: Data quality metrics and validation scorecard
- dw.mart_channel_performance: Store and channel breakdown (online vs in-store)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.db import get_connection, get_db_connection
from retailpulse.common.logging import get_logger

logger = get_logger("warehouse.marts")

# Path to SQL mart definitions
SQL_MARTS_DIR = Path("sql/marts")


def refresh_marts(conn: PgConnection, marts_dir: Optional[Union[str, Path]] = None) -> List[str]:
    """Execute SQL mart definitions to create or update all reporting views.

    Idempotent: Uses CREATE OR REPLACE VIEW.

    Args:
        conn: Open PostgreSQL database connection to warehouse_db.
        marts_dir: Directory containing mart SQL scripts (defaults to sql/marts).

    Returns:
        List[str]: Names of executed SQL mart files.
    """
    directory = Path(marts_dir) if marts_dir else SQL_MARTS_DIR
    executed: List[str] = []

    sql_files = sorted(directory.glob("*.sql"))
    if not sql_files:
        logger.warning("No SQL files found in %s", directory.resolve())
        return executed

    current_file: Optional[Path] = None
    try:
        with conn.cursor() as cur:
            for sql_file in sql_files:
                current_file = sql_file
                logger.info("Executing mart DDL: %s", sql_file.name)
                sql_text = sql_file.read_text(encoding="utf-8")
                cur.execute(sql_text)
                executed.append(sql_file.name)
            conn.commit()
    except Exception as exc:
        conn.rollback()
        file_name = current_file.name if current_file else "unknown"
        logger.error("Failed to refresh marts on file %s: %s", file_name, exc)
        raise

    logger.info("Successfully refreshed %d mart view(s)", len(executed))
    return executed


def get_daily_revenue(
    conn: PgConnection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query daily revenue trends with optional date filtering.

    Args:
        conn: Open PostgreSQL connection.
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).

    Returns:
        List[Dict[str, Any]]: Daily revenue records.
    """
    if start_date and end_date and start_date > end_date:
        raise ValueError(f"start_date '{start_date}' cannot be after end_date '{end_date}'")

    clauses: List[str] = []
    params: List[Any] = []

    if start_date:
        clauses.append("full_date >= %s::DATE")
        params.append(start_date)
    if end_date:
        clauses.append("full_date <= %s::DATE")
        params.append(end_date)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
    SELECT
        date_key, full_date, year, month, month_name, day_name, is_weekend,
        total_revenue, total_orders, total_units_sold, avg_order_value
    FROM dw.mart_daily_revenue
    {where_sql}
    ORDER BY full_date ASC;
    """

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_top_products(
    conn: PgConnection,
    limit: int = 10,
    category: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query top-performing products ranked by revenue.

    Args:
        conn: Open PostgreSQL connection.
        limit: Maximum number of products to return (default: 10, must be > 0).
        category: Optional category filter.

    Returns:
        List[Dict[str, Any]]: Top product records with revenue rank.
    """
    if limit <= 0:
        raise ValueError(f"limit must be greater than 0, got {limit}")

    clauses: List[str] = []
    params: List[Any] = []

    if category:
        clauses.append("category = %s")
        params.append(category)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
    SELECT
        product_key, product_id, product_name, category, price,
        total_revenue, total_units_sold, total_orders, revenue_rank
    FROM dw.mart_top_products
    {where_sql}
    ORDER BY revenue_rank ASC
    LIMIT %s;
    """
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_low_stock_alerts(
    conn: PgConnection,
    store_id: Optional[int] = None,
    threshold: Optional[int] = None,
    snapshot_date: Optional[str] = None,
    latest_only: bool = False,
) -> List[Dict[str, Any]]:
    """Query low stock alerts and inventory health metrics.

    Args:
        conn: Open PostgreSQL connection.
        store_id: Optional physical store ID filter.
        threshold: Optional custom stock quantity upper bound (must be >= 0).
        snapshot_date: Optional specific snapshot date (YYYY-MM-DD).
        latest_only: If True, restricts to the most recent date_key in the mart.

    Returns:
        List[Dict[str, Any]]: Low stock inventory records.
    """
    if threshold is not None and threshold < 0:
        raise ValueError(f"threshold must be non-negative, got {threshold}")

    clauses: List[str] = []
    params: List[Any] = []

    if store_id is not None:
        clauses.append("store_id = %s")
        params.append(store_id)
    if threshold is not None:
        clauses.append("stock_qty <= %s")
        params.append(threshold)
    if snapshot_date:
        clauses.append("snapshot_date = %s::DATE")
        params.append(snapshot_date)
    if latest_only:
        clauses.append("date_key = (SELECT MAX(date_key) FROM dw.mart_low_stock)")

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
    SELECT
        inventory_key, date_key, snapshot_date, product_key, product_id, product_name,
        category, store_key, store_id, store_name, store_city, stock_qty,
        low_stock_threshold, stock_status
    FROM dw.mart_low_stock
    {where_sql}
    ORDER BY stock_qty ASC, product_name ASC;
    """

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_dq_summary(
    conn: PgConnection,
    source: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query the data quality scorecard aggregated from dq.summary.

    Args:
        conn: Open PostgreSQL connection.
        source: Optional source filter (e.g. 'sales', 'inventory', 'customers').

    Returns:
        List[Dict[str, Any]]: Data quality metrics by source.
    """
    where_sql = "WHERE source = %s" if source else ""
    params = (source,) if source else ()
    query = f"""
    SELECT
        source, total_batches, total_records, total_accepted, total_rejected,
        overall_reject_pct, latest_batch_time
    FROM dw.mart_dq_summary
    {where_sql}
    ORDER BY source ASC;
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_channel_performance(
    conn: PgConnection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    channel: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query store and channel performance (online vs in-store).

    Args:
        conn: Open PostgreSQL connection.
        start_date: Optional start date filter (YYYY-MM-DD).
        end_date: Optional end date filter (YYYY-MM-DD).
        channel: Optional channel filter ('in_store' or 'online').

    Returns:
        List[Dict[str, Any]]: Channel and store performance records with channel_share_pct.
    """
    if start_date and end_date and start_date > end_date:
        raise ValueError(f"start_date '{start_date}' cannot be after end_date '{end_date}'")
    if channel and channel not in ("in_store", "online"):
        raise ValueError(f"channel must be 'in_store' or 'online', got '{channel}'")

    clauses: List[str] = []
    params: List[Any] = []

    if start_date:
        clauses.append("full_date >= %s::DATE")
        params.append(start_date)
    if end_date:
        clauses.append("full_date <= %s::DATE")
        params.append(end_date)
    if channel:
        clauses.append("channel = %s")
        params.append(channel)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
    SELECT
        date_key, full_date, channel, store_id, store_name, city, state,
        total_revenue, total_orders, total_units_sold, avg_order_value, channel_share_pct
    FROM dw.mart_channel_performance
    {where_sql}
    ORDER BY full_date ASC, channel ASC, total_revenue DESC;
    """

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
