"""Integration tests for warehouse data marts executing directly against PostgreSQL.

Requires a reachable PostgreSQL instance (e.g. disposable test container).
If no PostgreSQL instance is reachable, these tests are skipped cleanly.
"""

import os
from typing import Generator
import pytest

try:
    import psycopg2
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False

from retailpulse.warehouse.marts import (
    get_channel_performance,
    get_daily_revenue,
    get_dq_summary,
    get_low_stock_alerts,
    get_top_products,
    refresh_marts,
)


def _get_pg_conn():
    """Attempt connection to test PostgreSQL instance."""
    host = os.getenv("TEST_POSTGRES_HOST", os.getenv("POSTGRES_HOST", "temp_pg_test"))
    port = int(os.getenv("TEST_POSTGRES_PORT", os.getenv("POSTGRES_PORT", "5432")))
    user = os.getenv("TEST_POSTGRES_USER", os.getenv("POSTGRES_USER", "postgres"))
    password = os.getenv("TEST_POSTGRES_PASSWORD", os.getenv("POSTGRES_PASSWORD", "secret"))
    dbname = os.getenv("TEST_POSTGRES_DB", os.getenv("POSTGRES_DB", "postgres"))

    errors = []
    # Try configured host first, then localhost as fallback
    for h in [host, "localhost", "127.0.0.1"]:
        try:
            conn = psycopg2.connect(
                host=h,
                port=port,
                user=user,
                password=password,
                dbname=dbname,
                connect_timeout=2,
            )
            return conn, None
        except Exception as exc:
            errors.append(f"{h}:{port} -> {exc}")
    return None, "; ".join(errors)


@pytest.fixture(scope="module")
def pg_conn() -> Generator:
    """Fixture providing a connected PostgreSQL connection or skipping if unavailable."""
    if not PG_AVAILABLE:
        pytest.skip("psycopg2 not installed")

    conn, err = _get_pg_conn()
    if conn is None:
        pytest.skip(f"PostgreSQL test instance not reachable: {err}")

    # Refresh marts
    refresh_marts(conn)

    yield conn
    conn.close()


class TestMartsPostgresIntegration:
    """Integration tests verifying analytical correctness of mart queries on real PostgreSQL."""

    def test_refresh_marts_idempotent(self, pg_conn) -> None:
        """Verify that refresh_marts can be rerun without conflict."""
        res1 = refresh_marts(pg_conn)
        res2 = refresh_marts(pg_conn)
        assert len(res1) == 5
        assert len(res2) == 5

    def test_daily_revenue_analytical_correctness(self, pg_conn) -> None:
        """Verify exact aggregation, distinct orders, and AOV on daily revenue."""
        results = get_daily_revenue(pg_conn, start_date="2025-01-15", end_date="2025-01-15")
        assert len(results) == 1
        row = results[0]
        # Order 1 ($200) + Order 2 ($50 + $30 = $80) + Order 3 ($100 + $50 = $150) = $430.00
        assert float(row["total_revenue"]) == 430.00
        # 3 distinct orders (even though Order 2 and Order 3 have multiple line items)
        assert row["total_orders"] == 3
        # 2 keyboards + 1 mouse + 2 cables + 1 keyboard + 1 mouse = 7 units
        assert row["total_units_sold"] == 7
        # AOV = 430.00 / 3 = 143.33
        assert float(row["avg_order_value"]) == 143.33

    def test_top_products_ranking_and_category_filter(self, pg_conn) -> None:
        """Verify revenue dense ranking, product metrics, and category filtering."""
        all_top = get_top_products(pg_conn, limit=10)
        assert len(all_top) >= 3

        # Product 101 ($300 total rev) should be rank 1
        p101 = next(p for p in all_top if p["product_id"] == 101)
        assert float(p101["total_revenue"]) == 300.00
        assert p101["revenue_rank"] == 1
        assert p101["total_units_sold"] == 3

        # Category filter: Accessories should return only product 103 ($30 rev)
        acc_top = get_top_products(pg_conn, category="Accessories", limit=5)
        assert len(acc_top) == 1
        assert acc_top[0]["product_id"] == 103
        assert acc_top[0]["category"] == "Accessories"
        assert float(acc_top[0]["total_revenue"]) == 30.00

    def test_low_stock_alerts_thresholds_and_severity(self, pg_conn) -> None:
        """Verify stock alerts categorize severity and exclude stock >= 10."""
        alerts = get_low_stock_alerts(pg_conn, store_id=301)
        assert len(alerts) == 3

        # Check severities
        status_map = {a["product_id"]: a["stock_status"] for a in alerts}
        assert status_map[101] == "OUT_OF_STOCK"  # qty = 0
        assert status_map[102] == "CRITICAL"      # qty = 4 (1-5)
        assert status_map[103] == "LOW"           # qty = 8 (6-9)

        # Product 101 at store 302 had stock 25 and must NOT appear
        store_302_alerts = get_low_stock_alerts(pg_conn, store_id=302)
        assert len(store_302_alerts) == 0

    def test_dq_summary_metrics(self, pg_conn) -> None:
        """Verify data quality scorecard aggregated values and zero division handling."""
        summary = get_dq_summary(pg_conn)
        assert len(summary) >= 2

        sales_summary = next(s for s in summary if s["source"] == "sales")
        assert sales_summary["total_batches"] == 2
        assert sales_summary["total_records"] == 300
        assert sales_summary["total_accepted"] == 285
        assert sales_summary["total_rejected"] == 15
        assert float(sales_summary["overall_reject_pct"]) == 5.00

        inv_summary = next(s for s in summary if s["source"] == "inventory")
        assert inv_summary["total_batches"] == 1
        assert inv_summary["total_records"] == 50
        assert inv_summary["total_accepted"] == 50
        assert inv_summary["total_rejected"] == 0
        assert float(inv_summary["overall_reject_pct"]) == 0.00

    def test_channel_performance_and_share(self, pg_conn) -> None:
        """Verify channel metrics, nullable store_key for online, and channel_share_pct."""
        rows = get_channel_performance(pg_conn, start_date="2025-01-15", end_date="2025-01-15")
        assert len(rows) == 3

        online_row = next(r for r in rows if r["channel"] == "online")
        assert online_row["store_id"] == 0
        assert online_row["store_name"] == "Online Channel"
        assert float(online_row["total_revenue"]) == 150.00
        # Online share: 150 / 430 * 100 = 34.88%
        assert float(online_row["channel_share_pct"]) == 34.88

        store_301 = next(r for r in rows if r["store_id"] == 301)
        assert float(store_301["total_revenue"]) == 200.00
        # Store 301 share: 200 / 430 * 100 = 46.51%
        assert float(store_301["channel_share_pct"]) == 46.51

        store_302 = next(r for r in rows if r["store_id"] == 302)
        assert float(store_302["total_revenue"]) == 80.00
        # Store 302 share: 80 / 430 * 100 = 18.60%
        assert float(store_302["channel_share_pct"]) == 18.60

        # Total share sums to ~100%
        total_share = sum(float(r["channel_share_pct"]) for r in rows)
        assert round(total_share, 1) == 100.0

    def test_empty_results_gracefully_handled(self, pg_conn) -> None:
        """Verify queries for non-existent dates return empty lists without error."""
        assert get_daily_revenue(pg_conn, start_date="2024-01-01", end_date="2024-01-01") == []
        assert get_channel_performance(pg_conn, start_date="2024-01-01", end_date="2024-01-01") == []
