"""Unit tests for warehouse data marts and analytical reporting (Task 3.3)."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from retailpulse.warehouse.marts import (
    get_channel_performance,
    get_daily_revenue,
    get_dq_summary,
    get_low_stock_alerts,
    get_top_products,
    refresh_marts,
)


class TestMartSQLFiles:
    """Verifies that individual mart SQL files exist and define the expected views."""

    def test_sql_files_exist(self) -> None:
        marts_dir = Path("sql/marts")
        assert marts_dir.exists(), "sql/marts directory must exist"
        expected_files = [
            "01_mart_daily_revenue.sql",
            "02_mart_top_products.sql",
            "03_mart_low_stock.sql",
            "04_mart_dq_summary.sql",
            "05_mart_channel_performance.sql",
        ]
        for fname in expected_files:
            file_path = marts_dir / fname
            assert file_path.exists(), f"Expected mart SQL file {fname} not found"
            content = file_path.read_text(encoding="utf-8")
            assert "CREATE OR REPLACE VIEW dw." in content

    def test_channel_performance_calculates_share(self) -> None:
        file_path = Path("sql/marts/05_mart_channel_performance.sql")
        content = file_path.read_text(encoding="utf-8")
        assert "channel_share_pct" in content
        assert "OVER (PARTITION BY f.date_key)" in content


class TestRefreshMarts:
    """Tests for refresh_marts orchestrator."""

    def test_refresh_marts_executes_all_sql_scripts(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        executed = refresh_marts(mock_conn)

        assert len(executed) == 5
        assert "01_mart_daily_revenue.sql" in executed
        assert "02_mart_top_products.sql" in executed
        assert "03_mart_low_stock.sql" in executed
        assert "04_mart_dq_summary.sql" in executed
        assert "05_mart_channel_performance.sql" in executed
        assert mock_cur.execute.call_count == 5
        mock_conn.commit.assert_called_once()

    def test_refresh_marts_rolls_back_on_error(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.execute.side_effect = RuntimeError("Syntax error in SQL")

        with pytest.raises(RuntimeError, match="Syntax error in SQL"):
            refresh_marts(mock_conn)

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()


class TestMartQueries:
    """Tests for parameterized mart querying helper functions."""

    def test_get_daily_revenue_parameterized_dates(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        mock_cur.description = [
            ("date_key",), ("full_date",), ("year",), ("month",), ("month_name",),
            ("day_name",), ("is_weekend",), ("total_revenue",), ("total_orders",),
            ("total_units_sold",), ("avg_order_value",),
        ]
        mock_cur.fetchall.return_value = [
            (20250115, "2025-01-15", 2025, 1, "January", "Wednesday", False, 1500.0, 10, 25, 150.0)
        ]

        results = get_daily_revenue(mock_conn, start_date="2025-01-01", end_date="2025-01-31")

        assert len(results) == 1
        assert results[0]["total_revenue"] == 1500.0
        assert results[0]["total_orders"] == 10

        query, params = mock_cur.execute.call_args[0]
        assert "full_date >= %s::DATE" in query
        assert "full_date <= %s::DATE" in query
        assert params == ("2025-01-01", "2025-01-31")

    def test_get_daily_revenue_invalid_date_range_raises(self) -> None:
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="cannot be after end_date"):
            get_daily_revenue(mock_conn, start_date="2025-02-01", end_date="2025-01-01")

    def test_get_top_products_limit_and_category(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        mock_cur.description = [
            ("product_key",), ("product_id",), ("product_name",), ("category",),
            ("price",), ("total_revenue",), ("total_units_sold",), ("total_orders",),
            ("revenue_rank",),
        ]
        mock_cur.fetchall.return_value = [
            (1, 101, "Wireless Mouse", "Electronics", 49.99, 5000.0, 100, 80, 1)
        ]

        results = get_top_products(mock_conn, limit=5, category="Electronics")

        assert len(results) == 1
        assert results[0]["product_name"] == "Wireless Mouse"
        assert results[0]["revenue_rank"] == 1

        query, params = mock_cur.execute.call_args[0]
        assert "category = %s" in query
        assert "LIMIT %s" in query
        assert params == ("Electronics", 5)

    def test_get_top_products_invalid_limit_raises(self) -> None:
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="limit must be greater than 0"):
            get_top_products(mock_conn, limit=0)

        with pytest.raises(ValueError, match="limit must be greater than 0"):
            get_top_products(mock_conn, limit=-5)

    def test_get_low_stock_alerts_threshold_and_store(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        mock_cur.description = [
            ("inventory_key",), ("date_key",), ("snapshot_date",), ("product_key",),
            ("product_id",), ("product_name",), ("category",), ("store_key",),
            ("store_id",), ("store_name",), ("store_city",), ("stock_qty",),
            ("low_stock_threshold",), ("stock_status",),
        ]
        mock_cur.fetchall.return_value = [
            (1, 20250115, "2025-01-15", 1, 101, "Wireless Mouse", "Electronics", 10, 1, "Mumbai", "Mumbai", 3, 10, "CRITICAL")
        ]

        results = get_low_stock_alerts(mock_conn, store_id=1, threshold=5, snapshot_date="2025-01-15", latest_only=True)

        assert len(results) == 1
        assert results[0]["stock_status"] == "CRITICAL"
        assert results[0]["stock_qty"] == 3

        query, params = mock_cur.execute.call_args[0]
        assert "store_id = %s" in query
        assert "stock_qty <= %s" in query
        assert "snapshot_date = %s::DATE" in query
        assert "date_key = (SELECT MAX(date_key) FROM dw.mart_low_stock)" in query
        assert params == (1, 5, "2025-01-15")

    def test_get_low_stock_alerts_negative_threshold_raises(self) -> None:
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="threshold must be non-negative"):
            get_low_stock_alerts(mock_conn, threshold=-1)

    def test_get_dq_summary_source_filter(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        mock_cur.description = [
            ("source",), ("total_batches",), ("total_records",), ("total_accepted",),
            ("total_rejected",), ("overall_reject_pct",), ("latest_batch_time",),
        ]
        mock_cur.fetchall.return_value = [
            ("sales", 5, 1000, 980, 20, 2.0, "2025-01-15 12:00:00")
        ]

        results = get_dq_summary(mock_conn, source="sales")

        assert len(results) == 1
        assert results[0]["source"] == "sales"
        assert results[0]["overall_reject_pct"] == 2.0

        query, params = mock_cur.execute.call_args[0]
        assert "WHERE source = %s" in query
        assert params == ("sales",)

    def test_get_channel_performance(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        mock_cur.description = [
            ("date_key",), ("full_date",), ("channel",), ("store_id",),
            ("store_name",), ("city",), ("state",), ("total_revenue",),
            ("total_orders",), ("total_units_sold",), ("avg_order_value",),
            ("channel_share_pct",),
        ]
        mock_cur.fetchall.return_value = [
            (20250115, "2025-01-15", "online", 0, "Online Channel", "Online", "N/A", 2500.0, 15, 30, 166.67, 38.46),
            (20250115, "2025-01-15", "in_store", 1, "Mumbai Downtown", "Mumbai", "Maharashtra", 4000.0, 25, 50, 160.0, 61.54),
        ]

        results = get_channel_performance(mock_conn, channel="online", start_date="2025-01-01", end_date="2025-01-31")

        assert len(results) == 2
        assert results[0]["channel"] == "online"
        assert results[0]["channel_share_pct"] == 38.46
        assert results[1]["channel"] == "in_store"

        query, params = mock_cur.execute.call_args[0]
        assert "channel = %s" in query
        assert "full_date >= %s::DATE" in query
        assert "full_date <= %s::DATE" in query
        assert "channel_share_pct" in query
        assert params == ("2025-01-01", "2025-01-31", "online")

    def test_get_channel_performance_invalid_date_range_raises(self) -> None:
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="cannot be after end_date"):
            get_channel_performance(mock_conn, start_date="2025-02-01", end_date="2025-01-01")

    def test_get_channel_performance_invalid_channel_raises(self) -> None:
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="channel must be 'in_store' or 'online'"):
            get_channel_performance(mock_conn, channel="mail_order")
