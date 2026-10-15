"""Unit tests for warehouse fact loading (Task 3.2)."""

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock, call

import pyarrow as pa
import pytest

from retailpulse.warehouse.load_facts import (
    FactLoadResult,
    _parse_date_and_timestamp,
    fetch_dimension_lookups,
    load_fact_inventory,
    load_fact_sales,
    load_facts,
    main as load_facts_main,
)


@pytest.fixture
def mock_lookups() -> Any:
    """Mock dimension lookup caches."""
    return {
        "products": {101: 1, 102: 2},
        "customers": {501: 10, 502: 20},
        "stores": {1: 100, 2: 200},
    }


class TestFetchDimensionLookups:
    """Tests for dimension lookup cache population."""

    def test_fetch_dimension_lookups(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        mock_cur.fetchall.side_effect = [
            [(101, 1), (102, 2)],  # products
            [(501, 10), (502, 20)],  # customers
            [(1, 100), (2, 200)],  # stores
        ]

        lookups = fetch_dimension_lookups(mock_conn)

        assert lookups["products"] == {101: 1, 102: 2}
        assert lookups["customers"] == {501: 10, 502: 20}
        assert lookups["stores"] == {1: 100, 2: 200}
        assert mock_cur.execute.call_count == 3


class TestLoadFactSales:
    """Tests for fact_sales loading, surrogate key lookups, and validations."""

    def test_load_fact_sales_mapping_and_lookups(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mock_ensure_date = mocker.patch(
            "retailpulse.warehouse.load_facts.ensure_date_key"
        )
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch"
        )

        sample_sales = [
            {
                "order_id": 1001,
                "order_item_id": 5001,
                "customer_id": 501,
                "store_id": 1,
                "product_id": 101,
                "quantity": 2,
                "unit_price": 50.0,
                "total_amount": 100.0,
                "order_timestamp": "2025-01-15 10:30:00",
                "order_date": "2025-01-15",
                "channel": "in_store",
            }
        ]

        result = load_fact_sales(mock_conn, data=sample_sales, run_date="2025-01-15")

        assert result.fact_table == "fact_sales"
        assert result.total_records == 1
        assert result.loaded == 1
        assert result.skipped == 0

        # Verify date key was ensured
        mock_ensure_date.assert_called()

        # Verify delete-and-reload executed in transaction
        assert mock_cur.execute.call_count >= 1
        delete_call = mock_cur.execute.call_args_list[0]
        assert "DELETE FROM dw.fact_sales" in delete_call[0][0]

        # Verify insert batch parameters
        mock_execute_batch.assert_called_once()
        batch_params = mock_execute_batch.call_args[0][2]
        assert len(batch_params) == 1
        row = batch_params[0]
        assert row[0] == 1001
        assert row[1] == 5001
        assert row[2] == 20250115
        assert row[3] == 1  # product_key for 101
        assert row[4] == 10  # customer_key for 501
        assert row[5] == 100  # store_key for 1
        assert row[6] == 2  # quantity
        assert row[7] == 50.0  # unit_price
        assert row[8] == 100.0  # revenue
        assert row[9] == "in_store"
        assert isinstance(row[10], datetime)

        mock_conn.commit.assert_called_once()

    def test_load_fact_sales_online_channel_store_nullable(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch"
        )

        online_sale = [
            {
                "order_id": 2002,
                "order_item_id": 6001,
                "customer_id": 502,
                "store_id": None,
                "product_id": 102,
                "quantity": 1,
                "unit_price": 250.0,
                "total_amount": 250.0,
                "order_timestamp": "2025-01-15 15:00:00",
                "channel": "online", "event_id": "event-2002",
            }
        ]

        result = load_fact_sales(mock_conn, data=online_sale, channel="online")

        assert result.loaded == 1
        batch_params = mock_execute_batch.call_args[0][2]
        row = batch_params[0]
        assert row[3] == 2  # product_key
        assert row[4] == 20  # customer_key
        assert row[5] is None  # store_key is None for online orders
        assert row[9] == "online"

    def test_load_fact_sales_instore_missing_store_skipped(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch"
        )

        # In-store sale with missing store_id should be skipped
        sales = [
            {
                "order_id": 3001,
                "customer_id": 501,
                "store_id": None,
                "product_id": 101,
                "quantity": 1,
                "unit_price": 50.0,
                "order_timestamp": "2025-01-15 10:00:00",
                "channel": "in_store",
            }
        ]

        result = load_fact_sales(mock_conn, data=sales, channel="in_store")
        assert result.total_records == 1
        assert result.loaded == 0
        assert result.skipped == 1

    def test_load_fact_sales_mixed_channels_preserves_online(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mocker.patch("retailpulse.warehouse.load_facts.execute_batch")

        mixed_sales = [
            {
                "order_id": 1,
                "customer_id": 501,
                "store_id": 1,
                "product_id": 101,
                "quantity": 1,
                "unit_price": 50.0,
                "order_timestamp": "2025-01-15 10:00:00",
                "channel": "in_store",
            },
            {
                "order_id": 2,
                "customer_id": 502,
                "store_id": None,
                "product_id": 102,
                "quantity": 1,
                "unit_price": 100.0,
                "order_timestamp": "2025-01-15 11:00:00",
                "channel": "online", "event_id": "event-2002",
            },
        ]

        result = load_fact_sales(mock_conn, data=mixed_sales)
        assert result.loaded == 2

        # Verify that delete was executed for both channels
        delete_calls = [
            c[0][1] for c in mock_cur.execute.call_args_list if "DELETE FROM dw.fact_sales" in c[0][0]
        ]
        deleted_channels = {c[1] for c in delete_calls}
        assert "in_store" in deleted_channels
        assert "online" not in deleted_channels

    def test_load_fact_sales_nan_and_inf_rejected(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch"
        )

        nan_sales = [
            {"order_id": 1, "customer_id": 501, "store_id": 1, "product_id": 101, "quantity": 1, "unit_price": float("nan"), "order_timestamp": "2025-01-15"},
            {"order_id": 2, "customer_id": 501, "store_id": 1, "product_id": 101, "quantity": 1, "unit_price": float("inf"), "order_timestamp": "2025-01-15"},
            {"order_id": 3, "customer_id": 501, "store_id": 1, "product_id": 101, "quantity": 1, "unit_price": 25.0, "total_amount": float("nan"), "order_timestamp": "2025-01-15"},
            {"order_id": 4, "customer_id": 501, "store_id": 1, "product_id": 101, "quantity": 1, "unit_price": 25.0, "total_amount": 25.0, "order_timestamp": "2025-01-15"},
        ]

        result = load_fact_sales(mock_conn, data=nan_sales)
        assert result.total_records == 4
        assert result.loaded == 1
        assert result.skipped == 3

    def test_load_fact_sales_missing_dimensions_skipped(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch"
        )

        sales_with_missing_dims = [
            {
                "order_id": 1,
                "customer_id": 9999,  # Unknown customer
                "store_id": 1,
                "product_id": 101,
                "quantity": 1,
                "unit_price": 10.0,
                "order_timestamp": "2025-01-15 10:00:00",
            },
            {
                "order_id": 2,
                "customer_id": 501,
                "store_id": 1,
                "product_id": 8888,  # Unknown product
                "quantity": 1,
                "unit_price": 10.0,
                "order_timestamp": "2025-01-15 10:00:00",
            },
            {
                "order_id": 3,
                "customer_id": 501,
                "store_id": 7777,  # Unknown store for in-store order
                "product_id": 101,
                "quantity": 1,
                "unit_price": 10.0,
                "order_timestamp": "2025-01-15 10:00:00",
                "channel": "in_store",
            },
            {
                "order_id": 4,  # Valid record
                "customer_id": 501,
                "store_id": 1,
                "product_id": 101,
                "quantity": 1,
                "unit_price": 10.0,
                "order_timestamp": "2025-01-15 10:00:00",
            },
        ]

        result = load_fact_sales(mock_conn, data=sales_with_missing_dims)

        assert result.total_records == 4
        assert result.loaded == 1
        assert result.skipped == 3
        batch_params = mock_execute_batch.call_args[0][2]
        assert len(batch_params) == 1
        assert batch_params[0][0] == 4

    def test_load_fact_sales_database_error_rollback(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch",
            side_effect=Exception("Database connection failure"),
        )

        valid_sale = [
            {"order_id": 1, "customer_id": 501, "store_id": 1, "product_id": 101, "quantity": 1, "unit_price": 10.0, "order_timestamp": "2025-01-15"}
        ]

        with pytest.raises(Exception, match="Database connection failure"):
            load_fact_sales(mock_conn, data=valid_sale)

        mock_conn.rollback.assert_called_once()


class TestLoadFactInventory:
    """Tests for fact_inventory loading and upsert conflict handling."""

    def test_load_fact_inventory_mapping_and_lookups(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mock_ensure_date = mocker.patch(
            "retailpulse.warehouse.load_facts.ensure_date_key"
        )
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch"
        )

        sample_inventory = [
            {
                "product_id": 101,
                "store_id": 1,
                "stock_qty": 45,
                "snapshot_date": "2025-01-15",
            },
            {
                "product_id": 102,
                "store_id": 2,
                "stock_qty": 0,
                "snapshot_date": "2025-01-15",
            },
        ]

        result = load_fact_inventory(
            mock_conn, data=sample_inventory, run_date="2025-01-15"
        )

        assert result.fact_table == "fact_inventory"
        assert result.total_records == 2
        assert result.loaded == 2
        assert result.skipped == 0

        mock_ensure_date.assert_called()
        mock_execute_batch.assert_called_once()
        sql = mock_execute_batch.call_args[0][1]
        params = mock_execute_batch.call_args[0][2]

        assert "INSERT INTO dw.fact_inventory" in sql
        assert "ON CONFLICT (date_key, product_key, store_key) DO UPDATE" in sql
        assert len(params) == 2
        assert params[0] == (20250115, 1, 100, 45)
        assert params[1] == (20250115, 2, 200, 0)
        mock_conn.commit.assert_called_once()

    def test_load_fact_inventory_skips_negative_stock_and_missing_dims(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch"
        )

        records = [
            {"product_id": 101, "store_id": 1, "stock_qty": -10, "snapshot_date": "2025-01-15"},
            {"product_id": 9999, "store_id": 1, "stock_qty": 10, "snapshot_date": "2025-01-15"},
            {"product_id": 101, "store_id": 8888, "stock_qty": 10, "snapshot_date": "2025-01-15"},
            {"product_id": 101, "store_id": 1, "stock_qty": 10, "snapshot_date": "2025-01-15"},
        ]

        result = load_fact_inventory(mock_conn, data=records)

        assert result.total_records == 4
        assert result.loaded == 1
        assert result.skipped == 3
        batch_params = mock_execute_batch.call_args[0][2]
        assert len(batch_params) == 1
        assert batch_params[0] == (20250115, 1, 100, 10)

    def test_load_fact_inventory_database_error_rollback(
        self, mocker: Any, mock_lookups: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mocker.patch(
            "retailpulse.warehouse.load_facts.fetch_dimension_lookups",
            return_value=mock_lookups,
        )
        mocker.patch("retailpulse.warehouse.load_facts.ensure_date_key")
        mocker.patch(
            "retailpulse.warehouse.load_facts.execute_batch",
            side_effect=Exception("Database write error"),
        )

        records = [{"product_id": 101, "store_id": 1, "stock_qty": 5, "snapshot_date": "2025-01-15"}]

        with pytest.raises(Exception, match="Database write error"):
            load_fact_inventory(mock_conn, data=records)

        mock_conn.rollback.assert_called_once()


class TestDateParsingAndNormalization:
    """Tests for timestamp normalization and timezone stripping."""

    def test_timezone_aware_timestamp_normalized_to_utc(self) -> None:
        tz_ts = datetime(2025, 5, 20, 14, 0, 0, tzinfo=timezone.utc)
        d_val, ts_val = _parse_date_and_timestamp(tz_ts)
        assert d_val == date(2025, 5, 20)
        assert ts_val.tzinfo is None  # Naive UTC for PostgreSQL TIMESTAMP column
        assert ts_val.hour == 14

    def test_iso_string_with_z_parsed(self) -> None:
        d_val, ts_val = _parse_date_and_timestamp("2026-03-10T12:00:00Z")
        assert d_val == date(2026, 3, 10)
        assert ts_val.year == 2026
        assert ts_val.tzinfo is None


class TestLoadFactsOrchestrator:
    """Tests for load_facts orchestrator and lake integration."""

    def test_load_facts_orchestrator_handles_missing_lake_files(
        self, mocker: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_minio = MagicMock()
        mocker.patch(
            "retailpulse.warehouse.load_facts.read_parquet",
            side_effect=Exception("Lake object not found"),
        )
        mock_cfg = MagicMock()
        mock_cfg.minio.bucket = "retailpulse"

        with pytest.raises(Exception, match="not found"):
            load_facts(run_date="2025-01-15", config=mock_cfg,
                       minio_client=mock_minio, conn=mock_conn)

    def test_load_facts_unexpected_storage_error_reraised(
        self, mocker: Any
    ) -> None:
        mock_conn = MagicMock()
        mock_minio = MagicMock()
        mocker.patch(
            "retailpulse.warehouse.load_facts.read_parquet",
            side_effect=PermissionError("Access denied to MinIO storage"),
        )
        mock_cfg = MagicMock()
        mock_cfg.minio.bucket = "retailpulse"

        with pytest.raises(PermissionError, match="Access denied"):
            load_facts(
                run_date="2025-01-15",
                config=mock_cfg,
                minio_client=mock_minio,
                conn=mock_conn,
            )

    def test_load_facts_selective(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_minio = MagicMock()
        mocker.patch(
            "retailpulse.warehouse.load_facts.read_parquet",
            return_value=pa.table({"order_id": []}),
        )
        mock_cfg = MagicMock()
        mock_cfg.minio.bucket = "retailpulse"

        results = load_facts(
            run_date="2025-01-15",
            facts=["fact_sales"],
            config=mock_cfg,
            minio_client=mock_minio,
            conn=mock_conn,
        )

        assert "fact_sales" in results
        assert "fact_inventory" not in results
