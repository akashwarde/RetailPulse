"""Unit tests for warehouse dimension loading (Task 3.1)."""

from datetime import date, datetime
from typing import Any
from unittest.mock import MagicMock, call

import pyarrow as pa
import pytest

from retailpulse.warehouse.load_dimensions import (
    DimensionLoadResult,
    ensure_date_key,
    load_dim_customer,
    load_dim_date,
    load_dim_product,
    load_dim_store,
    load_dimensions,
    main as load_dimensions_main,
)


class TestLoadDimDate:
    """Tests for dim_date loading and dynamic date key generation."""

    def test_load_dim_date_executes_generate_series(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 731
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        result = load_dim_date(mock_conn, start_date="2024-01-01", end_date="2025-12-31")

        assert result.dimension == "dim_date"
        assert result.loaded == 731
        assert result.skipped == 0
        mock_cur.execute.assert_called_once()
        query = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        assert "INSERT INTO dw.dim_date" in query
        assert "generate_series" in query
        assert "ON CONFLICT (date_key) DO NOTHING" in query
        assert params == ("2024-01-01", "2025-12-31")
        mock_conn.commit.assert_called_once()

    def test_ensure_date_key_inserts_future_date(self) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        date_key = ensure_date_key(mock_conn, "2026-06-15")

        assert date_key == 20260615
        mock_cur.execute.assert_called_once()
        query = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1]
        assert "INSERT INTO dw.dim_date" in query
        assert "ON CONFLICT (date_key) DO NOTHING" in query
        assert params[0] == 20260615
        assert params[1] == "2026-06-15"
        mock_conn.commit.assert_called_once()


class TestLoadDimProduct:
    """Tests for dim_product loading, column mappings, and validation."""

    def test_load_dim_product_maps_product_name_to_name(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_dimensions.execute_batch"
        )

        sample_data = [
            {
                "product_id": 101,
                "product_name": "Wireless Mouse",
                "category": "Electronics",
                "unit_price": 599.99,
            },
            {
                "product_id": 102,
                "product_name": "Desk Lamp",
                "category": "Home",
                "unit_price": 849.50,
            },
        ]

        result = load_dim_product(mock_conn, data=sample_data)

        assert result.dimension == "dim_product"
        assert result.total_records == 2
        assert result.loaded == 2
        assert result.skipped == 0

        mock_execute_batch.assert_called_once()
        call_args = mock_execute_batch.call_args
        sql = call_args[0][1]
        params = call_args[0][2]

        assert "INSERT INTO dw.dim_product" in sql
        assert "ON CONFLICT (product_id) DO UPDATE SET" in sql
        assert "name = EXCLUDED.name" in sql
        assert params == [
            (101, "Wireless Mouse", "Electronics", 599.99),
            (102, "Desk Lamp", "Home", 849.50),
        ]
        mock_conn.commit.assert_called_once()

    def test_load_dim_product_handles_pyarrow_table(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_dimensions.execute_batch"
        )

        table = pa.table({
            "product_id": [201],
            "product_name": ["Noise Cancelling Headphones"],
            "category": ["Electronics"],
            "unit_price": [4999.0],
        })

        result = load_dim_product(mock_conn, data=table)

        assert result.loaded == 1
        assert result.skipped == 0
        params = mock_execute_batch.call_args[0][2]
        assert params == [(201, "Noise Cancelling Headphones", "Electronics", 4999.0)]

    def test_load_dim_product_skips_invalid_records(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_dimensions.execute_batch"
        )

        sample_data = [
            {"product_id": 1, "product_name": "Valid 1", "category": "Cat A", "unit_price": 10.0},
            {"product_id": None, "product_name": "No ID", "category": "Cat A", "unit_price": 10.0},
            {"product_id": 2, "product_name": None, "category": "Cat B", "unit_price": 20.0},
            {"product_id": 3, "product_name": "Negative Price", "category": "Cat C", "unit_price": -5.0},
            {"product_id": 4, "product_name": "Valid 2", "category": "Cat D", "unit_price": 30.0},
        ]

        result = load_dim_product(mock_conn, data=sample_data)

        assert result.total_records == 5
        assert result.loaded == 2
        assert result.skipped == 3
        params = mock_execute_batch.call_args[0][2]
        assert len(params) == 2
        assert params[0][0] == 1
        assert params[1][0] == 4


class TestLoadDimCustomer:
    """Tests for dim_customer loading and column mappings."""

    def test_load_dim_customer_upsert(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_dimensions.execute_batch"
        )

        sample_customers = [
            {
                "customer_id": 501,
                "first_name": "Aarav",
                "last_name": "Sharma",
                "email": "aarav@example.com",
                "city": "Mumbai",
                "state": "Maharashtra",
                "segment": "Premium",
            },
            {
                "customer_id": 502,
                "first_name": "Diya",
                "last_name": "Patel",
                "email": "diya@example.com",
                "city": "Ahmedabad",
                "state": "Gujarat",
            },
        ]

        result = load_dim_customer(mock_conn, data=sample_customers)

        assert result.dimension == "dim_customer"
        assert result.total_records == 2
        assert result.loaded == 2
        assert result.skipped == 0

        mock_execute_batch.assert_called_once()
        sql = mock_execute_batch.call_args[0][1]
        params = mock_execute_batch.call_args[0][2]

        assert "INSERT INTO dw.dim_customer" in sql
        assert "ON CONFLICT (customer_id) DO UPDATE SET" in sql
        assert params[0] == (501, "Aarav", "Sharma", "aarav@example.com", "Mumbai", "Maharashtra", "Premium")
        # Second customer defaults to 'Standard' segment
        assert params[1] == (502, "Diya", "Patel", "diya@example.com", "Ahmedabad", "Gujarat", "Standard")

    def test_load_dim_customer_skips_missing_id(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_dimensions.execute_batch"
        )

        sample = [
            {"customer_id": None, "first_name": "Ghost"},
            {"customer_id": "not_an_int", "first_name": "Bad ID"},
            {"customer_id": 123, "first_name": "Real"},
        ]

        result = load_dim_customer(mock_conn, data=sample)

        assert result.total_records == 3
        assert result.loaded == 1
        assert result.skipped == 2


class TestLoadDimStore:
    """Tests for dim_store loading and column mappings."""

    def test_load_dim_store_upsert(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_dimensions.execute_batch"
        )

        sample_stores = [
            {
                "store_id": 1,
                "store_name": "Mumbai Downtown",
                "city": "Mumbai",
                "state": "Maharashtra",
                "store_type": "Physical",
            },
            {
                "store_id": 2,
                "city": "Bengaluru",
                "type": "Online",
            },
        ]

        result = load_dim_store(mock_conn, data=sample_stores)

        assert result.dimension == "dim_store"
        assert result.total_records == 2
        assert result.loaded == 2
        assert result.skipped == 0

        mock_execute_batch.assert_called_once()
        sql = mock_execute_batch.call_args[0][1]
        params = mock_execute_batch.call_args[0][2]

        assert "INSERT INTO dw.dim_store" in sql
        assert "ON CONFLICT (store_id) DO UPDATE SET" in sql
        assert params[0] == (1, "Mumbai Downtown", "Mumbai", "Maharashtra", "Physical")
        # Second store generates fallback name and maps 'type' to store_type
        assert params[1] == (2, "Store 2", "Bengaluru", None, "Online")

    def test_load_dim_store_skips_invalid(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch = mocker.patch(
            "retailpulse.warehouse.load_dimensions.execute_batch"
        )

        sample = [
            {"store_id": None, "city": "Pune"},
            {"store_id": 1, "city": None},
            {"store_id": 2, "city": "Delhi"},
        ]

        result = load_dim_store(mock_conn, data=sample)

        assert result.total_records == 3
        assert result.loaded == 1
        assert result.skipped == 2


class TestLoadDimensionsOrchestration:
    """Tests for the overall load_dimensions orchestrator."""

    def test_missing_silver_dataset_handled_gracefully(self, mocker: Any) -> None:
        mock_conn = MagicMock()
        mock_minio = MagicMock()
        mocker.patch(
            "retailpulse.warehouse.load_dimensions.read_parquet",
            side_effect=Exception("Object does not exist"),
        )
        mock_cur = MagicMock()
        mock_cur.rowcount = 0
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        mock_cfg = MagicMock()
        mock_cfg.minio.bucket = "test-bucket"

        with pytest.raises(Exception, match="does not exist"):
            load_dimensions(run_date="2025-01-15", config=mock_cfg,
                            minio_client=mock_minio, conn=mock_conn)

    def test_load_dimensions_selective(self, mocker: Any) -> None:
        mocker.patch("retailpulse.warehouse.load_dimensions.read_parquet", return_value=pa.table({"product_id": []}))
        mock_conn = MagicMock()
        mock_minio = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 0
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cfg = MagicMock()
        mock_cfg.minio.bucket = "test-bucket"

        results = load_dimensions(
            run_date="2025-01-15",
            dimensions=["dim_date", "dim_product"],
            config=mock_cfg,
            minio_client=mock_minio,
            conn=mock_conn,
        )

        assert "dim_date" in results
        assert "dim_product" in results
        assert "dim_customer" not in results
        assert "dim_store" not in results
