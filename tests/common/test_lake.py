"""Tests for retailpulse.common.lake."""

import io
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig
from retailpulse.common.lake import (
    build_lake_path,
    get_minio_client,
    list_partitions,
    read_parquet,
    write_parquet,
)


def _make_config() -> AppConfig:
    """Return a test AppConfig."""
    return AppConfig(
        postgres=PostgresConfig(password="p"),
        minio=MinIOConfig(
            endpoint="localhost:9000",
            access_key="key",
            secret_key="secret",
            bucket="testbucket",
        ),
    )


# -- build_lake_path ---------------------------------------------------------

class TestBuildLakePath:
    """Tests for build_lake_path()."""

    def test_bronze_path(self) -> None:
        path = build_lake_path("bronze", "sales", "2025-01-15")
        assert path == "bronze/sales/dt=2025-01-15/data.parquet"

    def test_silver_path(self) -> None:
        path = build_lake_path("silver", "inventory", "2025-03-01")
        assert path == "silver/inventory/dt=2025-03-01/data.parquet"

    def test_quarantine_path(self) -> None:
        path = build_lake_path("quarantine", "customers", "2025-06-30")
        assert path == "quarantine/customers/dt=2025-06-30/data.parquet"

    def test_custom_filename(self) -> None:
        path = build_lake_path("silver", "sales", "2025-03-01", "part-0000.parquet")
        assert path == "silver/sales/dt=2025-03-01/part-0000.parquet"

    def test_invalid_layer_raises(self) -> None:
        with pytest.raises(ValueError, match="layer must be"):
            build_lake_path("gold", "sales", "2025-01-01")


# -- get_minio_client --------------------------------------------------------

class TestGetMinioClient:
    """Tests for get_minio_client()."""

    @mock.patch("retailpulse.common.lake.Minio")
    def test_creates_client_from_config(self, mock_minio_cls: mock.MagicMock) -> None:
        cfg = _make_config()
        client = get_minio_client(config=cfg)

        mock_minio_cls.assert_called_once_with(
            "localhost:9000",
            access_key="key",
            secret_key="secret",
            secure=False,
        )
        assert client is mock_minio_cls.return_value


# -- write_parquet -----------------------------------------------------------

class TestWriteParquet:
    """Tests for write_parquet()."""

    def test_writes_valid_parquet(self) -> None:
        client = mock.MagicMock()
        table = pa.table({"col": [1, 2, 3]})

        write_parquet(client, "bucket", "test/path.parquet", table)

        client.put_object.assert_called_once()
        call_args = client.put_object.call_args
        assert call_args[0][0] == "bucket"
        assert call_args[0][1] == "test/path.parquet"

        # The uploaded bytes must be valid Parquet.
        buf = call_args[0][2]
        buf.seek(0)
        roundtrip = pq.read_table(buf)
        assert roundtrip.num_rows == 3
        assert roundtrip.column_names == ["col"]


# -- read_parquet ------------------------------------------------------------

class TestReadParquet:
    """Tests for read_parquet()."""

    def test_reads_valid_parquet(self) -> None:
        # Prepare a Parquet buffer to act as the stored object.
        table = pa.table({"x": [10, 20]})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        parquet_bytes = buf.getvalue()

        response = mock.MagicMock()
        response.read.return_value = parquet_bytes

        client = mock.MagicMock()
        client.get_object.return_value = response

        result = read_parquet(client, "bucket", "obj.parquet")

        assert result.num_rows == 2
        assert result.column_names == ["x"]
        response.close.assert_called_once()
        response.release_conn.assert_called_once()


# -- list_partitions ---------------------------------------------------------

class TestListPartitions:
    """Tests for list_partitions()."""

    def test_extracts_sorted_dates(self) -> None:
        obj1 = mock.MagicMock()
        obj1.object_name = "bronze/sales/dt=2025-01-02/"
        obj2 = mock.MagicMock()
        obj2.object_name = "bronze/sales/dt=2025-01-01/"

        client = mock.MagicMock()
        client.list_objects.return_value = [obj1, obj2]

        dates = list_partitions(client, "bucket", "bronze", "sales")

        assert dates == ["2025-01-01", "2025-01-02"]
        client.list_objects.assert_called_once_with(
            "bucket", prefix="bronze/sales/dt=", recursive=False,
        )

    def test_empty_bucket_returns_empty_list(self) -> None:
        client = mock.MagicMock()
        client.list_objects.return_value = []

        dates = list_partitions(client, "bucket", "bronze", "sales")
        assert dates == []

    def test_deduplicates_dates(self) -> None:
        obj1 = mock.MagicMock()
        obj1.object_name = "bronze/sales/dt=2025-01-01/a.parquet"
        obj2 = mock.MagicMock()
        obj2.object_name = "bronze/sales/dt=2025-01-01/b.parquet"

        client = mock.MagicMock()
        client.list_objects.return_value = [obj1, obj2]

        dates = list_partitions(client, "bucket", "bronze", "sales")
        assert dates == ["2025-01-01"]