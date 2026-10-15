"""Unit tests for PySpark Silver inventory transformation pipeline."""

from datetime import date
from unittest import mock
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    TimestampType,
)

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig
from retailpulse.transform import (
    SILVER_INVENTORY_COLUMNS,
    inventory_main,
    run_silver_inventory,
    transform_inventory,
)
from retailpulse.validation import QualityGateError


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession fixture for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("RetailPulseSilverInventoryTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_config() -> AppConfig:
    """Return a test AppConfig."""
    return AppConfig(
        postgres=PostgresConfig(password="pass"),
        minio=MinIOConfig(
            endpoint="localhost:9000",
            access_key="admin",
            secret_key="secret",
            bucket="testbucket",
        ),
    )


class TestTransformInventory:
    """Tests for the pure DataFrame transformation function transform_inventory()."""

    def test_schema_types_and_date_parsing(self, spark: SparkSession) -> None:
        """Verifies type casting, date normalization, and processed timestamp generation."""
        data = [
            (101, 1, 50, "2025-01-15"),
            (102, 2, 0, "15/01/2025"),
        ]
        df = spark.createDataFrame(data, ["product_id", "store_id", "stock_qty", "snapshot_date"])

        silver_df = transform_inventory(df)

        assert silver_df.columns == SILVER_INVENTORY_COLUMNS
        assert silver_df.count() == 2

        schema_dict = {f.name: f.dataType for f in silver_df.schema.fields}
        assert isinstance(schema_dict["product_id"], LongType)
        assert isinstance(schema_dict["store_id"], LongType)
        assert isinstance(schema_dict["stock_qty"], IntegerType)
        assert isinstance(schema_dict["snapshot_date"], DateType)
        assert isinstance(schema_dict["_processed_at"], TimestampType)

        rows = {r["product_id"]: r for r in silver_df.collect()}
        assert rows[101]["snapshot_date"] == date(2025, 1, 15)
        assert rows[102]["snapshot_date"] == date(2025, 1, 15)
        assert rows[101]["_processed_at"] is not None

    def test_deduplication(self, spark: SparkSession) -> None:
        """Deduplicates on composite key (product_id, store_id, snapshot_date)."""
        data = [
            (501, 1, 20, "2025-01-15"),
            (501, 1, 25, "2025-01-15"),  # duplicate snapshot
            (502, 1, 30, "2025-01-15"),
        ]
        df = spark.createDataFrame(data, ["product_id", "store_id", "stock_qty", "snapshot_date"])

        silver_df = transform_inventory(df)

        assert silver_df.count() == 2
        prod_ids = [r["product_id"] for r in silver_df.collect()]
        assert prod_ids == [501, 502]

    def test_empty_dataframe(self, spark: SparkSession) -> None:
        """Empty input DataFrame produces empty Silver DataFrame with correct schema."""
        schema = spark.createDataFrame([], "product_id long, store_id long, stock_qty int, snapshot_date string").schema
        df = spark.createDataFrame([], schema)

        silver_df = transform_inventory(df)

        assert silver_df.count() == 0
        assert silver_df.columns == SILVER_INVENTORY_COLUMNS


class TestRunSilverInventory:
    """Tests for the end-to-end run_silver_inventory() pipeline."""

    @mock.patch("retailpulse.transform.silver_inventory.write_parquet")
    @mock.patch("retailpulse.transform.silver_inventory.write_quarantine")
    @mock.patch("retailpulse.transform.silver_inventory.write_quality_summary")
    def test_end_to_end_successful_run(
        self,
        mock_summary: mock.MagicMock,
        mock_quarantine: mock.MagicMock,
        mock_write_parquet: mock.MagicMock,
        spark: SparkSession,
    ) -> None:
        """End-to-end execution validates, quarantines bad rows, writes summary, and saves Silver."""
        # 20 rows: 19 valid, 1 invalid (null product_id) -> 5% reject rate <= 5%
        valid_rows = [(i, 1, 100, "2025-01-15") for i in range(1, 20)]
        invalid_row = [(None, 1, 100, "2025-01-15")]
        df = spark.createDataFrame(
            valid_rows + invalid_row,
            "product_id long, store_id long, stock_qty int, snapshot_date string",
        )

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()
        cfg = _make_config()

        result = run_silver_inventory(
            run_date="2025-01-15",
            spark=spark,
            config=cfg,
            client=mock_client,
            conn=mock_conn,
            source_df=df,
        )

        assert result["status"] == "SUCCESS"
        assert result["total_records"] == 20
        assert result["accepted_records"] == 19
        assert result["rejected_records"] == 1
        assert result["silver_records"] == 19

        mock_quarantine.assert_called_once()
        mock_summary.assert_called_once()
        mock_write_parquet.assert_called_once()
        silver_call_path = mock_write_parquet.call_args[0][2]
        assert "silver/inventory/dt=2025-01-15/" in silver_call_path

    @mock.patch("retailpulse.transform.silver_inventory.write_parquet")
    @mock.patch("retailpulse.transform.silver_inventory.write_quarantine")
    @mock.patch("retailpulse.transform.silver_inventory.write_quality_summary")
    def test_quality_gate_failure_aborts_silver_write(
        self,
        mock_summary: mock.MagicMock,
        mock_quarantine: mock.MagicMock,
        mock_write_parquet: mock.MagicMock,
        spark: SparkSession,
    ) -> None:
        """When reject rate exceeds 5.0%, QualityGateError is raised and Silver write is aborted."""
        # 10 rows: 8 valid, 2 invalid (negative stock) -> 20% reject rate > 5%
        data = [
            (1, 1, 10, "2025-01-15"),
            (2, 1, -5, "2025-01-15"),  # invalid
            (3, 1, -10, "2025-01-15"), # invalid
        ]
        df = spark.createDataFrame(data, ["product_id", "store_id", "stock_qty", "snapshot_date"])

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()
        cfg = _make_config()

        with pytest.raises(QualityGateError):
            run_silver_inventory(
                run_date="2025-01-15",
                spark=spark,
                config=cfg,
                client=mock_client,
                conn=mock_conn,
                source_df=df,
            )

        mock_quarantine.assert_called_once()
        mock_summary.assert_called_once()
        mock_write_parquet.assert_not_called()

    @mock.patch("retailpulse.transform.silver_inventory.run_silver_inventory")
    def test_main_entrypoint(self, mock_run: mock.MagicMock) -> None:
        """Verifies main entrypoint callable."""
        mock_run.return_value = {"status": "SUCCESS"}
        inventory_main("2025-01-15")
        mock_run.assert_called_once_with(run_date="2025-01-15")
