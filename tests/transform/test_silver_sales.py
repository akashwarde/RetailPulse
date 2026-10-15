"""Unit tests for PySpark Silver sales transformation pipeline."""

from datetime import date, datetime, timedelta
from unittest import mock
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig
from retailpulse.transform import (
    SILVER_SALES_COLUMNS,
    main,
    run_silver_sales,
    transform_sales,
)
from retailpulse.validation import QualityGateError


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession fixture for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("RetailPulseSilverSalesTests")
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


class TestTransformSales:
    """Tests for the pure DataFrame transformation function transform_sales()."""

    def test_schema_types_and_normalizations(self, spark: SparkSession) -> None:
        """Verifies type casting, string trimming, uppercase formatting, and total amount calculation."""
        ts = datetime(2025, 1, 15, 14, 30, 0)
        data = [
            (101, 1001, 1, 10, 501, 3, 49.99, ts, " completed ", " card "),
            (102, 1002, 2, 11, 502, 1, 150.00, ts, "CANCELLED", "upi"),
        ]
        cols = [
            "order_id", "order_item_id", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "order_timestamp", "order_status", "payment_method",
        ]
        df = spark.createDataFrame(data, cols)

        silver_df = transform_sales(df)

        assert silver_df.columns == SILVER_SALES_COLUMNS
        assert silver_df.count() == 2

        # Check types
        schema_dict = {f.name: f.dataType for f in silver_df.schema.fields}
        assert isinstance(schema_dict["order_id"], LongType)
        assert isinstance(schema_dict["order_item_id"], LongType)
        assert isinstance(schema_dict["customer_id"], LongType)
        assert isinstance(schema_dict["store_id"], LongType)
        assert isinstance(schema_dict["product_id"], LongType)
        assert isinstance(schema_dict["quantity"], IntegerType)
        assert isinstance(schema_dict["unit_price"], DoubleType)
        assert isinstance(schema_dict["total_amount"], DoubleType)
        assert isinstance(schema_dict["order_timestamp"], TimestampType)
        assert isinstance(schema_dict["order_date"], DateType)
        assert isinstance(schema_dict["order_status"], StringType)
        assert isinstance(schema_dict["payment_method"], StringType)
        assert isinstance(schema_dict["_processed_at"], TimestampType)

        # Check values
        rows = {r["order_id"]: r for r in silver_df.collect()}
        row1 = rows[101]
        assert row1["order_status"] == "COMPLETED"
        assert row1["payment_method"] == "CARD"
        assert row1["total_amount"] == round(3 * 49.99, 2)
        assert row1["order_date"] == date(2025, 1, 15)
        assert row1["_processed_at"] is not None

    def test_deduplication_keeps_latest_timestamp(self, spark: SparkSession) -> None:
        """Deduplication on business keys preserves the record with the latest order_timestamp."""
        earlier_ts = datetime(2025, 1, 15, 10, 0, 0)
        later_ts = datetime(2025, 1, 15, 10, 30, 0)

        data = [
            (201, 2001, 5, 1, 10, 1, 99.0, earlier_ts, "PENDING", "CASH"),
            (201, 2001, 5, 1, 10, 1, 99.0, later_ts, "COMPLETED", "CARD"),
        ]
        cols = [
            "order_id", "order_item_id", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "order_timestamp", "order_status", "payment_method",
        ]
        df = spark.createDataFrame(data, cols)

        silver_df = transform_sales(df)

        assert silver_df.count() == 1
        record = silver_df.collect()[0]
        assert record["order_id"] == 201
        assert record["order_item_id"] == 2001
        assert record["order_status"] == "COMPLETED"
        assert record["payment_method"] == "CARD"
        assert record["order_timestamp"] == later_ts

    def test_empty_dataframe(self, spark: SparkSession) -> None:
        """Empty input DataFrame produces empty Silver DataFrame matching expected schema."""
        data = []
        cols = [
            "order_id", "order_item_id", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "order_timestamp", "order_status", "payment_method",
        ]
        # Create empty DataFrame with basic string schema
        schema = spark.createDataFrame([], "order_id long").schema
        for c in cols[1:]:
            schema = schema.add(c, "string")
        df = spark.createDataFrame([], schema)

        silver_df = transform_sales(df)

        assert silver_df.count() == 0
        assert silver_df.columns == SILVER_SALES_COLUMNS


class TestRunSilverSales:
    """Tests for the end-to-end run_silver_sales() pipeline."""

    @mock.patch("retailpulse.transform.silver_sales.write_parquet")
    @mock.patch("retailpulse.transform.silver_sales.write_quarantine")
    @mock.patch("retailpulse.transform.silver_sales.write_quality_summary")
    def test_end_to_end_successful_run(
        self,
        mock_summary: mock.MagicMock,
        mock_quarantine: mock.MagicMock,
        mock_write_parquet: mock.MagicMock,
        spark: SparkSession,
    ) -> None:
        """End-to-end run with valid and invalid rows completes quarantine, summary, and Silver write."""
        now = datetime.now() - timedelta(days=1)
        # 10 records: 9 valid, 1 invalid (null customer_id) -> 10% invalid? No, 1/20 is 5%
        # Let's use 20 records: 19 valid, 1 invalid (null customer_id) -> 5.0% reject rate (passes <= 5%)
        valid_rows = [
            (i, i * 10, i, 1, 100, 1, 50.0, now, "COMPLETED", "CARD")
            for i in range(1, 20)
        ]
        invalid_row = [(99, 990, None, 1, 100, 1, 50.0, now, "COMPLETED", "CARD")]
        cols = [
            "order_id", "order_item_id", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "order_timestamp", "order_status", "payment_method",
        ]
        df = spark.createDataFrame(valid_rows + invalid_row, cols)

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()
        cfg = _make_config()

        result = run_silver_sales(
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

        # Verify quarantine called
        mock_quarantine.assert_called_once()
        # Verify quality summary recorded
        mock_summary.assert_called_once()
        # Verify Silver Parquet written
        mock_write_parquet.assert_called_once()
        silver_call_path = mock_write_parquet.call_args[0][2]
        assert "silver/sales/dt=2025-01-15/" in silver_call_path

    @mock.patch("retailpulse.transform.silver_sales.write_parquet")
    @mock.patch("retailpulse.transform.silver_sales.write_quarantine")
    @mock.patch("retailpulse.transform.silver_sales.write_quality_summary")
    def test_quality_gate_failure_aborts_silver_write(
        self,
        mock_summary: mock.MagicMock,
        mock_quarantine: mock.MagicMock,
        mock_write_parquet: mock.MagicMock,
        spark: SparkSession,
    ) -> None:
        """When reject rate exceeds 5.0%, QualityGateError is raised and Silver write is aborted."""
        now = datetime.now() - timedelta(days=1)
        # 10 records: 8 valid, 2 invalid (20% reject rate > 5.0%)
        data = [
            (1, 10, 1, 1, 10, 1, 50.0, now, "COMPLETED", "CARD"),
            (2, 20, None, 1, 10, 1, 50.0, now, "COMPLETED", "CARD"),  # invalid
            (3, 30, None, 1, 10, 1, 50.0, now, "COMPLETED", "CARD"),  # invalid
        ]
        cols = [
            "order_id", "order_item_id", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "order_timestamp", "order_status", "payment_method",
        ]
        df = spark.createDataFrame(data, cols)

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()
        cfg = _make_config()

        with pytest.raises(QualityGateError):
            run_silver_sales(
                run_date="2025-01-15",
                spark=spark,
                config=cfg,
                client=mock_client,
                conn=mock_conn,
                source_df=df,
            )

        # Quarantine and summary are written, but Silver write must NOT occur
        mock_quarantine.assert_called_once()
        mock_summary.assert_called_once()
        mock_write_parquet.assert_not_called()

    @mock.patch("retailpulse.transform.silver_sales.run_silver_sales")
    def test_main_entrypoint(self, mock_run: mock.MagicMock) -> None:
        """Verifies main() entrypoint function for Airflow DAGs."""
        mock_run.return_value = {"status": "SUCCESS"}
        main("2025-01-15")
        mock_run.assert_called_once_with(run_date="2025-01-15")
