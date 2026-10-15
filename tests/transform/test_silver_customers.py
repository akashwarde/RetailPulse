"""Unit tests for PySpark Silver customer transformation pipeline."""

from datetime import date
from unittest import mock
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    TimestampType,
)

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig
from retailpulse.transform import (
    SILVER_CUSTOMERS_COLUMNS,
    customers_main,
    run_silver_customers,
    transform_customers,
)
from retailpulse.validation import QualityGateError


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession fixture for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("RetailPulseSilverCustomersTests")
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


class TestTransformCustomers:
    """Tests for the pure DataFrame transformation function transform_customers()."""

    def test_schema_types_and_normalizations(self, spark: SparkSession) -> None:
        """Verifies type casting, name/city title-casing, email lowercasing, and date parsing."""
        data = [
            (101, " aarav ", " sharma ", "AARAV@EXAMPLE.COM", " mumbai ", " maharashtra ", "2024-05-10"),
            (102, "DIYA", "PATEL", "diya.patel@RETAIL.in", "pune", "maharashtra", "10/06/2024"),
        ]
        cols = ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"]
        df = spark.createDataFrame(data, cols)

        silver_df = transform_customers(df)

        assert silver_df.columns == SILVER_CUSTOMERS_COLUMNS
        assert silver_df.count() == 2

        schema_dict = {f.name: f.dataType for f in silver_df.schema.fields}
        assert isinstance(schema_dict["customer_id"], LongType)
        assert isinstance(schema_dict["first_name"], StringType)
        assert isinstance(schema_dict["last_name"], StringType)
        assert isinstance(schema_dict["email"], StringType)
        assert isinstance(schema_dict["city"], StringType)
        assert isinstance(schema_dict["state"], StringType)
        assert isinstance(schema_dict["signup_date"], DateType)
        assert isinstance(schema_dict["_processed_at"], TimestampType)

        rows = {r["customer_id"]: r for r in silver_df.collect()}
        row1 = rows[101]
        assert row1["first_name"] == "Aarav"
        assert row1["last_name"] == "Sharma"
        assert row1["email"] == "aarav@example.com"
        assert row1["city"] == "Mumbai"
        assert row1["state"] == "Maharashtra"
        assert row1["signup_date"] == date(2024, 5, 10)
        assert row1["_processed_at"] is not None

        row2 = rows[102]
        assert row2["first_name"] == "Diya"
        assert row2["email"] == "diya.patel@retail.in"
        assert row2["signup_date"] == date(2024, 6, 10)

    def test_deduplication_keeps_latest_signup(self, spark: SparkSession) -> None:
        """Deduplication on customer_id retains record with the latest signup_date."""
        data = [
            (201, "Isha", "Rao", "isha@example.com", "Hyderabad", "Telangana", "2024-01-01"),
            (201, "Isha", "Rao", "isha@example.com", "Hyderabad", "Telangana", "2024-08-15"),
        ]
        cols = ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"]
        df = spark.createDataFrame(data, cols)

        silver_df = transform_customers(df)

        assert silver_df.count() == 1
        record = silver_df.collect()[0]
        assert record["customer_id"] == 201
        assert record["signup_date"] == date(2024, 8, 15)

    def test_empty_dataframe(self, spark: SparkSession) -> None:
        """Empty input DataFrame produces empty Silver DataFrame with correct schema."""
        cols = ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"]
        schema = spark.createDataFrame([], "customer_id long").schema
        for c in cols[1:]:
            schema = schema.add(c, "string")
        df = spark.createDataFrame([], schema)

        silver_df = transform_customers(df)

        assert silver_df.count() == 0
        assert silver_df.columns == SILVER_CUSTOMERS_COLUMNS


class TestRunSilverCustomers:
    """Tests for the end-to-end run_silver_customers() pipeline."""

    @mock.patch("retailpulse.transform.silver_customers.write_parquet")
    @mock.patch("retailpulse.transform.silver_customers.write_quarantine")
    @mock.patch("retailpulse.transform.silver_customers.write_quality_summary")
    def test_end_to_end_successful_run(
        self,
        mock_summary: mock.MagicMock,
        mock_quarantine: mock.MagicMock,
        mock_write_parquet: mock.MagicMock,
        spark: SparkSession,
    ) -> None:
        """End-to-end execution validates, quarantines bad rows, writes summary, and saves Silver."""
        # 20 rows: 19 valid, 1 invalid (bad email) -> 5.0% reject rate <= 5%
        valid_rows = [
            (i, f"Name{i}", "Last", f"user{i}@example.com", "City", "State", "2024-01-01")
            for i in range(1, 20)
        ]
        invalid_row = [(99, "Bad", "User", "not-an-email", "City", "State", "2024-01-01")]
        cols = ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"]
        df = spark.createDataFrame(valid_rows + invalid_row, cols)

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()
        cfg = _make_config()

        result = run_silver_customers(
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
        assert "silver/customers/dt=2025-01-15/" in silver_call_path

    @mock.patch("retailpulse.transform.silver_customers.write_parquet")
    @mock.patch("retailpulse.transform.silver_customers.write_quarantine")
    @mock.patch("retailpulse.transform.silver_customers.write_quality_summary")
    def test_quality_gate_failure_aborts_silver_write(
        self,
        mock_summary: mock.MagicMock,
        mock_quarantine: mock.MagicMock,
        mock_write_parquet: mock.MagicMock,
        spark: SparkSession,
    ) -> None:
        """When reject rate exceeds 5.0%, QualityGateError is raised and Silver write is aborted."""
        # 3 rows: 1 valid, 2 invalid (bad email, null city) -> 66% reject rate > 5%
        data = [
            (1, "Aarav", "Sharma", "aarav@example.com", "Mumbai", "Maharashtra", "2024-01-01"),
            (2, "Diya", "Patel", "bad-email", "Pune", "Maharashtra", "2024-01-01"),  # invalid email
            (3, "Isha", "Rao", "isha@example.com", None, "Telangana", "2024-01-01"),    # null city
        ]
        cols = ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"]
        df = spark.createDataFrame(data, cols)

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()
        cfg = _make_config()

        with pytest.raises(QualityGateError):
            run_silver_customers(
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

    @mock.patch("retailpulse.transform.silver_customers.run_silver_customers")
    def test_main_entrypoint(self, mock_run: mock.MagicMock) -> None:
        """Verifies main entrypoint callable."""
        mock_run.return_value = {"status": "SUCCESS"}
        customers_main("2025-01-15")
        mock_run.assert_called_once_with(run_date="2025-01-15")
