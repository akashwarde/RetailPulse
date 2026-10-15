"""Unit tests for quarantine writing, quality summary reporting, and quality gate."""

import json
from pathlib import Path
from unittest import mock
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig
from retailpulse.validation import (
    QualityGateError,
    QualitySummary,
    calculate_quality_summary,
    quality_gate,
    write_quality_summary,
    write_quarantine,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession fixture for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("RetailPulseQuarantineTests")
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


class TestCalculateQualitySummary:
    """Tests for calculate_quality_summary()."""

    def test_accurate_metrics_and_rule_breakdown(self, spark: SparkSession) -> None:
        """Calculates accurate total, accepted, rejected, reject_pct, and rule counts."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("rule_failed", StringType(), True),
            StructField("reason", StringType(), True),
        ])
        data = [
            (1, "not_null:id", "id is null"),
            (2, "range:qty", "qty is negative"),
            (3, "not_null:id, range:qty", "id is null; qty is negative"),
        ]
        bad_df = spark.createDataFrame(data, schema)

        summary = calculate_quality_summary(
            bad_df=bad_df,
            total=100,
            source="sales",
            batch_id="batch_001",
        )

        assert summary.source == "sales"
        assert summary.batch_id == "batch_001"
        assert summary.total == 100
        assert summary.rejected == 3
        assert summary.accepted == 97
        assert summary.reject_pct == 3.0
        assert summary.per_rule_counts == {
            "not_null:id": 2,
            "range:qty": 2,
        }

    def test_zero_rejected_records(self, spark: SparkSession) -> None:
        """When bad_df is empty, rejected count is 0 and reject_pct is 0.0."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("rule_failed", StringType(), True),
            StructField("reason", StringType(), True),
        ])
        bad_df = spark.createDataFrame([], schema)

        summary = calculate_quality_summary(
            bad_df=bad_df,
            total=50,
            source="inventory",
            batch_id="batch_002",
        )

        assert summary.total == 50
        assert summary.rejected == 0
        assert summary.accepted == 50
        assert summary.reject_pct == 0.0
        assert summary.per_rule_counts == {}

    def test_zero_total_records(self, spark: SparkSession) -> None:
        """When total input records is 0, reject_pct should be 0.0 without ZeroDivisionError."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("rule_failed", StringType(), True),
            StructField("reason", StringType(), True),
        ])
        bad_df = spark.createDataFrame([], schema)

        summary = calculate_quality_summary(
            bad_df=bad_df,
            total=0,
            source="customers",
            batch_id="batch_003",
        )

        assert summary.total == 0
        assert summary.rejected == 0
        assert summary.accepted == 0
        assert summary.reject_pct == 0.0


class TestWriteQuarantine:
    """Tests for write_quarantine()."""

    @mock.patch("retailpulse.validation.quarantine.write_parquet")
    def test_writes_to_minio_and_postgres(
        self,
        mock_write_parquet: mock.MagicMock,
        spark: SparkSession,
    ) -> None:
        """Writes invalid records to MinIO lake and inserts raw JSON into dq.quarantine."""
        schema = StructType([
            StructField("order_id", IntegerType(), True),
            StructField("amount", IntegerType(), True),
            StructField("rule_failed", StringType(), True),
            StructField("reason", StringType(), True),
        ])
        data = [
            (101, -10, "range:amount", "amount is negative"),
            (102, -20, "range:amount", "amount is negative"),
        ]
        bad_df = spark.createDataFrame(data, schema)

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.closed = False
        mock_conn.autocommit = False

        cfg = _make_config()

        written_count = write_quarantine(
            bad_df=bad_df,
            source="sales",
            batch_id="batch_101",
            dt="2025-01-15",
            client=mock_client,
            conn=mock_conn,
            config=cfg,
        )

        assert written_count == 2

        # Verify MinIO write_parquet
        mock_write_parquet.assert_called_once()
        call_args = mock_write_parquet.call_args[0]
        assert call_args[0] is mock_client
        assert call_args[1] == "testbucket"
        assert "quarantine/sales/dt=2025-01-15/" in call_args[2]

        # Verify PostgreSQL insertion
        mock_cursor.executemany.assert_called_once()
        sql, inserted_rows = mock_cursor.executemany.call_args[0]
        assert "INSERT INTO dq.quarantine" in sql
        assert len(inserted_rows) == 2

        row1 = inserted_rows[0]
        assert row1[0] == "sales"
        assert row1[1] == "range:amount"
        assert row1[2] == "amount is negative"
        assert row1[3] == "batch_101"

        raw_json = json.loads(row1[4])
        assert raw_json == {"order_id": 101, "amount": -10}

        mock_conn.commit.assert_called_once()

    def test_write_quarantine_empty_does_nothing(self, spark: SparkSession) -> None:
        """When bad_df has zero records, no external writes occur."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("rule_failed", StringType(), True),
            StructField("reason", StringType(), True),
        ])
        bad_df = spark.createDataFrame([], schema)

        mock_client = mock.MagicMock()
        mock_conn = mock.MagicMock()

        written = write_quarantine(
            bad_df=bad_df,
            source="sales",
            batch_id="batch_000",
            client=mock_client,
            conn=mock_conn,
        )

        assert written == 0
        mock_client.put_object.assert_not_called()
        mock_conn.cursor.assert_not_called()


class TestWriteQualitySummary:
    """Tests for write_quality_summary()."""

    def test_persists_summary_to_postgres(self) -> None:
        """Inserts summary row with accurate metrics and rule breakdown into dq.summary."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.closed = False
        mock_conn.autocommit = False

        cfg = _make_config()

        summary = write_quality_summary(
            source="sales",
            batch_id="batch_202",
            total=1000,
            accepted=960,
            rejected=40,
            per_rule_counts={"not_null:customer_id": 30, "range:quantity": 10},
            conn=mock_conn,
            config=cfg,
        )

        assert summary.reject_pct == 4.0
        assert summary.accepted == 960
        assert summary.rejected == 40

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "INSERT INTO dq.summary" in sql
        assert params[0] == "sales"
        assert params[1] == "batch_202"
        assert params[2] == 1000
        assert params[3] == 960
        assert params[4] == 40
        assert params[5] == 4.0
        assert json.loads(params[6]) == {
            "not_null:customer_id": 30,
            "range:quantity": 10,
        }
        mock_conn.commit.assert_called_once()


class TestQualityGate:
    """Tests for quality_gate()."""

    def test_passes_when_reject_pct_under_threshold(self) -> None:
        """Rejection rate strictly lower than threshold must pass."""
        summary = QualitySummary(
            source="sales",
            batch_id="batch_1",
            total=100,
            accepted=96,
            rejected=4,
            reject_pct=4.0,
            per_rule_counts={},
        )
        assert quality_gate(summary, threshold=5.0) is True

    def test_passes_when_reject_pct_equals_threshold(self) -> None:
        """Rejection rate equal to threshold must pass."""
        summary = QualitySummary(
            source="sales",
            batch_id="batch_2",
            total=100,
            accepted=95,
            rejected=5,
            reject_pct=5.0,
            per_rule_counts={},
        )
        assert quality_gate(summary, threshold=5.0) is True

    def test_raises_when_reject_pct_exceeds_threshold(self) -> None:
        """Rejection rate strictly exceeding threshold must raise QualityGateError."""
        summary = QualitySummary(
            source="sales",
            batch_id="batch_3",
            total=100,
            accepted=94,
            rejected=6,
            reject_pct=6.0,
            per_rule_counts={"not_null:id": 6},
        )
        with pytest.raises(QualityGateError) as exc_info:
            quality_gate(summary, threshold=5.0)

        err_msg = str(exc_info.value)
        assert "Quality gate FAILED" in err_msg
        assert "reject_pct=6.00% exceeds threshold=5.00%" in err_msg
        assert "sales" in err_msg
        assert "batch_3" in err_msg
        assert exc_info.value.threshold == 5.0

    def test_dict_input_support(self) -> None:
        """Accepts dictionary representation of quality summary."""
        valid_dict = {
            "source": "inventory",
            "batch_id": "b_inv",
            "total": 200,
            "rejected": 4,
            "reject_pct": 2.0,
        }
        assert quality_gate(valid_dict, threshold=5.0) is True

        failing_dict = {
            "source": "inventory",
            "batch_id": "b_inv_fail",
            "total": 100,
            "rejected": 10,
            "reject_pct": 10.0,
        }
        with pytest.raises(QualityGateError):
            quality_gate(failing_dict, threshold=5.0)


class TestDQSchemaDDL:
    """Tests verifying the SQL DDL definition."""

    def test_ddl_file_contains_required_tables_and_indexes(self) -> None:
        """Verify 02_dq_schema.sql defines dq schema, tables, and indexes."""
        ddl_path = Path("sql/ddl/02_dq_schema.sql")
        assert ddl_path.exists(), "02_dq_schema.sql must exist"

        sql_text = ddl_path.read_text(encoding="utf-8")
        assert "CREATE SCHEMA IF NOT EXISTS dq;" in sql_text
        assert "CREATE TABLE IF NOT EXISTS dq.quarantine" in sql_text
        assert "CREATE TABLE IF NOT EXISTS dq.summary" in sql_text
        assert "raw_record JSONB" in sql_text
        assert "per_rule_counts JSONB" in sql_text
        assert "idx_dq_quarantine_batch" in sql_text
        assert "idx_dq_summary_batch" in sql_text
