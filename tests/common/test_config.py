"""Tests for retailpulse.common.config."""

import os
from unittest import mock

import pytest

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig, RedpandaConfig, get_config

# Minimal valid environment for get_config() to succeed.
_VALID_ENV = {
    "POSTGRES_PASSWORD": "testpass",
    "MINIO_ROOT_PASSWORD": "miniopass",
}


class TestGetConfig:
    """Tests for the get_config() loader."""

    @mock.patch.dict(os.environ, _VALID_ENV, clear=True)
    def test_loads_defaults(self) -> None:
        cfg = get_config()
        assert isinstance(cfg, AppConfig)
        assert cfg.postgres.host == "localhost"
        assert cfg.postgres.port == 5432
        assert cfg.postgres.user == "retail"
        assert cfg.postgres.source_db == "source_db"
        assert cfg.postgres.warehouse_db == "warehouse_db"
        assert cfg.minio.endpoint == "localhost:9000"
        assert cfg.minio.bucket == "retailpulse"
        assert cfg.minio.secure is False
        assert cfg.redpanda.broker == "localhost:19092"
        assert cfg.redpanda.container_broker == "redpanda:9092"
        assert cfg.redpanda.orders_topic == "orders_stream"
        assert cfg.redpanda.dlq_topic == "orders_dlq"
        assert cfg.redpanda.producer_rate_per_sec == 5.0
        assert cfg.redpanda.producer_invalid_rate == 0.03
        assert cfg.redpanda.producer_late_rate == 0.02
        assert cfg.redpanda.get_broker(in_container=False) == "localhost:19092"
        assert cfg.redpanda.get_broker(in_container=True) == "redpanda:9092"
        assert cfg.log_level == "INFO"

    @mock.patch.dict(
        os.environ,
        {
            **_VALID_ENV,
            "POSTGRES_HOST": "dbhost",
            "POSTGRES_PORT": "5433",
            "POSTGRES_USER": "admin",
            "SOURCE_DB": "mydb",
            "WAREHOUSE_DB": "wh",
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ROOT_USER": "myadmin",
            "MINIO_BUCKET": "mybucket",
            "MINIO_SECURE": "true",
            "REDPANDA_BROKER": "custom-broker:19092",
            "REDPANDA_CONTAINER_BROKER": "custom-container:9092",
            "REDPANDA_ORDERS_TOPIC": "custom_orders",
            "REDPANDA_DLQ_TOPIC": "custom_dlq",
            "PRODUCER_RATE_PER_SEC": "12.5",
            "PRODUCER_INVALID_RATE": "0.05",
            "PRODUCER_LATE_RATE": "0.04",
            "LOG_LEVEL": "debug",
        },
        clear=True,
    )
    def test_reads_all_env_vars(self) -> None:
        cfg = get_config()
        assert cfg.postgres.host == "dbhost"
        assert cfg.postgres.port == 5433
        assert cfg.postgres.user == "admin"
        assert cfg.postgres.source_db == "mydb"
        assert cfg.postgres.warehouse_db == "wh"
        assert cfg.minio.endpoint == "minio:9000"
        assert cfg.minio.access_key == "myadmin"
        assert cfg.minio.bucket == "mybucket"
        assert cfg.minio.secure is True
        assert cfg.redpanda.broker == "custom-broker:19092"
        assert cfg.redpanda.container_broker == "custom-container:9092"
        assert cfg.redpanda.orders_topic == "custom_orders"
        assert cfg.redpanda.dlq_topic == "custom_dlq"
        assert cfg.redpanda.producer_rate_per_sec == 12.5
        assert cfg.redpanda.producer_invalid_rate == 0.05
        assert cfg.redpanda.producer_late_rate == 0.04
        assert cfg.log_level == "DEBUG"

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_raises_on_missing_postgres_password(self) -> None:
        with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
            get_config()

    @mock.patch.dict(os.environ, {"POSTGRES_PASSWORD": "p"}, clear=True)
    def test_raises_on_missing_minio_password(self) -> None:
        with pytest.raises(ValueError, match="MINIO_ROOT_PASSWORD"):
            get_config()

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_raises_lists_all_missing(self) -> None:
        with pytest.raises(ValueError, match="POSTGRES_PASSWORD.*MINIO_ROOT_PASSWORD"):
            get_config()


class TestPostgresDsn:
    """Tests for PostgresConfig.dsn()."""

    def _make(self) -> PostgresConfig:
        return PostgresConfig(
            host="h", port=5432, user="u", password="p",
            source_db="src", warehouse_db="wh",
        )

    def test_source_dsn(self) -> None:
        dsn = self._make().dsn("source")
        assert "dbname=src" in dsn
        assert "host=h" in dsn
        assert "user=u" in dsn

    def test_warehouse_dsn(self) -> None:
        dsn = self._make().dsn("warehouse")
        assert "dbname=wh" in dsn

    def test_invalid_database_raises(self) -> None:
        with pytest.raises(ValueError, match="must be 'source' or 'warehouse'"):
            self._make().dsn("invalid")


class TestRedpandaProducerConfig:
    """Tests for RedpandaConfig producer settings and rate validations."""

    def test_default_producer_settings(self) -> None:
        cfg = RedpandaConfig()
        assert cfg.producer_rate_per_sec == 5.0
        assert cfg.producer_invalid_rate == 0.03
        assert cfg.producer_late_rate == 0.02

    def test_non_positive_rate_raises(self) -> None:
        with pytest.raises(ValueError, match="producer_rate_per_sec must be positive"):
            RedpandaConfig(producer_rate_per_sec=0.0)

        with pytest.raises(ValueError, match="producer_rate_per_sec must be positive"):
            RedpandaConfig(producer_rate_per_sec=-1.5)

    def test_invalid_defect_rates_raise(self) -> None:
        with pytest.raises(ValueError, match="producer_invalid_rate must be between 0.0 and 1.0"):
            RedpandaConfig(producer_invalid_rate=-0.05)

        with pytest.raises(ValueError, match="producer_invalid_rate must be between 0.0 and 1.0"):
            RedpandaConfig(producer_invalid_rate=1.2)

        with pytest.raises(ValueError, match="producer_late_rate must be between 0.0 and 1.0"):
            RedpandaConfig(producer_late_rate=-0.01)

        with pytest.raises(ValueError, match="producer_late_rate must be between 0.0 and 1.0"):
            RedpandaConfig(producer_late_rate=1.01)

    def test_combined_rates_exceed_one_raises(self) -> None:
        with pytest.raises(ValueError, match="rates exceed 1.0"):
            RedpandaConfig(producer_invalid_rate=0.6, producer_late_rate=0.5)

    @mock.patch.dict(
        os.environ,
        {**_VALID_ENV, "PRODUCER_RATE_PER_SEC": "not-a-float"},
        clear=True,
    )
    def test_get_config_invalid_rate_str_raises(self) -> None:
        with pytest.raises(ValueError, match="PRODUCER_RATE_PER_SEC must be a float"):
            get_config()

    @mock.patch.dict(
        os.environ,
        {**_VALID_ENV, "PRODUCER_INVALID_RATE": "bad-float"},
        clear=True,
    )
    def test_get_config_invalid_invalid_rate_str_raises(self) -> None:
        with pytest.raises(ValueError, match="PRODUCER_INVALID_RATE must be a float"):
            get_config()

    @mock.patch.dict(
        os.environ,
        {**_VALID_ENV, "PRODUCER_LATE_RATE": "bad-float"},
        clear=True,
    )
    def test_get_config_invalid_late_rate_str_raises(self) -> None:
        with pytest.raises(ValueError, match="PRODUCER_LATE_RATE must be a float"):
            get_config()

