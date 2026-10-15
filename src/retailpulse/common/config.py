"""Application configuration loader for RetailPulse."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Union
from dotenv import load_dotenv

# Load .env file at import time if present
load_dotenv()


@dataclass(frozen=True)
class PostgresConfig:
    """PostgreSQL connection configuration."""

    host: str = "localhost"
    port: int = 5432
    user: str = "retail"
    password: str = ""
    source_db: str = "source_db"
    warehouse_db: str = "warehouse_db"

    def dsn(self, database: str = "source") -> str:
        """Build a PostgreSQL DSN connection string.

        Args:
            database: Target database name ('source' or 'warehouse').

        Returns:
            DSN string suitable for psycopg2.connect.

        Raises:
            ValueError: If database is not 'source' or 'warehouse'.
        """
        if database == "source":
            dbname = self.source_db
        elif database == "warehouse":
            dbname = self.warehouse_db
        else:
            raise ValueError(
                f"database must be 'source' or 'warehouse', got '{database}'"
            )

        from psycopg2.extensions import make_dsn
        return make_dsn(dbname=dbname, host=self.host, port=self.port,
                        user=self.user, password=self.password)


@dataclass(frozen=True)
class MinIOConfig:
    """MinIO (S3-compatible) storage configuration."""

    endpoint: str = "localhost:9000"
    access_key: str = "retailadmin"
    secret_key: str = ""
    bucket: str = "retailpulse"
    secure: bool = False


@dataclass(frozen=True)
class RedpandaConfig:
    """Redpanda / Kafka streaming configuration."""

    broker: str = "localhost:19092"
    container_broker: str = "redpanda:9092"
    orders_topic: str = "orders_stream"
    dlq_topic: str = "orders_dlq"
    producer_rate_per_sec: float = 5.0
    producer_invalid_rate: float = 0.03
    producer_late_rate: float = 0.02

    def __post_init__(self) -> None:
        if self.producer_rate_per_sec <= 0:
            raise ValueError(
                f"producer_rate_per_sec must be positive, got {self.producer_rate_per_sec}"
            )
        if self.producer_invalid_rate < 0 or self.producer_invalid_rate > 1:
            raise ValueError(
                f"producer_invalid_rate must be between 0.0 and 1.0, got {self.producer_invalid_rate}"
            )
        if self.producer_late_rate < 0 or self.producer_late_rate > 1:
            raise ValueError(
                f"producer_late_rate must be between 0.0 and 1.0, got {self.producer_late_rate}"
            )
        if self.producer_invalid_rate + self.producer_late_rate > 1:
            raise ValueError(
                f"Combined invalid ({self.producer_invalid_rate}) and late ({self.producer_late_rate}) "
                f"rates exceed 1.0"
            )

    def get_broker(self, in_container: bool = False) -> str:
        """Return the appropriate broker address based on execution environment.

        Args:
            in_container: True if running inside a Docker container, False if on host.

        Returns:
            str: Broker address (host:port).
        """
        return self.container_broker if in_container else self.broker


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration container."""

    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    redpanda: RedpandaConfig = field(default_factory=RedpandaConfig)
    log_level: str = "INFO"


def get_config(env_file: Optional[Union[str, Path]] = None) -> AppConfig:
    """Load application configuration from environment variables.

    Args:
        env_file: Optional path to a specific .env file to load.

    Returns:
        AppConfig: Immutable configuration object.

    Raises:
        ValueError: If required environment variables are missing.
    """
    if env_file is not None:
        load_dotenv(dotenv_path=env_file, override=True)

    missing: List[str] = []
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    if not postgres_password:
        missing.append("POSTGRES_PASSWORD")

    minio_root_password = os.getenv("MINIO_ROOT_PASSWORD")
    if not minio_root_password:
        missing.append("MINIO_ROOT_PASSWORD")

    if missing:
        raise ValueError(
            f"Missing required environment variable(s): {', '.join(missing)}"
        )

    # Postgres settings
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user = os.getenv("POSTGRES_USER", "retail")
    source_db = os.getenv("SOURCE_DB", "source_db")
    warehouse_db = os.getenv("WAREHOUSE_DB", "warehouse_db")

    postgres_cfg = PostgresConfig(
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password,
        source_db=source_db,
        warehouse_db=warehouse_db,
    )

    # MinIO settings
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_root_user = os.getenv("MINIO_ROOT_USER", "retailadmin")
    minio_bucket = os.getenv("MINIO_BUCKET", "retailpulse")
    minio_secure_raw = os.getenv("MINIO_SECURE", "false").strip().lower()
    minio_secure = minio_secure_raw in ("true", "1", "yes", "t", "y")

    minio_cfg = MinIOConfig(
        endpoint=minio_endpoint,
        access_key=minio_root_user,
        secret_key=minio_root_password,
        bucket=minio_bucket,
        secure=minio_secure,
    )

    # Redpanda settings
    redpanda_broker = os.getenv("REDPANDA_BROKER", "localhost:19092")
    redpanda_container_broker = os.getenv("REDPANDA_CONTAINER_BROKER", "redpanda:9092")
    redpanda_orders_topic = os.getenv("REDPANDA_ORDERS_TOPIC", "orders_stream")
    redpanda_dlq_topic = os.getenv("REDPANDA_DLQ_TOPIC", "orders_dlq")

    try:
        producer_rate = float(os.getenv("PRODUCER_RATE_PER_SEC", "5.0"))
    except ValueError as exc:
        raise ValueError(f"PRODUCER_RATE_PER_SEC must be a float: {exc}") from exc

    try:
        producer_invalid_rate = float(os.getenv("PRODUCER_INVALID_RATE", "0.03"))
    except ValueError as exc:
        raise ValueError(f"PRODUCER_INVALID_RATE must be a float: {exc}") from exc

    try:
        producer_late_rate = float(os.getenv("PRODUCER_LATE_RATE", "0.02"))
    except ValueError as exc:
        raise ValueError(f"PRODUCER_LATE_RATE must be a float: {exc}") from exc

    redpanda_cfg = RedpandaConfig(
        broker=redpanda_broker,
        container_broker=redpanda_container_broker,
        orders_topic=redpanda_orders_topic,
        dlq_topic=redpanda_dlq_topic,
        producer_rate_per_sec=producer_rate,
        producer_invalid_rate=producer_invalid_rate,
        producer_late_rate=producer_late_rate,
    )

    # Logging settings
    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()

    return AppConfig(
        postgres=postgres_cfg,
        minio=minio_cfg,
        redpanda=redpanda_cfg,
        log_level=log_level,
    )
