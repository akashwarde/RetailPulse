"""Opt-in PostgreSQL integration fixtures; each test gets a new database."""
import os
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
import pytest


@pytest.fixture(scope="session")
def spark():
    """Shared Spark session for the additional contract regression tests."""
    from retailpulse.transform.preparation import session
    active = session("RetailPulseContractTests")
    yield active
    active.stop()


@pytest.fixture
def pg():
    """Create an isolated test DB only when a test-server DSN is supplied."""
    dsn = os.getenv("RETAILPULSE_TEST_DSN")
    if not dsn:
        pytest.skip("Set RETAILPULSE_TEST_DSN for an isolated PostgreSQL test server")
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    name = "retailpulse_test_" + uuid4().hex
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    conn = psycopg2.connect(make_dsn(dsn, dbname=name))
    try:
        ddl = Path(__file__).resolve().parents[1] / "sql/ddl"
        with conn.cursor() as cur:
            for filename in ["01_source_schema.sql", "02_dq_schema.sql", "03_dw_schema.sql", "04_raw_sales.sql", "05_retry_keys.sql"]:
                cur.execute((ddl / filename).read_text(encoding="utf-8"))
        conn.commit()
        yield conn
    finally:
        conn.close()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        admin.close()


# ---- in-memory MinIO stand-in and config used by the streaming / pipeline tests ----
import io  # noqa: E402
from typing import Dict  # noqa: E402

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig  # noqa: E402

os.environ.setdefault("POSTGRES_PASSWORD", "test-password")
os.environ.setdefault("MINIO_ROOT_PASSWORD", "test-password")


class FakeResponse:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:  # noqa: D401
        pass

    def release_conn(self) -> None:
        pass


class FakeMinio:
    """Minimal subset of the minio.Minio API, backed by a dict."""

    def __init__(self) -> None:
        self.objects: Dict[str, bytes] = {}

    def put_object(self, bucket, name, data, length, content_type=None, **_kw):
        payload = data.read() if hasattr(data, "read") else bytes(data)
        self.objects[f"{bucket}/{name}"] = payload
        return None

    def get_object(self, bucket, name):
        key = f"{bucket}/{name}"
        if key not in self.objects:
            from minio.error import S3Error

            raise S3Error("NoSuchKey", "missing", name, "rid", "hid", None, bucket_name=bucket, object_name=name)
        return FakeResponse(self.objects[key])

    def stat_object(self, bucket, name):
        self.get_object(bucket, name)
        return object()

    def keys(self, prefix: str = ""):
        return sorted(k for k in self.objects if k.startswith(prefix))


@pytest.fixture
def fake_minio() -> FakeMinio:
    return FakeMinio()


@pytest.fixture
def app_config() -> AppConfig:
    return AppConfig(postgres=PostgresConfig(password="pw"), minio=MinIOConfig(secret_key="secret"))
