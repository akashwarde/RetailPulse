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
