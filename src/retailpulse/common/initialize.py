"""Apply source/warehouse SQL to new or existing databases without dropping databases or tables."""
from pathlib import Path

import psycopg2
from psycopg2 import sql

from retailpulse.common.config import get_config
from retailpulse.common.db import get_db_connection
from retailpulse.warehouse.marts import refresh_marts


DDL_PLAN = {
    "source": ["01_source_schema.sql", "04_raw_sales.sql"],
    "warehouse": ["02_dq_schema.sql", "03_dw_schema.sql", "05_retry_keys.sql", "06_pipeline_events.sql"],
}
AIRFLOW_DB = "airflow_db"


def ensure_databases() -> list[str]:
    """Create the warehouse and Airflow metadata databases when they are missing.

    The Postgres image only runs its init scripts on a brand-new volume, and some hosts
    (Docker Desktop bind mounts) cannot execute them, so the application makes sure itself.
    Nothing is dropped; existing databases are left alone.
    """
    cfg = get_config().postgres
    created = []
    conn = psycopg2.connect(cfg.dsn("source"))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for name in (cfg.warehouse_db, AIRFLOW_DB):
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
                if cur.fetchone() is None:
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                    created.append(name)
    finally:
        conn.close()
    return created


def main() -> None:
    """Initialize project databases and schemas (idempotent)."""
    ensure_databases()
    ddl = Path(__file__).resolve().parents[3] / "sql" / "ddl"
    for database, files in DDL_PLAN.items():
        with get_db_connection(database) as conn:
            try:
                with conn.cursor() as cur:
                    for name in files:
                        cur.execute((ddl / name).read_text(encoding="utf-8"))
                conn.commit()
                if database == "warehouse":
                    refresh_marts(conn)
            except Exception:
                conn.rollback()
                raise


if __name__ == "__main__":
    main()
