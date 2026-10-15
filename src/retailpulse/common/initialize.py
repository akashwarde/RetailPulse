"""Apply source/warehouse SQL to new or existing databases without dropping databases or tables."""
from pathlib import Path

from retailpulse.common.db import get_db_connection
from retailpulse.warehouse.marts import refresh_marts


def main() -> None:
    """Initialize project schemas; database creation belongs to PostgreSQL init."""
    ddl = Path(__file__).resolve().parents[3] / "sql" / "ddl"
    for database, files in [("source", ["01_source_schema.sql", "04_raw_sales.sql"]),
                            ("warehouse", ["02_dq_schema.sql", "03_dw_schema.sql", "05_retry_keys.sql"])]:
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
