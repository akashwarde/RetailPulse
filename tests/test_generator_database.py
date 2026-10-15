"""Source staging round-trip on an isolated PostgreSQL database."""
from contextlib import contextmanager
from unittest.mock import patch

from retailpulse.common.config import AppConfig, PostgresConfig, MinIOConfig
from retailpulse.generators.demo import generate
from retailpulse.ingestion.batch import extract_sales
from retailpulse.common.lake import read_parquet
from tests.fakes import Lake


def test_generator_database_retry_and_extraction(pg, tmp_path):
    @contextmanager
    def connection(*args, **kwargs):
        yield pg
    kwargs = dict(days=2, orders=1000, defect_rate=0.04, end_date="2025-01-15", output_dir=tmp_path)
    with patch("retailpulse.generators.demo.get_db_connection", connection):
        generate(**kwargs)
        generate(**kwargs)
    with pg.cursor() as cur:
        for table, expected in [("customers", 1000), ("orders", 1000), ("order_items", 1000)]:
            cur.execute(f"SELECT count(*) FROM {table}")
            assert cur.fetchone()[0] == expected
        cur.execute("SELECT count(*), count(*) FILTER (WHERE customer_id IS NULL), count(*) FILTER (WHERE quantity<0) FROM raw_sales")
        count, nulls, negatives = cur.fetchone()
        assert count == 1000 and nulls > 0 and negatives > 0
    cfg = AppConfig(postgres=PostgresConfig(password="test"), minio=MinIOConfig(secret_key="test"))
    lake = Lake()
    result = extract_sales("2025-01-15", cfg, lake, pg)
    assert result["rows"] == 500
    assert read_parquet(lake, cfg.minio.bucket, result["path"]).num_rows == 500
