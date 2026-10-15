"""Exercise default DQ connections and idempotency against real PostgreSQL."""
from unittest.mock import MagicMock
import os

import pandas as pd

from retailpulse.common.config import AppConfig, PostgresConfig, MinIOConfig
from retailpulse.validation import write_quality_summary, write_quarantine, calculate_quality_summary
from tests.fakes import Lake
from psycopg2.extensions import parse_dsn


def config(pg):
    credentials = parse_dsn(os.environ["RETAILPULSE_TEST_DSN"])
    return AppConfig(postgres=PostgresConfig(host=pg.info.host, port=pg.info.port,
                    user=pg.info.user, password=credentials.get("password", "test"), warehouse_db=pg.info.dbname),
                    minio=MinIOConfig(secret_key="test"))


def frame(rows):
    df = MagicMock()
    df.count.return_value = len(rows)
    df.toPandas.return_value = pd.DataFrame(rows, columns=["customer_id", "rule_failed", "reason"])
    return df


def test_default_connection_and_retry_summary(pg):
    cfg = config(pg)
    for _ in range(2):
        write_quality_summary("sales", "sales_2025-01-15", 10, 9, 1, {"range:quantity": 1}, config=cfg)
    with pg.cursor() as cur:
        cur.execute("SELECT count(*),sum(total) FROM dq.summary")
        assert cur.fetchone() == (1, 10)


def test_quarantine_retry_and_empty_correction(pg):
    cfg, lake = config(pg), Lake()
    bad = frame([dict(customer_id=None, rule_failed="not_null:customer_id", reason="missing")])
    for _ in range(2):
        write_quarantine(bad, "sales", "sales_2025-01-15", dt="2025-01-15", client=lake, config=cfg)
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM dq.quarantine")
        assert cur.fetchone()[0] == 1
    write_quarantine(frame([]), "sales", "sales_2025-01-15", dt="2025-01-15", client=lake, config=cfg)
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM dq.quarantine")
        assert cur.fetchone()[0] == 0


def test_composite_rule_names_are_not_split_into_fake_rules():
    bad = frame([dict(customer_id=1, rule_failed="unique:order_id,order_item_id, range:quantity", reason="bad")])
    bad.columns = ["rule_failed"]
    bad.select.return_value.collect.return_value = [{"rule_failed": "unique:order_id,order_item_id, range:quantity"}]
    summary = calculate_quality_summary(bad, 1, "sales", "batch")
    assert summary.per_rule_counts == {"unique:order_id,order_item_id": 1, "range:quantity": 1}
