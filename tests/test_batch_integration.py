"""Batch components against real PostgreSQL, MinIO and local Spark in Docker."""
from uuid import uuid4

import pytest

from retailpulse.common.config import get_config
from retailpulse.common.lake import get_minio_client
from retailpulse.generators.demo import generate
from retailpulse.pipeline import run_batch


def test_complete_batch_and_repeat_do_not_duplicate(pg, tmp_path, monkeypatch):
    import os
    if os.getenv("RETAILPULSE_TEST_MINIO") != "1":
        pytest.skip("Enable RETAILPULSE_TEST_MINIO with an isolated MinIO test service")
    monkeypatch.setenv("SOURCE_DB", pg.info.dbname)
    monkeypatch.setenv("WAREHOUSE_DB", pg.info.dbname)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    bucket = "retailpulse-test-" + uuid4().hex
    monkeypatch.setenv("MINIO_BUCKET", bucket)
    lake = get_minio_client(get_config())
    lake.make_bucket(bucket)
    try:
        generate(days=1, orders=2000, defect_rate=0.02, end_date="2025-01-15")
        previous = None
        for _ in range(2):
            result = run_batch("2025-01-15")
            assert result["sales"]["silver"]["status"] == "SUCCESS"
            with pg.cursor() as cur:
                cur.execute("SELECT count(*),sum(revenue) FROM dw.fact_sales")
                sales = cur.fetchone()
                assert sales[0] > 1800 and sales[1] > 0
                cur.execute("SELECT count(*) FROM dw.fact_inventory")
                inventory = cur.fetchone()[0]
                assert inventory > 230
                cur.execute("SELECT count(*) FROM dq.summary")
                assert cur.fetchone()[0] == 3
                cur.execute("SELECT count(*) FROM dq.quarantine")
                rejected = cur.fetchone()[0]
                assert rejected > 0
                cur.execute("SELECT order_id,order_item_id FROM dw.fact_sales GROUP BY 1,2 HAVING count(*)>1")
                assert cur.fetchall() == []
            current = (sales, inventory, rejected)
            if previous is not None:
                assert current == previous
            previous = current
    finally:
        # Only objects in this test's newly-created bucket are removed.
        assert bucket.startswith("retailpulse-test-")
        for obj in lake.list_objects(bucket, recursive=True):
            lake.remove_object(bucket, obj.object_name)
        lake.remove_bucket(bucket)
