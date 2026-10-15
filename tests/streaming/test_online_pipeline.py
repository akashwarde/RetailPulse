"""Silver + warehouse-load tests for the streaming path (pandas / mocked database)."""

from __future__ import annotations

import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from retailpulse.transform.silver_online_orders import SILVER_ONLINE_COLUMNS, run_silver_online, transform_online_orders


def bronze_frame(**over):
    base = {"event_id": ["a", "b", "b"], "order_id": [1, 2, 2], "customer_id": [5, 6, 6], "product_id": [3, 4, 4],
            "quantity": [1, 2, 2], "unit_price": [10.0, 5.5, 5.5],
            "event_time": pd.to_datetime(["2026-03-10 10:00", "2026-03-10 11:00", "2026-03-10 11:00"]),
            "_kafka_offset": [0, 1, 2]}
    base.update(over)
    return pd.DataFrame(base)


def test_transform_dedupes_on_event_id_and_derives_revenue():
    out = transform_online_orders(bronze_frame())
    assert list(out.columns) == SILVER_ONLINE_COLUMNS
    assert out["event_id"].tolist() == ["a", "b"]
    assert out["total_amount"].tolist() == [10.0, 11.0]
    assert out["order_date"].iloc[0].isoformat() == "2026-03-10"


def test_transform_drops_non_positive_rows_defensively():
    out = transform_online_orders(bronze_frame(quantity=[1, 0, 2]))
    assert out["event_id"].tolist() == ["a"]


def test_silver_keeps_partition_and_file_name(fake_minio, app_config):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(bronze_frame(), preserve_index=False), buf)
    fake_minio.objects["retailpulse/bronze/online_orders/dt=2026-03-10/part-online_x.parquet"] = buf.getvalue()
    res = run_silver_online(["bronze/online_orders/dt=2026-03-10/part-online_x.parquet"], app_config, fake_minio)
    assert res["silver_paths"] == ["silver/online_orders/dt=2026-03-10/part-online_x.parquet"]
    assert (res["rows_in"], res["rows_out"], res["duplicates_dropped"]) == (3, 2, 1)
    assert "retailpulse/silver/online_orders/dt=2026-03-10/part-online_x.parquet" in fake_minio.objects
