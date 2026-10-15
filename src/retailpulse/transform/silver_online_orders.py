"""Bronze -> silver for streaming online orders (pandas, not Spark).

Micro-batches are tiny (hundreds of rows every few minutes); starting a Spark session for each
would cost far more than the work itself, so this stage uses pandas. The daily batch jobs keep
using Spark. Events were already validated by the consumer; this step standardises and
de-duplicates (``event_id``) and derives the revenue amount.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.lake import get_minio_client, read_parquet, write_parquet
from retailpulse.common.logging import get_logger

logger = get_logger("transform.silver_online_orders")

SILVER_ONLINE_COLUMNS = [
    "event_id", "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "total_amount", "order_timestamp", "order_date", "_processed_at",
]


def transform_online_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise one bronze frame: types, revenue, de-duplicate on ``event_id``."""
    out = df.copy()
    out = out.sort_values(["event_time", "_kafka_offset"] if "_kafka_offset" in out else ["event_time"])
    out = out.drop_duplicates(subset=["event_id"], keep="first")
    out = out[(out["quantity"] > 0) & (out["unit_price"] > 0)]       # defensive re-check
    out["order_timestamp"] = pd.to_datetime(out["event_time"])
    out["order_date"] = out["order_timestamp"].dt.date
    out["total_amount"] = (out["quantity"] * out["unit_price"]).round(2)
    out["_processed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    return out[SILVER_ONLINE_COLUMNS].reset_index(drop=True)


def run_silver_online(
    bronze_paths: List[str],
    config: Optional[AppConfig] = None,
    client: Optional[Minio] = None,
) -> Dict[str, Any]:
    """Transform the bronze files of one micro-batch into the matching silver files.

    Each ``bronze/online_orders/dt=D/part-X.parquet`` becomes
    ``silver/online_orders/dt=D/part-X.parquet`` (same name => idempotent on replay).
    """
    cfg = config or get_config()
    minio_client = client or get_minio_client(cfg)
    silver_paths: List[str] = []
    rows_in = rows_out = 0
    for bronze_path in bronze_paths:
        table = read_parquet(minio_client, cfg.minio.bucket, bronze_path)
        df = table.to_pandas()
        clean = transform_online_orders(df)
        rows_in += len(df)
        rows_out += len(clean)
        silver_path = bronze_path.replace("bronze/", "silver/", 1)
        write_parquet(minio_client, cfg.minio.bucket, silver_path, pa.Table.from_pandas(clean, preserve_index=False))
        silver_paths.append(silver_path)
    logger.info("Silver online orders: %d bronze rows -> %d silver rows in %d file(s)",
                rows_in, rows_out, len(silver_paths))
    return {"silver_paths": silver_paths, "rows_in": rows_in, "rows_out": rows_out,
            "duplicates_dropped": rows_in - rows_out}
