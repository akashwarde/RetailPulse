"""Date-scoped extractors. Pandas/PyArrow keep laptop ingestion lightweight."""
from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
from minio.error import InvalidResponseError, S3Error
from psycopg2.extras import RealDictCursor

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.db import get_db_connection
from retailpulse.common.lake import build_lake_path, get_minio_client, read_parquet, write_parquet
from retailpulse.common.logging import get_logger

log = get_logger("ingestion.batch")


def _root(data_dir: str | Path | None) -> Path:
    return Path(data_dir or os.getenv("DATA_DIR", "data/source"))


def _write(source: str, run_date: str, rows: list[dict], config: AppConfig,
           client, source_file: str, columns: list[str] | None = None) -> dict:
    date.fromisoformat(run_date)
    # Stable logical batch IDs make downstream retries replace their DQ results.
    batch_id = f"{source}_{run_date}"
    timestamp = datetime.now(timezone.utc).isoformat()
    fields = columns or (list(rows[0]) if rows else [])
    arrays = {c: [r.get(c) for r in rows] for c in fields}
    # Preserve source values as text; Spark explicitly validates casts later.
    arrays = {c: pa.array([str(v) if v is not None else None for v in values], type=pa.string())
              for c, values in arrays.items()}
    arrays.update({"_ingested_at": pa.array([timestamp] * len(rows), type=pa.string()),
                   "_source_file": pa.array([source_file] * len(rows), type=pa.string()),
                   "_batch_id": pa.array([batch_id] * len(rows), type=pa.string())})
    path = build_lake_path("bronze", source, run_date)
    write_parquet(client, config.minio.bucket, path, pa.table(arrays))
    log.info("Extracted source=%s date=%s rows=%d path=%s", source, run_date, len(rows), path)
    return dict(source=source, run_date=run_date, rows=len(rows), path=path, batch_id=batch_id)


def extract_sales(run_date: str, config: AppConfig | None = None, client=None, conn=None) -> dict:
    """Read permissive source staging, including defects, into one bronze partition."""
    date.fromisoformat(run_date)
    cfg = config or get_config()
    lake = client or get_minio_client(cfg)
    columns = ["order_id", "order_item_id", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "order_timestamp", "order_status", "payment_method"]
    def read(connection):
        with connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT {', '.join(columns)} FROM raw_sales WHERE source_date = %s ORDER BY raw_id", (run_date,))
            return [dict(r) for r in cur.fetchall()]
    if conn is None:
        with get_db_connection("source", cfg) as connection:
            rows = read(connection)
    else:
        rows = read(conn)
    return _write("sales", run_date, rows, cfg, lake, "postgres:raw_sales", columns)


def extract_inventory(run_date: str, data_dir: str | Path | None = None,
                      config: AppConfig | None = None, client=None) -> dict:
    """Recover harmless header variations; unknown/missing columns fail visibly.

    Fingerprints live beside the bronze partition. Identical successful imports
    skip work; changed content or a missing bronze object forces a fresh write.
    """
    date.fromisoformat(run_date)
    cfg = config or get_config()
    lake = client or get_minio_client(cfg)
    files = sorted((_root(data_dir) / "inventory").glob(f"inventory_{run_date}*.csv"))
    if not files:
        raise FileNotFoundError(f"No inventory CSV for {run_date}")
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    fingerprint = digest.hexdigest()
    marker = build_lake_path("bronze", "inventory", run_date, "manifest.parquet")
    bronze = build_lake_path("bronze", "inventory", run_date)
    try:
        prior = read_parquet(lake, cfg.minio.bucket, marker).to_pylist()
        if prior and prior[0]["fingerprint"] == fingerprint:
            lake.stat_object(cfg.minio.bucket, bronze)
            return dict(source="inventory", run_date=run_date, rows=prior[0]["rows"],
                        path=bronze, batch_id=f"inventory_{run_date}", skipped=True)
    except (S3Error, InvalidResponseError) as exc:
        # InvalidResponseError: some S3 look-alikes answer a missing key with an HTML 404
        missing = exc.code in ("NoSuchKey", "NoSuchObject") if isinstance(exc, S3Error) else "NoSuchKey" in str(exc)
        if not missing:
            raise
    required = ["product_id", "store_id", "stock_qty", "snapshot_date"]
    aliases = {"productid": "product_id", "storeid": "store_id", "stock_quantity": "stock_qty", "date": "snapshot_date"}
    rows = []
    for path in files:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = [h.strip().lower().replace(" ", "_") for h in (reader.fieldnames or [])]
            headers = [aliases.get(h, h) for h in headers]
            if len(set(headers)) != len(headers) or set(headers) != set(required):
                raise ValueError(f"Invalid inventory headers in {path.name}: {headers}")
            reader.fieldnames = headers
            for row in reader:
                if None in row:
                    raise ValueError(f"Extra CSV fields in {path.name}")
                rows.append({k: v.strip() or None if v is not None else None for k, v in row.items()})
    result = _write("inventory", run_date, rows, cfg, lake, ",".join(p.name for p in files), required)
    write_parquet(lake, cfg.minio.bucket, marker, pa.Table.from_pylist([dict(fingerprint=fingerprint, rows=len(rows))]))
    return result


def extract_customers(run_date: str, data_dir: str | Path | None = None,
                      config: AppConfig | None = None, client=None) -> dict:
    """Snapshot customer JSON; duplicate/invalid rows remain visible to validation."""
    cfg = config or get_config()
    path = _root(data_dir) / "customers.json"
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
        raise ValueError("customers.json must contain an array of objects")
    columns = ["customer_id", "first_name", "last_name", "email", "city", "state", "signup_date"]
    return _write("customers", run_date, rows, cfg, client or get_minio_client(cfg), path.name, columns)


def extract_masters(run_date: str, data_dir: str | Path | None = None,
                    config: AppConfig | None = None, client=None) -> dict:
    """Publish validated product/store masters to bronze and silver for dimension loads."""
    cfg = config or get_config()
    lake = client or get_minio_client(cfg)
    result = {}
    for source, key, required in [("products", "product_id", ["product_name", "category", "unit_price"]),
                                   ("stores", "store_id", ["store_name", "city"])]:
        path = _root(data_dir) / f"{source}.csv"
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        seen = set()
        for row in rows:
            if any(not row.get(c, "").strip() for c in [key, *required]):
                raise ValueError(f"Missing master fields in {path.name}")
            row[key] = int(row[key])
            if row[key] <= 0 or row[key] in seen:
                raise ValueError(f"Invalid/duplicate {key} in {path.name}")
            seen.add(row[key])
            if source == "products":
                from decimal import Decimal
                price = Decimal(row["unit_price"])
                if not price.is_finite() or price <= 0:
                    raise ValueError("Product price must be finite and positive")
        if not rows:
            raise ValueError(f"Empty master file: {path.name}")
        result[source] = _write(source, run_date, rows, cfg, lake, path.name)
        write_parquet(lake, cfg.minio.bucket, build_lake_path("silver", source, run_date), pa.Table.from_pylist(rows))
    return result
