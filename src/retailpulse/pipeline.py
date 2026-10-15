"""Plain-Python entrypoints for each pipeline stage and for a full batch run.

The Airflow DAGs call exactly these functions (the DAG files stay thin), and
``python -m retailpulse.pipeline YYYY-MM-DD`` runs the same stages in-process, which is handy
for debugging without the scheduler.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any, Dict

from retailpulse.common.db import get_db_connection
from retailpulse.common.logging import get_logger

# NOTE: heavy imports (pyspark, pyarrow, ...) happen inside the stage functions so importing this
# module, which the Airflow scheduler does on every DAG parse, stays cheap.

logger = get_logger("pipeline")


def _plain(obj: Any) -> Any:
    """Make dataclass results JSON/XCom friendly."""
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, dict):
        return {k: _plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_plain(v) for v in obj]
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _plain(getattr(obj, k)) for k in obj.__dataclass_fields__}
    return obj


# ---- batch stages (one per DAG task) -------------------------------------------------
def extract_stage(source: str, run_date: str) -> Dict[str, Any]:
    from retailpulse.ingestion.batch import (
        extract_customers,
        extract_inventory,
        extract_masters,
        extract_sales,
    )

    extractors = {"sales": extract_sales, "inventory": extract_inventory,
                  "customers": extract_customers, "masters": extract_masters}
    if source not in extractors:
        raise ValueError(f"Unknown source {source!r}")
    return _plain(extractors[source](run_date))


def silver_stage(source: str, run_date: str) -> Dict[str, Any]:
    from retailpulse.transform import run_silver_customers, run_silver_inventory, run_silver_sales

    runner = {"sales": run_silver_sales, "inventory": run_silver_inventory,
              "customers": run_silver_customers}[source]
    return _plain(runner(run_date))


def load_dimensions_stage(run_date: str) -> Dict[str, Any]:
    from retailpulse.warehouse import load_dimensions

    return _plain(load_dimensions(run_date))


def load_facts_stage(run_date: str) -> Dict[str, Any]:
    from retailpulse.warehouse import load_facts

    return _plain(load_facts(run_date))


def refresh_marts_stage() -> Dict[str, Any]:
    from retailpulse.warehouse.marts import refresh_marts

    with get_db_connection("warehouse") as conn:
        return {"marts": _plain(refresh_marts(conn))}


# ---- streaming stages ----------------------------------------------------------------
def stream_consume_stage(max_messages: int = 500, max_seconds: float = 30.0) -> Dict[str, Any]:
    from retailpulse.ingestion.stream.order_consumer import run_microbatch

    return run_microbatch(max_messages=max_messages, max_seconds=max_seconds)


def stream_silver_stage(bronze_paths: list) -> Dict[str, Any]:
    from retailpulse.transform.silver_online_orders import run_silver_online

    return run_silver_online(bronze_paths)


def stream_load_stage(silver_paths: list, batch_id: str | None = None) -> Dict[str, Any]:
    """Load clean online events with the shared fact loader (replay-safe on event_id)."""
    from retailpulse.common.config import get_config
    from retailpulse.common.lake import get_minio_client, read_parquet
    from retailpulse.warehouse import load_fact_sales

    cfg, client = get_config(), None
    totals = {"files": len(silver_paths), "total": 0, "loaded": 0, "skipped": 0, "quarantined": 0}
    if not silver_paths:
        return totals
    client = get_minio_client(cfg)
    with get_db_connection("warehouse") as conn:
        for path in silver_paths:
            rows = read_parquet(client, cfg.minio.bucket, path).to_pylist()
            res = load_fact_sales(conn, data=rows, channel="online",
                                  batch_id=f"{batch_id or 'online'}:{path}")
            totals["total"] += res.total_records
            totals["loaded"] += res.loaded
            totals["skipped"] += res.skipped
            totals["quarantined"] += res.quarantined
    return totals


def run_batch(run_date: str) -> Dict[str, Any]:
    """Run the whole daily batch in-process (same stages the DAG schedules)."""
    out: Dict[str, Any] = {"extract_masters": extract_stage("masters", run_date)}
    for source in ("customers", "sales", "inventory"):
        out[f"extract_{source}"] = extract_stage(source, run_date)
        out[f"silver_{source}"] = silver_stage(source, run_date)
    out["load_dimensions"] = load_dimensions_stage(run_date)
    out["load_facts"] = load_facts_stage(run_date)
    out["refresh_marts"] = refresh_marts_stage()
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the RetailPulse batch for one date in-process.")
    parser.add_argument("run_date", nargs="?", default=date.today().isoformat())
    args = parser.parse_args(argv)
    print(json.dumps(run_batch(args.run_date), indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
