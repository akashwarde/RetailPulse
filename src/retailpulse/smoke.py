"""End-to-end smoke test (Phase 5.2). Run inside the Airflow container:

    python -m retailpulse.smoke --days 10 --with-stream     # `make smoke`
    python -m retailpulse.smoke --direct                    # no Airflow: run stages in-process
    python -m retailpulse.smoke wait --run-id X --dag Y     # used by `make batch`

Steps: apply DDL -> generate data -> run the batch (through Airflow, or directly) -> assert on
the warehouse -> optionally push events through the streaming path -> print PASS/FAIL per check.
Exit code is 0 only if every check passes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Callable, List, Optional, Tuple

from retailpulse.common.db import get_db_connection
from retailpulse.common.logging import get_logger

logger = get_logger("smoke")
DAG_ID = "retail_batch_pipeline"
Check = Tuple[str, bool, str]


def _scalar(conn, sql: str, params: tuple = ()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def airflow_state(dag_id: str, run_id: str) -> str:
    out = subprocess.run(["airflow", "dags", "list-runs", "-d", dag_id, "-o", "json"],
                         capture_output=True, text=True)
    text = out.stdout
    # Airflow log lines also start with "[", so look for the JSON array of run objects itself.
    start = text.find("[{")
    if start < 0:
        start = text.find("[\n  {")
    if start < 0:
        return "unknown"
    try:
        runs = json.loads(text[start:])
    except ValueError:
        return "unknown"
    for run in runs:
        if run.get("run_id") == run_id:
            return str(run.get("state", "unknown")).lower()
    return "unknown"


def wait_for_run(dag_id: str, run_id: str, timeout: int = 1200) -> str:
    deadline = time.time() + timeout
    state = "unknown"
    while time.time() < deadline:
        state = airflow_state(dag_id, run_id)
        if state in ("success", "failed"):
            return state
        time.sleep(10)
    return f"timeout({state})"


def trigger_batch(run_date: str) -> str:
    run_id = f"smoke_{run_date}_{int(time.time())}"
    subprocess.run(["airflow", "dags", "unpause", DAG_ID], check=True, capture_output=True)
    subprocess.run(["airflow", "dags", "trigger", DAG_ID, "-e", f"{run_date}T{datetime.now(timezone.utc):%H:%M:%S}+00:00",
                    "--run-id", run_id], check=True, capture_output=True)
    return run_id


def batch_assertions(run_date: str) -> List[Check]:
    """SQL assertions on the warehouse after a batch for ``run_date``."""
    from retailpulse.validation.quarantine import get_quality_threshold

    checks: List[Check] = []
    d_key = int(run_date.replace("-", ""))
    with get_db_connection("warehouse") as conn:
        n = _scalar(conn, "SELECT COUNT(*) FROM dw.fact_sales WHERE date_key = %s AND channel = 'in_store'", (d_key,))
        checks.append(("fact_sales has rows for the run date", n > 0, f"{n} rows"))

        dup = _scalar(conn, """SELECT COUNT(*) FROM (SELECT order_id, order_item_id FROM dw.fact_sales
                               WHERE channel = 'in_store' GROUP BY 1, 2 HAVING COUNT(*) > 1) d""")
        checks.append(("no duplicate (order_id, order_item_id) in fact_sales", dup == 0, f"{dup} duplicates"))

        n = _scalar(conn, "SELECT COUNT(*) FROM dw.fact_inventory WHERE date_key = %s", (d_key,))
        checks.append(("fact_inventory has rows for the run date", n > 0, f"{n} rows"))

        for dim in ("dim_product", "dim_customer", "dim_store"):
            n = _scalar(conn, f"SELECT COUNT(*) FROM dw.{dim}")
            checks.append((f"{dim} is populated", n > 0, f"{n} rows"))

        neg = _scalar(conn, "SELECT COUNT(*) FROM dw.fact_sales WHERE quantity <= 0 OR revenue < 0")
        checks.append(("no negative quantity / revenue reached the warehouse", neg == 0, f"{neg} bad rows"))

        with conn.cursor() as cur:
            cur.execute("""SELECT DISTINCT ON (source) source, reject_pct FROM dq.summary
                           WHERE batch_id ~ %s ORDER BY source, created_at DESC""", (f"_{run_date}(_|$)",))
            rows = cur.fetchall()
        threshold = get_quality_threshold()
        got = {r[0] for r in rows}
        checks.append(("dq.summary has all three batch sources", {"sales", "inventory", "customers"} <= got, str(sorted(got))))
        worst = max((float(r[1]) for r in rows), default=100.0)
        checks.append((f"reject_pct below the {threshold:g}% threshold", worst <= threshold, f"worst {worst}%"))

        q = _scalar(conn, "SELECT COUNT(*) FROM dq.quarantine WHERE source IN ('sales','inventory','customers')")
        checks.append(("injected defects were caught and quarantined", q > 0, f"{q} quarantined rows"))

        for mart in ("mart_daily_revenue", "mart_top_products", "mart_low_stock", "mart_dq_summary",
                     "mart_channel_performance"):
            try:
                n = _scalar(conn, f"SELECT COUNT(*) FROM dw.{mart}")
                checks.append((f"{mart} is queryable", True, f"{n} rows"))
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                checks.append((f"{mart} is queryable", False, str(exc)[:80]))
    return checks


def idempotency_check(run_date: str) -> Check:
    """Re-run the warehouse loads and verify the fact row counts do not change."""
    from retailpulse import pipeline

    with get_db_connection("warehouse") as conn:
        before = (_scalar(conn, "SELECT COUNT(*) FROM dw.fact_sales"), _scalar(conn, "SELECT COUNT(*) FROM dw.fact_inventory"))
    pipeline.load_dimensions_stage(run_date)
    pipeline.load_facts_stage(run_date)
    with get_db_connection("warehouse") as conn:
        after = (_scalar(conn, "SELECT COUNT(*) FROM dw.fact_sales"), _scalar(conn, "SELECT COUNT(*) FROM dw.fact_inventory"))
    return ("re-running the loads creates no duplicates (idempotent)", before == after, f"{before} -> {after}")


def stream_checks(n_events: int = 300) -> List[Check]:
    """Push events through Redpanda and the whole streaming path (needs the streaming profile)."""
    from confluent_kafka import Producer

    from retailpulse import pipeline
    from retailpulse.common.config import get_config
    from retailpulse.ingestion.stream.order_generator import OrderGenerator

    cfg = get_config()
    gen = OrderGenerator(invalid_rate=0.04, late_rate=0.02, seed=int(time.time()) % 10_000)
    producer = Producer({"bootstrap.servers": cfg.redpanda.broker})
    for _ in range(n_events):
        evt = gen.generate_event()
        producer.produce(cfg.redpanda.orders_topic, key=evt.key, value=evt.payload)
    producer.flush(30)

    checks: List[Check] = []
    total = {"consumed": 0, "valid": 0, "invalid": 0}
    bronze: List[str] = []
    for _ in range(5):                      # drain in a few micro-batches
        batch = pipeline.stream_consume_stage(max_messages=500, max_seconds=20)
        for k in total:
            total[k] += batch[k]
        bronze += batch["bronze_paths"]
        if batch["consumed"] == 0:
            break
    checks.append(("stream: events consumed from orders_stream", total["consumed"] >= n_events * 0.9, str(total)))
    checks.append(("stream: invalid events detected", total["invalid"] > 0, f"{total['invalid']} invalid"))
    silver = pipeline.stream_silver_stage(sorted(set(bronze)))
    loaded = pipeline.stream_load_stage(silver["silver_paths"])
    checks.append(("stream: online rows loaded into fact_sales", loaded["loaded"] > 0, str(loaded)))
    with get_db_connection("warehouse") as conn:
        online_before = _scalar(conn, "SELECT COUNT(*) FROM dw.fact_sales WHERE channel = 'online'")
    pipeline.stream_load_stage(silver["silver_paths"])  # replay the same silver files
    with get_db_connection("warehouse") as conn:
        online_after = _scalar(conn, "SELECT COUNT(*) FROM dw.fact_sales WHERE channel = 'online'")
    checks.append(("stream: replaying a batch inserts nothing (idempotent)",
                   online_after == online_before, f"online rows {online_before} -> {online_after}"))
    with get_db_connection("warehouse") as conn:
        dlq_rows = _scalar(conn, "SELECT COUNT(*) FROM dq.quarantine WHERE source = 'online_orders'")
    checks.append(("stream: invalid events quarantined in Postgres", dlq_rows > 0, f"{dlq_rows} rows"))
    return checks


def report(checks: List[Check]) -> int:
    width = max(len(c[0]) for c in checks)
    for name, ok, detail in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<{width}}  {detail}")
    failed = [c for c in checks if not c[1]]
    print(f"\n{'ALL CHECKS PASSED' if not failed else str(len(failed)) + ' CHECK(S) FAILED'} ({len(checks)} checks)")
    return 1 if failed else 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="RetailPulse end-to-end smoke test")
    parser.add_argument("command", nargs="?", choices=["run", "wait"], default="run")
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--date", default=(date.today() - timedelta(days=1)).isoformat())
    parser.add_argument("--direct", action="store_true", help="skip Airflow, run stages in-process")
    parser.add_argument("--with-stream", action="store_true")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--dag", default=DAG_ID)
    parser.add_argument("--run-id")
    parser.add_argument("--timeout", type=int, default=1200)
    args = parser.parse_args(argv)

    if args.command == "wait":
        state = wait_for_run(args.dag, args.run_id, args.timeout)
        print(f"{args.dag} {args.run_id}: {state}")
        return 0 if state == "success" else 1

    import os

    from retailpulse.common.initialize import main as apply_ddl
    from retailpulse.generators.demo import generate

    checks: List[Check] = []
    apply_ddl()
    if not args.skip_generate:
        stats = generate(days=args.days, orders=args.days * 250, defect_rate=0.03,
                         end_date=args.date, output_dir=os.getenv("DATA_DIR", "data/source"))
        checks.append(("generated source data", stats["orders"] > 0, f"{stats['orders']} orders"))

    if args.direct:
        from retailpulse import pipeline

        pipeline.run_batch(args.date)
        checks.append(("batch ran in-process", True, args.date))
    else:
        run_id = trigger_batch(args.date)
        print(f"Triggered {DAG_ID} ({run_id}); waiting ...")
        state = wait_for_run(DAG_ID, run_id, args.timeout)
        checks.append(("Airflow batch DAG run succeeded", state == "success", state))
        if state != "success":
            return report(checks)

    checks += batch_assertions(args.date)
    checks.append(idempotency_check(args.date))
    if args.with_stream:
        try:
            checks += stream_checks()
        except Exception as exc:  # noqa: BLE001
            checks.append(("streaming path", False, f"{type(exc).__name__}: {exc}"[:120]))
    return report(checks)


if __name__ == "__main__":
    sys.exit(main())
