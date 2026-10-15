"""Failure logging and notifications (Phase 4.6).

Everything here is best-effort: monitoring must never be the reason a pipeline
fails, so database / network problems are logged and swallowed.

Notification channels (all optional, configured via environment):

* always: structured log line + ``dq.pipeline_events`` row + ``logs/alerts.log``
* ``ALERT_WEBHOOK_URL``: JSON ``{"text": ...}`` POST (Slack / Discord / Teams compatible)
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.db import get_db_connection
from retailpulse.common.logging import get_logger

logger = get_logger("monitoring")


def record_event(
    dag_id: str,
    status: str,
    message: str,
    task_id: Optional[str] = None,
    run_id: Optional[str] = None,
    run_date: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> bool:
    """Insert a row into ``dq.pipeline_events``. Returns False (never raises) on failure."""
    try:
        with get_db_connection(database="warehouse", config=config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dq.pipeline_events (dag_id, task_id, run_id, run_date, status, message) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (dag_id, task_id, run_id, run_date, status, message[:4000]),
                )
            conn.commit()
        return True
    except Exception as exc:  # noqa: BLE001 - monitoring must not raise
        logger.error("Could not record pipeline event: %s", exc)
        return False


def post_webhook(text: str, url: Optional[str] = None) -> bool:
    """POST ``{"text": text}`` to the alert webhook, if configured."""
    target = url or os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not target:
        return False
    try:
        req = urllib.request.Request(
            target, data=json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 - operator-supplied URL
            return 200 <= resp.status < 300
    except Exception as exc:  # noqa: BLE001
        logger.error("Webhook notification failed: %s", exc)
        return False


def append_alert_log(line: str, path: Optional[str] = None) -> None:
    target = Path(path or os.getenv("ALERT_LOG_PATH", "logs/alerts.log"))
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now().isoformat(timespec='seconds')} {line}\n")
    except OSError as exc:
        logger.error("Could not write alert log %s: %s", target, exc)


def _is_gate_error(exc: Any) -> bool:
    from retailpulse.validation.quarantine import QualityGateError

    return isinstance(exc, (QualityGateError, StreamQualityGateError))


def format_failure_message(context: Mapping[str, Any]) -> str:
    """Readable one-paragraph description of a failed Airflow task."""
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", None) or context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = getattr(ti, "task_id", "unknown_task")
    run_id = context.get("run_id", "unknown_run")
    tries = getattr(ti, "try_number", "?")
    exc = context.get("exception")
    kind = "QUALITY GATE FAILED" if _is_gate_error(exc) else "TASK FAILED"
    return (f"[RetailPulse] {kind}: dag={dag_id} task={task_id} run={run_id} "
            f"ds={context.get('ds', '?')} attempt={tries} -- {type(exc).__name__}: {exc}")


def on_failure_callback(context: Mapping[str, Any]) -> None:
    """Airflow ``on_failure_callback``: log, persist, and notify."""
    message = format_failure_message(context)
    logger.error(message)
    exc = context.get("exception")
    ti = context.get("task_instance")
    status = "GATE_FAILED" if _is_gate_error(exc) else "FAILED"
    record_event(
        dag_id=getattr(ti, "dag_id", "unknown"), task_id=getattr(ti, "task_id", None),
        run_id=context.get("run_id"), run_date=context.get("ds"), status=status, message=message)
    append_alert_log(message)
    post_webhook(message)


def summarize_run(run_date: str, config: Optional[AppConfig] = None) -> List[Dict[str, Any]]:
    """Latest per-source DQ summary rows for the batches of ``run_date``."""
    sql = """
        SELECT DISTINCT ON (source) source, batch_id, total, accepted, rejected, reject_pct
        FROM dq.summary
        WHERE batch_id ~ %s
        ORDER BY source, created_at DESC
    """
    with get_db_connection(database="warehouse", config=config) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (f"_{run_date}(_|$)",))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def write_dq_summary(
    run_date: str,
    dag_id: str = "retail_batch_pipeline",
    run_id: Optional[str] = None,
    config: Optional[AppConfig] = None,
) -> Dict[str, Any]:
    """Final batch task: log the quality outcome of the run and persist it as an event."""
    from retailpulse.validation.quarantine import get_quality_threshold

    rows = summarize_run(run_date, config)
    threshold = get_quality_threshold()
    worst = max((float(r["reject_pct"]) for r in rows), default=0.0)
    text = "; ".join(f"{r['source']}: {r['accepted']}/{r['total']} accepted ({r['reject_pct']}% rejected)"
                     for r in rows) or "no quality summaries found"
    status = "SUCCESS" if worst <= threshold else "GATE_FAILED"
    record_event(dag_id, status, f"Run {run_date} data quality -- {text}", task_id="write_dq_summary",
                 run_id=run_id, run_date=run_date, config=config)
    logger.info("DQ summary for %s: %s (threshold %.1f%%)", run_date, text, threshold)
    return {"run_date": run_date, "sources": rows, "worst_reject_pct": worst, "status": status}


class StreamQualityGateError(RuntimeError):
    """Raised when a streaming micro-batch has too many invalid events."""


def stream_quality_gate(result: Mapping[str, Any]) -> Dict[str, Any]:
    """Fail the run when a micro-batch's invalid-event percentage is too high.

    Streaming uses its own (looser) threshold -- ``STREAM_QUALITY_THRESHOLD``, default 15% --
    and ignores tiny batches (``STREAM_GATE_MIN_MESSAGES``, default 50) where one bad event
    would swing the percentage. It runs *after* the load, so valid events are never held back.
    """
    threshold = float(os.getenv("STREAM_QUALITY_THRESHOLD", "15"))
    min_messages = int(os.getenv("STREAM_GATE_MIN_MESSAGES", "50"))
    consumed, pct = int(result.get("consumed", 0)), float(result.get("reject_pct", 0.0))
    if consumed >= min_messages and pct > threshold:
        raise StreamQualityGateError(
            f"Stream quality gate FAILED: {result.get('invalid')}/{consumed} events invalid "
            f"({pct:.2f}% > {threshold:.2f}%) in batch {result.get('batch_id')}")
    return {"passed": True, "reject_pct": pct, "threshold": threshold, "consumed": consumed}
