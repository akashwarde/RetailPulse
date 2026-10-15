"""PostgreSQL quarantine writes shared by Spark jobs and warehouse loaders."""
from __future__ import annotations

import json
from typing import Any


def persist_rejections(conn, source: str, batch_id: str, rows: list[dict[str, Any]],
                       replace: bool = True) -> int:
    """Write rejections in the caller's transaction without committing it.

    Batch jobs replace a stable logical batch. Event loaders append idempotently
    so a different micro-batch on the same date does not remove earlier failures.
    """
    with conn.cursor() as cur:
        if replace:
            cur.execute("DELETE FROM dq.quarantine WHERE source=%s AND batch_id=%s", (source, batch_id))
        if rows:
            cur.executemany("""
                INSERT INTO dq.quarantine (source, rule_failed, reason, batch_id, raw_record)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, [(source, r["rule_failed"], r["reason"], batch_id,
                   json.dumps(r["raw_record"], default=str, allow_nan=False)) for r in rows])
    return len(rows)
