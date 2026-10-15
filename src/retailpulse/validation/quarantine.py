"""Quarantine persistence, quality summary reporting, and quality gate for RetailPulse."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pyarrow as pa

if TYPE_CHECKING:
    from minio import Minio
    from psycopg2.extensions import connection as PgConnection
    from pyspark.sql import DataFrame

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.db import get_connection
from retailpulse.common.lake import build_lake_path, get_minio_client, write_parquet
from retailpulse.common.logging import get_logger

logger = get_logger("validation.quarantine")


class QualityGateError(Exception):
    """Raised when the data quality rejection percentage exceeds the allowed threshold."""

    def __init__(
        self,
        message: str,
        summary: Optional[QualitySummary] = None,
        threshold: float = 5.0,
    ) -> None:
        super().__init__(message)
        self.summary = summary
        self.threshold = threshold


@dataclass
class QualitySummary:
    """Summary metrics of a validation batch."""

    source: str
    batch_id: str
    total: int
    accepted: int
    rejected: int
    reject_pct: float
    per_rule_counts: Dict[str, int]
    created_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert QualitySummary to a dictionary."""
        return asdict(self)


def calculate_quality_summary(
    bad_df: DataFrame,
    total: int,
    source: str,
    batch_id: str,
) -> QualitySummary:
    """Calculate data quality metrics from a validation run.

    Args:
        bad_df: DataFrame of records failing validation (must have 'rule_failed').
        total: Total input records processed.
        source: Data source name (e.g. 'sales', 'inventory', 'customers').
        batch_id: Identifier for the ingestion batch.

    Returns:
        QualitySummary: Aggregated metrics.
    """
    rejected = bad_df.count()
    accepted = max(0, total - rejected)
    reject_pct = round((rejected / total) * 100.0, 2) if total > 0 else 0.0

    per_rule_counts: Dict[str, int] = {}
    if rejected > 0 and "rule_failed" in bad_df.columns:
        for row in bad_df.select("rule_failed").collect():
            val = row["rule_failed"]
            if val:
                for rule in str(val).split(","):
                    clean_rule = rule.strip()
                    if clean_rule:
                        per_rule_counts[clean_rule] = (
                            per_rule_counts.get(clean_rule, 0) + 1
                        )

    return QualitySummary(
        source=source,
        batch_id=batch_id,
        total=total,
        accepted=accepted,
        rejected=rejected,
        reject_pct=reject_pct,
        per_rule_counts=per_rule_counts,
        created_at=datetime.now(),
    )


def write_quarantine(
    bad_df: DataFrame,
    source: str,
    batch_id: str,
    dt: Optional[Union[str, date, datetime]] = None,
    filename: Optional[str] = None,
    client: Optional[Minio] = None,
    conn: Optional[PgConnection] = None,
    config: Optional[AppConfig] = None,
    bucket: Optional[str] = None,
    db_target: str = "warehouse",
) -> int:
    """Write rejected records to MinIO (Parquet) and PostgreSQL (dq.quarantine table).

    Args:
        bad_df: DataFrame containing rejected rows with 'rule_failed' and 'reason'.
        source: Data source name.
        batch_id: Ingestion batch identifier.
        dt: Date partition (defaults to today's date YYYY-MM-DD).
        filename: Optional Parquet file name. Defaults to 'data.parquet'.
        client: Optional Minio client instance.
        conn: Optional active PostgreSQL connection.
        config: Optional AppConfig instance.
        bucket: Optional MinIO bucket name (defaults to configured bucket).
        db_target: PostgreSQL database target ('warehouse' or 'source').

    Returns:
        int: Number of bad records written to quarantine.
    """
    count = bad_df.count()
    if count == 0:
        logger.info(
            "No rejected records to quarantine for source '%s' (batch '%s').",
            source,
            batch_id,
        )
        return 0

    cfg = config or get_config()
    target_bucket = bucket or cfg.minio.bucket
    date_str = (
        dt.strftime("%Y-%m-%d")
        if isinstance(dt, (date, datetime))
        else (str(dt) if dt else datetime.now().strftime("%Y-%m-%d"))
    )
    target_filename = filename or f"quarantine_{batch_id}.parquet"

    # 1. Write to MinIO Parquet
    lake_path = build_lake_path(
        layer="quarantine",
        source=source,
        dt=date_str,
        filename=target_filename,
    )
    minio_client = client or get_minio_client(cfg)

    # Convert Spark DataFrame to PyArrow Table
    pdf = bad_df.toPandas()
    arrow_table = pa.Table.from_pandas(pdf)
    write_parquet(minio_client, target_bucket, lake_path, arrow_table)
    logger.info(
        "Quarantined %d rows to lake s3://%s/%s", count, target_bucket, lake_path
    )

    # 2. Write to PostgreSQL dq.quarantine
    payload_cols = [
        c for c in pdf.columns if c not in ("rule_failed", "reason")
    ]

    records_to_insert = []
    for _, row in pdf.iterrows():
        raw_dict = {col: row[col] for col in payload_cols}
        raw_json = json.dumps(raw_dict, default=str)
        records_to_insert.append((
            source,
            str(row.get("rule_failed", "")),
            str(row.get("reason", "")),
            batch_id,
            raw_json,
        ))

    insert_sql = """
        INSERT INTO dq.quarantine (
            source, rule_failed, reason, batch_id, raw_record
        ) VALUES (%s, %s, %s, %s, %s);
    """

    should_close_conn = False
    active_conn = conn
    if active_conn is None:
        active_conn = get_connection(db_target=db_target, config=cfg)
        should_close_conn = True

    try:
        with active_conn.cursor() as cur:
            cur.executemany(insert_sql, records_to_insert)
        if not active_conn.autocommit:
            active_conn.commit()
        logger.info(
            "Inserted %d rows into PostgreSQL dq.quarantine for source '%s'",
            len(records_to_insert),
            source,
        )
    finally:
        if should_close_conn and not active_conn.closed:
            active_conn.close()

    return count


def write_quality_summary(
    source: str,
    batch_id: str,
    total: int,
    accepted: int,
    rejected: int,
    per_rule_counts: Dict[str, int],
    conn: Optional[PgConnection] = None,
    config: Optional[AppConfig] = None,
    db_target: str = "warehouse",
) -> QualitySummary:
    """Persist a per-batch data quality summary record to dq.summary table.

    Args:
        source: Data source name.
        batch_id: Batch identifier.
        total: Total input rows processed.
        accepted: Total valid rows accepted.
        rejected: Total invalid rows rejected.
        per_rule_counts: Breakdown mapping rule name -> failure count.
        conn: Optional active PostgreSQL connection.
        config: Optional AppConfig instance.
        db_target: Target database ('warehouse' by default).

    Returns:
        QualitySummary: The persisted summary object.
    """
    reject_pct = round((rejected / total) * 100.0, 2) if total > 0 else 0.0
    summary = QualitySummary(
        source=source,
        batch_id=batch_id,
        total=total,
        accepted=accepted,
        rejected=rejected,
        reject_pct=reject_pct,
        per_rule_counts=per_rule_counts,
        created_at=datetime.now(),
    )

    insert_sql = """
        INSERT INTO dq.summary (
            source, batch_id, total, accepted, rejected, reject_pct, per_rule_counts
        ) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    cfg = config or get_config()
    should_close_conn = False
    active_conn = conn
    if active_conn is None:
        active_conn = get_connection(db_target=db_target, config=cfg)
        should_close_conn = True

    try:
        with active_conn.cursor() as cur:
            cur.execute(
                insert_sql,
                (
                    source,
                    batch_id,
                    total,
                    accepted,
                    rejected,
                    reject_pct,
                    json.dumps(per_rule_counts),
                ),
            )
        if not active_conn.autocommit:
            active_conn.commit()
        logger.info(
            "Recorded quality summary for source '%s' (batch '%s'): reject_pct=%.2f%%",
            source,
            batch_id,
            reject_pct,
        )
    finally:
        if should_close_conn and not active_conn.closed:
            active_conn.close()

    return summary


def quality_gate(
    summary: Union[QualitySummary, Dict[str, Any]],
    threshold: float = 5.0,
) -> bool:
    """Evaluate whether batch data quality satisfies the rejection threshold.

    Args:
        summary: QualitySummary object or dict containing 'reject_pct'.
        threshold: Maximum allowed rejection percentage (default: 5.0%).

    Returns:
        bool: True if reject_pct <= threshold.

    Raises:
        QualityGateError: If reject_pct > threshold.
    """
    if isinstance(summary, QualitySummary):
        reject_pct = summary.reject_pct
        source = summary.source
        batch_id = summary.batch_id
        rejected = summary.rejected
        total = summary.total
    elif isinstance(summary, dict):
        reject_pct = float(summary.get("reject_pct", 0.0))
        source = str(summary.get("source", "unknown"))
        batch_id = str(summary.get("batch_id", "unknown"))
        rejected = int(summary.get("rejected", 0))
        total = int(summary.get("total", 0))
    else:
        raise TypeError(
            f"Expected QualitySummary or dict, got {type(summary).__name__}"
        )

    if reject_pct > threshold:
        msg = (
            f"Quality gate FAILED for source '{source}' (batch '{batch_id}'): "
            f"reject_pct={reject_pct:.2f}% exceeds threshold={threshold:.2f}% "
            f"({rejected}/{total} rejected)."
        )
        logger.error(msg)
        raise QualityGateError(msg, summary=summary if isinstance(summary, QualitySummary) else None, threshold=threshold)

    logger.info(
        "Quality gate PASSED for source '%s' (batch '%s'): reject_pct=%.2f%% <= %.2f%%",
        source,
        batch_id,
        reject_pct,
        threshold,
    )
    return True
