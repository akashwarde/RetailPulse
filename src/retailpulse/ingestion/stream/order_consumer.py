"""Streaming consumer for ``orders_stream`` with validation and a dead-letter queue (Phase 4.3).

One call to :meth:`OrderStreamConsumer.run_microbatch` consumes a bounded micro-batch
(at most ``max_messages`` messages or ``max_seconds`` seconds), so Airflow can schedule it
every few minutes:

1. validate every event (JSON, required fields, types, positive quantity and price)
2. **valid** events    -> ``bronze/online_orders/dt=<event date>/part-<batch>.parquet``
3. **invalid** events  -> DLQ topic ``orders_dlq`` + ``dq.quarantine`` + quarantine Parquet
4. write a ``dq.summary`` row for the micro-batch
5. **commit Kafka offsets only after steps 2-4 all succeeded**

Delivery guarantee: at-least-once from Kafka plus idempotent writes (deterministic batch id /
file name derived from the offsets, ``event_id`` de-duplication downstream) gives an
exactly-once *effect* in the warehouse. If anything fails, offsets are not committed and the
same messages are redelivered on the next run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pyarrow as pa

try:  # pragma: no cover - import guarded so unit tests run without the C client
    from confluent_kafka import Consumer, KafkaError, Producer
except ImportError:  # pragma: no cover
    Consumer = Producer = None  # type: ignore[assignment, misc]
    KafkaError = None  # type: ignore[assignment, misc]

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.db import get_db_connection
from retailpulse.common.lake import build_lake_path, get_minio_client, write_parquet
from retailpulse.common.logging import get_logger
from retailpulse.common.records import persist_rejections
from retailpulse.validation.quarantine import write_quality_summary

logger = get_logger("ingestion.stream.order_consumer")

SOURCE_NAME = "online_orders"
CONSUMER_GROUP = "retailpulse-orders-consumer"
REQUIRED_FIELDS = ("event_id", "order_id", "customer_id", "product_id", "quantity", "unit_price", "event_time")
PARTITION_EOF = -191  # confluent_kafka.KafkaError._PARTITION_EOF
LATE_AFTER_DAYS = 7


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ----------------------------------------------------------------------------- validation
@dataclass
class ValidationOutcome:
    """Result of validating one raw message."""

    event: Optional[Dict[str, Any]] = None
    rule_failed: Optional[str] = None
    reason: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.event is not None


def _as_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return float(value)


def parse_event_time(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp to a naive UTC datetime (None if invalid)."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def validate_event(payload: Union[bytes, str, None]) -> ValidationOutcome:
    """Validate one raw message. Pure function: no I/O, easy to unit test."""
    if payload is None or (isinstance(payload, (bytes, str)) and len(payload) == 0):
        return ValidationOutcome(rule_failed="empty_payload", reason="Message value is empty")
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        data = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return ValidationOutcome(rule_failed="malformed_json", reason=f"Not valid JSON: {exc}")
    if not isinstance(data, dict):
        return ValidationOutcome(rule_failed="malformed_json", reason="JSON payload is not an object")

    for name in REQUIRED_FIELDS:
        if data.get(name) is None:
            return ValidationOutcome(rule_failed=f"missing_field:{name}", reason=f"Required field '{name}' is missing")

    ids = {}
    for name in ("order_id", "customer_id", "product_id"):
        value = _as_int(data[name])
        if value is None or value <= 0:
            return ValidationOutcome(rule_failed=f"type:{name}", reason=f"'{name}' must be a positive integer, got {data[name]!r}")
        ids[name] = value

    quantity = _as_int(data["quantity"])
    if quantity is None:
        return ValidationOutcome(rule_failed="type:quantity", reason=f"'quantity' must be an integer, got {data['quantity']!r}")
    if quantity <= 0:
        return ValidationOutcome(rule_failed="range:quantity_positive", reason=f"quantity must be > 0, got {quantity}")

    price = _as_number(data["unit_price"])
    if price is None:
        return ValidationOutcome(rule_failed="type:unit_price", reason=f"'unit_price' must be a number, got {data['unit_price']!r}")
    if price <= 0:
        return ValidationOutcome(rule_failed="range:unit_price_positive", reason=f"unit_price must be > 0, got {price}")

    event_time = parse_event_time(data["event_time"])
    if event_time is None:
        return ValidationOutcome(rule_failed="format:event_time", reason=f"'event_time' is not ISO-8601: {data['event_time']!r}")

    event_id = str(data["event_id"]).strip()
    if not event_id:
        return ValidationOutcome(rule_failed="missing_field:event_id", reason="event_id is blank")

    return ValidationOutcome(event={
        "event_id": event_id, **ids, "quantity": quantity, "unit_price": round(price, 2),
        "event_time": event_time, "event_date": event_time.date().isoformat(),
    })


# ----------------------------------------------------------------------------- result
@dataclass
class MicroBatchResult:
    """Counts and bookkeeping for one micro-batch (also the Airflow XCom payload)."""

    batch_id: str
    consumed: int = 0
    valid: int = 0
    invalid: int = 0
    late: int = 0
    reject_pct: float = 0.0
    stop_reason: str = "idle"           # max_messages | max_seconds | idle
    committed: bool = False
    bronze_paths: List[str] = field(default_factory=list)
    dlq_sent: int = 0
    elapsed_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


class ConsumerError(RuntimeError):
    """Raised on a non-recoverable Kafka consumer error."""


# ----------------------------------------------------------------------------- consumer
class OrderStreamConsumer:
    """Bounded micro-batch consumer. All collaborators are injectable for testing."""

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        consumer: Any = None,
        dlq_producer: Any = None,
        minio_client: Any = None,
        conn_factory: Optional[Callable[[], Any]] = None,
        max_messages: int = 500,
        max_seconds: float = 30.0,
        idle_timeout_seconds: float = 5.0,
        startup_wait_seconds: float = 20.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_messages < 1:
            raise ValueError("max_messages must be >= 1")
        if max_seconds <= 0:
            raise ValueError("max_seconds must be > 0")
        self.config = config or get_config()
        self.topic = self.config.redpanda.orders_topic
        self.dlq_topic = self.config.redpanda.dlq_topic
        self.max_messages = max_messages
        self.max_seconds = max_seconds
        self.idle_timeout_seconds = idle_timeout_seconds
        self.startup_wait_seconds = startup_wait_seconds
        self._clock = clock
        self._consumer = consumer
        self._dlq_producer = dlq_producer
        self._minio = minio_client
        self._conn_factory = conn_factory or (lambda: get_db_connection(database="warehouse", config=self.config))

    # -- lazily built clients -------------------------------------------------------
    def _get_consumer(self) -> Any:
        if self._consumer is None:
            if Consumer is None:
                raise RuntimeError("confluent-kafka is not installed")
            self._consumer = Consumer({
                "bootstrap.servers": self.config.redpanda.broker,
                "group.id": CONSUMER_GROUP,
                "enable.auto.commit": False,         # commit manually, only after successful writes
                "auto.offset.reset": "earliest",
                "session.timeout.ms": 15000,
            })
            self._consumer.subscribe([self.topic])
        return self._consumer

    def _get_dlq_producer(self) -> Any:
        if self._dlq_producer is None:
            if Producer is None:
                raise RuntimeError("confluent-kafka is not installed")
            self._dlq_producer = Producer({"bootstrap.servers": self.config.redpanda.broker, "acks": "all"})
        return self._dlq_producer

    def _get_minio(self) -> Any:
        if self._minio is None:
            self._minio = get_minio_client(self.config)
        return self._minio

    # -- polling --------------------------------------------------------------------
    def poll_messages(self) -> Tuple[List[Any], str]:
        """Collect messages until a limit is hit. Returns (messages, stop_reason)."""
        consumer = self._get_consumer()
        started = self._clock()
        last_message_at = started
        messages: List[Any] = []
        while True:
            now = self._clock()
            if len(messages) >= self.max_messages:
                return messages, "max_messages"
            if now - started >= self.max_seconds:
                return messages, "max_seconds"
            wait_limit = self.idle_timeout_seconds if messages else self.startup_wait_seconds
            if now - last_message_at >= wait_limit:
                return messages, "idle"
            msg = consumer.poll(min(1.0, max(0.05, self.max_seconds - (now - started))))
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                if err.code() == PARTITION_EOF:
                    continue
                raise ConsumerError(f"Kafka consumer error: {err}")
            messages.append(msg)
            last_message_at = self._clock()

    # -- one micro-batch ------------------------------------------------------------
    @staticmethod
    def make_batch_id(messages: List[Any]) -> str:
        """Deterministic id from the offset ranges, so a redelivered batch reuses its file names."""
        ranges: Dict[int, List[int]] = {}
        for m in messages:
            lo_hi = ranges.setdefault(m.partition(), [m.offset(), m.offset()])
            lo_hi[0], lo_hi[1] = min(lo_hi[0], m.offset()), max(lo_hi[1], m.offset())
        key = "|".join(f"{p}:{lo}-{hi}" for p, (lo, hi) in sorted(ranges.items())) or "empty"
        return "online_" + hashlib.sha1(key.encode()).hexdigest()[:12]

    def run_microbatch(self) -> MicroBatchResult:
        started = self._clock()
        messages, stop_reason = self.poll_messages()
        batch_id = self.make_batch_id(messages)
        result = MicroBatchResult(batch_id=batch_id, consumed=len(messages), stop_reason=stop_reason)
        if not messages:
            logger.info("Micro-batch: no messages (%s)", stop_reason)
            result.elapsed_seconds = round(self._clock() - started, 2)
            return result

        valid_rows: List[Dict[str, Any]] = []
        invalid: List[Tuple[Any, ValidationOutcome]] = []
        for msg in messages:
            outcome = validate_event(msg.value())
            if outcome.is_valid:
                row = dict(outcome.event)  # type: ignore[arg-type]
                row["_kafka_partition"], row["_kafka_offset"] = msg.partition(), msg.offset()
                valid_rows.append(row)
            else:
                invalid.append((msg, outcome))

        result.valid, result.invalid = len(valid_rows), len(invalid)
        result.reject_pct = round(100.0 * result.invalid / result.consumed, 2)
        cutoff = _utcnow().date().toordinal() - LATE_AFTER_DAYS
        result.late = sum(1 for r in valid_rows if r["event_time"].date().toordinal() < cutoff)

        # --- all side effects first; offsets are committed last --------------------
        result.bronze_paths = self._write_bronze(valid_rows, batch_id)
        if invalid:
            self._write_quarantine(invalid, batch_id)
            result.dlq_sent = self._publish_dlq(invalid, batch_id)
        self._write_summary(batch_id, result)

        self._get_consumer().commit(asynchronous=False)
        result.committed = True
        result.elapsed_seconds = round(self._clock() - started, 2)
        logger.info("Micro-batch %s committed: consumed=%d valid=%d invalid=%d late=%d (%s)",
                    batch_id, result.consumed, result.valid, result.invalid, result.late, stop_reason)
        return result

    # -- sinks ----------------------------------------------------------------------
    def _write_bronze(self, rows: List[Dict[str, Any]], batch_id: str) -> List[str]:
        by_date: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            by_date.setdefault(r["event_date"], []).append(r)
        paths = []
        ingested = _utcnow()
        for event_date, group in sorted(by_date.items()):
            table = pa.table({
                "event_id": [r["event_id"] for r in group],
                "order_id": [r["order_id"] for r in group],
                "customer_id": [r["customer_id"] for r in group],
                "product_id": [r["product_id"] for r in group],
                "quantity": [r["quantity"] for r in group],
                "unit_price": [r["unit_price"] for r in group],
                "event_time": pa.array([r["event_time"] for r in group], type=pa.timestamp("us")),
                "_kafka_partition": [r["_kafka_partition"] for r in group],
                "_kafka_offset": [r["_kafka_offset"] for r in group],
                "_ingested_at": pa.array([ingested] * len(group), type=pa.timestamp("us")),
                "_batch_id": [batch_id] * len(group),
            })
            path = build_lake_path("bronze", SOURCE_NAME, dt=event_date, filename=f"part-{batch_id}.parquet")
            write_parquet(self._get_minio(), self.config.minio.bucket, path, table)
            paths.append(path)
        return paths

    def _write_quarantine(self, invalid: List[Tuple[Any, ValidationOutcome]], batch_id: str) -> None:
        records = []
        for msg, outcome in invalid:
            raw = msg.value()
            raw_text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
            records.append({
                "rule_failed": outcome.rule_failed, "reason": outcome.reason, "raw_payload": raw_text,
                "kafka_partition": msg.partition(), "kafka_offset": msg.offset(),
            })
        # lake copy (replayable) ...
        table = pa.Table.from_pylist(records)
        path = build_lake_path("quarantine", SOURCE_NAME, dt=_utcnow().date().isoformat(),
                               filename=f"quarantine_{batch_id}.parquet")
        write_parquet(self._get_minio(), self.config.minio.bucket, path, table)
        # ... and queryable copy (idempotent: replace this batch's rows)
        with self._conn_factory() as conn:
            try:
                persist_rejections(conn, SOURCE_NAME, batch_id,
                                   [{"rule_failed": r["rule_failed"], "reason": r["reason"], "raw_record": r}
                                    for r in records], replace=True)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _publish_dlq(self, invalid: List[Tuple[Any, ValidationOutcome]], batch_id: str) -> int:
        producer = self._get_dlq_producer()
        errors: List[Any] = []

        def _on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(err)

        for msg, outcome in invalid:
            raw = msg.value()
            envelope = {
                "batch_id": batch_id, "rule_failed": outcome.rule_failed, "reason": outcome.reason,
                "source_topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                "original_payload": raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.produce(self.dlq_topic, key=msg.key(), value=json.dumps(envelope).encode("utf-8"),
                             callback=_on_delivery)
            producer.poll(0)
        remaining = producer.flush(10)
        if errors or (isinstance(remaining, int) and remaining > 0):
            raise RuntimeError(f"DLQ publish failed (errors={len(errors)}, undelivered={remaining})")
        return len(invalid)

    def _write_summary(self, batch_id: str, result: MicroBatchResult) -> None:
        with self._conn_factory() as conn:
            write_quality_summary(
                source=SOURCE_NAME, batch_id=batch_id, total=result.consumed, accepted=result.valid,
                rejected=result.invalid, per_rule_counts={}, conn=conn, config=self.config)

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()


def run_microbatch(max_messages: int = 500, max_seconds: float = 30.0) -> Dict[str, Any]:
    """Airflow / CLI entrypoint: consume one micro-batch and return its counts."""
    consumer = OrderStreamConsumer(max_messages=max_messages, max_seconds=max_seconds)
    try:
        return consumer.run_microbatch().to_dict()
    finally:
        consumer.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Consume one micro-batch from orders_stream.")
    parser.add_argument("--max-messages", type=int, default=500)
    parser.add_argument("--max-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    print(json.dumps(run_microbatch(args.max_messages, args.max_seconds), indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
