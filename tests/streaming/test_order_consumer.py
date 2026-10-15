"""Tests for the streaming consumer: validation, micro-batch limits, DLQ, commit semantics."""

from __future__ import annotations

import io
import json
from contextlib import contextmanager
from typing import List, Optional
from unittest import mock

import pyarrow.parquet as pq
import pytest

from retailpulse.ingestion.stream.order_consumer import (
    ConsumerError,
    OrderStreamConsumer,
    parse_event_time,
    validate_event,
)


def event(**overrides) -> dict:
    base = {"event_id": "e-1", "order_id": 1001, "customer_id": 7, "product_id": 3, "quantity": 2,
            "unit_price": 49.5, "event_time": "2026-10-14T10:30:00+00:00"}
    base.update(overrides)
    return base


def payload(**overrides) -> bytes:
    data = event(**overrides)
    for k in [k for k, v in data.items() if v is ...]:
        del data[k]
    return json.dumps(data).encode()


# ------------------------------------------------------------------ validate_event
class TestValidateEvent:
    def test_valid_event_is_normalised(self):
        out = validate_event(payload())
        assert out.is_valid
        assert out.event["order_id"] == 1001 and out.event["quantity"] == 2
        assert out.event["event_date"] == "2026-10-14"

    def test_timezone_converted_to_utc(self):
        assert parse_event_time("2026-10-14T23:30:00-05:00").isoformat() == "2026-10-15T04:30:00"
        assert parse_event_time("2026-10-14T10:00:00Z").isoformat() == "2026-10-14T10:00:00"

    @pytest.mark.parametrize("raw", [b"NOT_JSON", b'{"a": 1,,}', b"\xff\xfe", b"[1,2]"])
    def test_malformed_json(self, raw):
        assert validate_event(raw).rule_failed == "malformed_json"

    @pytest.mark.parametrize("raw", [None, b"", ""])
    def test_empty_payload(self, raw):
        assert validate_event(raw).rule_failed == "empty_payload"

    @pytest.mark.parametrize("field", ["event_id", "order_id", "customer_id", "product_id",
                                       "quantity", "unit_price", "event_time"])
    def test_missing_field(self, field):
        out = validate_event(payload(**{field: ...}))
        assert out.rule_failed == f"missing_field:{field}"

    @pytest.mark.parametrize("qty", [0, -1, -10])
    def test_non_positive_quantity(self, qty):
        assert validate_event(payload(quantity=qty)).rule_failed == "range:quantity_positive"

    @pytest.mark.parametrize("price", [0, -0.01, -100])
    def test_non_positive_price(self, price):
        assert validate_event(payload(unit_price=price)).rule_failed == "range:unit_price_positive"

    def test_wrong_types(self):
        assert validate_event(payload(quantity="two")).rule_failed == "type:quantity"
        assert validate_event(payload(quantity=True)).rule_failed == "type:quantity"
        assert validate_event(payload(unit_price="9.99")).rule_failed == "type:unit_price"
        assert validate_event(payload(order_id="abc")).rule_failed == "type:order_id"
        assert validate_event(payload(customer_id=-5)).rule_failed == "type:customer_id"

    def test_bad_event_time(self):
        assert validate_event(payload(event_time="yesterday")).rule_failed == "format:event_time"


# ------------------------------------------------------------------ fakes
class FakeError:
    def __init__(self, code): self._code = code
    def code(self): return self._code
    def __str__(self): return f"KafkaError({self._code})"


class FakeMessage:
    def __init__(self, offset: int, value: Optional[bytes], partition: int = 0, error=None):
        self._o, self._v, self._p, self._e = offset, value, partition, error

    def value(self): return self._v
    def key(self): return str(self._o).encode()
    def offset(self): return self._o
    def partition(self): return self._p
    def topic(self): return "orders_stream"
    def error(self): return self._e


class FakeConsumer:
    def __init__(self, messages: List[FakeMessage], on_poll=None):
        self.queue = list(messages)
        self.commits = 0
        self.polls = 0
        self.on_poll = on_poll

    def poll(self, timeout):
        self.polls += 1
        if self.on_poll:
            self.on_poll()
        return self.queue.pop(0) if self.queue else None

    def commit(self, asynchronous=False):
        self.commits += 1

    def close(self): pass


class FakeProducer:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def produce(self, topic, key=None, value=None, callback=None):
        self.sent.append((topic, key, json.loads(value)))
        if callback:
            callback("boom" if self.fail else None, None)

    def poll(self, t): pass
    def flush(self, t): return 0


class Clock:
    def __init__(self): self.t = 0.0
    def __call__(self): return self.t
    def advance(self, s): self.t += s


@contextmanager
def _conn_ctx(conn):
    yield conn


def make_consumer(app_config, fake_minio, messages, producer=None, conn=None, clock=None, **kw):
    conn = conn or mock.MagicMock()
    consumer = FakeConsumer(messages)
    clock = clock or Clock()
    consumer.on_poll = lambda: clock.advance(0.1)       # time always moves, so loops always end
    oc = OrderStreamConsumer(
        config=app_config, consumer=consumer, dlq_producer=producer or FakeProducer(),
        minio_client=fake_minio, conn_factory=lambda: _conn_ctx(conn),
        clock=clock, **kw)
    return oc, consumer, conn


def good_messages(n: int, start: int = 0) -> List[FakeMessage]:
    return [FakeMessage(start + i, payload(event_id=f"e-{start + i}", order_id=2000 + start + i)) for i in range(n)]


# ------------------------------------------------------------------ micro-batch limits
class TestMicroBatchLimits:
    def test_stops_at_max_messages(self, app_config, fake_minio):
        oc, consumer, _ = make_consumer(app_config, fake_minio, good_messages(100), max_messages=10)
        result = oc.run_microbatch()
        assert result.consumed == 10 and result.stop_reason == "max_messages"
        assert len(consumer.queue) == 90          # the rest stays on the topic for the next run

    def test_stops_at_max_seconds(self, app_config, fake_minio):
        clock = Clock()
        oc, consumer, _ = make_consumer(app_config, fake_minio, good_messages(1000), clock=clock,
                                        max_messages=1000, max_seconds=5)
        consumer.on_poll = lambda: clock.advance(1.0)          # each poll "takes" one second
        result = oc.run_microbatch()
        assert result.stop_reason == "max_seconds"
        assert 3 <= result.consumed <= 6

    def test_stops_when_idle_after_messages(self, app_config, fake_minio):
        clock = Clock()
        oc, consumer, _ = make_consumer(app_config, fake_minio, good_messages(3), clock=clock,
                                        max_seconds=60, idle_timeout_seconds=5)
        consumer.on_poll = lambda: clock.advance(1.0)
        result = oc.run_microbatch()
        assert result.consumed == 3 and result.stop_reason == "idle"

    def test_empty_topic_returns_quickly_without_commit(self, app_config, fake_minio):
        clock = Clock()
        oc, consumer, _ = make_consumer(app_config, fake_minio, [], clock=clock, startup_wait_seconds=3)
        consumer.on_poll = lambda: clock.advance(1.0)
        result = oc.run_microbatch()
        assert result.consumed == 0 and result.committed is False and consumer.commits == 0

    def test_partition_eof_is_ignored_and_errors_raise(self, app_config, fake_minio):
        msgs = [FakeMessage(0, None, error=FakeError(-191))] + good_messages(1)
        oc, _, _ = make_consumer(app_config, fake_minio, msgs, max_messages=1)
        assert oc.run_microbatch().consumed == 1
        oc2, _, _ = make_consumer(app_config, fake_minio, [FakeMessage(0, None, error=FakeError(-195))])
        with pytest.raises(ConsumerError):
            oc2.run_microbatch()


# ------------------------------------------------------------------ routing + commit semantics
class TestRoutingAndCommit:
    def mixed(self):
        return good_messages(8) + [
            FakeMessage(8, b"NOT_JSON"),
            FakeMessage(9, payload(quantity=-3, event_id="bad-q")),
        ]

    def test_valid_to_bronze_invalid_to_dlq_and_quarantine(self, app_config, fake_minio):
        producer = FakeProducer()
        oc, consumer, conn = make_consumer(app_config, fake_minio, self.mixed(), producer=producer)
        result = oc.run_microbatch()

        assert (result.consumed, result.valid, result.invalid) == (10, 8, 2)
        assert result.reject_pct == 20.0 and result.dlq_sent == 2
        assert result.committed and consumer.commits == 1

        # bronze parquet written under the event date partition
        key = f"retailpulse/bronze/online_orders/dt=2026-10-14/part-{result.batch_id}.parquet"
        assert key in fake_minio.objects
        table = pq.read_table(io.BytesIO(fake_minio.objects[key]))
        assert table.num_rows == 8 and "event_id" in table.column_names and "_batch_id" in table.column_names

        # DLQ envelopes carry reason + original payload
        assert [m[0] for m in producer.sent] == ["orders_dlq", "orders_dlq"]
        assert producer.sent[0][2]["rule_failed"] == "malformed_json"
        assert producer.sent[0][2]["original_payload"] == "NOT_JSON"
        assert producer.sent[1][2]["rule_failed"] == "range:quantity_positive"

        # quarantine lake copy + Postgres rows
        assert any("quarantine/online_orders" in k for k in fake_minio.objects)
        cur = conn.cursor.return_value.__enter__.return_value
        assert cur.executemany.call_count == 1
        assert len(cur.executemany.call_args[0][1]) == 2

    def test_no_commit_when_bronze_write_fails(self, app_config, fake_minio):
        fake_minio.put_object = mock.Mock(side_effect=OSError("minio down"))
        oc, consumer, _ = make_consumer(app_config, fake_minio, good_messages(5))
        with pytest.raises(OSError):
            oc.run_microbatch()
        assert consumer.commits == 0

    def test_no_commit_when_dlq_publish_fails(self, app_config, fake_minio):
        oc, consumer, _ = make_consumer(app_config, fake_minio, self.mixed(), producer=FakeProducer(fail=True))
        with pytest.raises(RuntimeError, match="DLQ publish failed"):
            oc.run_microbatch()
        assert consumer.commits == 0

    def test_no_commit_when_quarantine_db_fails(self, app_config, fake_minio):
        conn = mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value.executemany.side_effect = RuntimeError("db down")
        oc, consumer, _ = make_consumer(app_config, fake_minio, self.mixed(), conn=conn)
        with pytest.raises(RuntimeError):
            oc.run_microbatch()
        assert consumer.commits == 0

    def test_batch_id_is_deterministic_for_redelivery(self, app_config, fake_minio):
        a = OrderStreamConsumer.make_batch_id(good_messages(5, start=40))
        b = OrderStreamConsumer.make_batch_id(good_messages(5, start=40))
        c = OrderStreamConsumer.make_batch_id(good_messages(5, start=45))
        assert a == b and a != c

    def test_late_events_are_counted_but_accepted(self, app_config, fake_minio):
        msgs = [FakeMessage(0, payload(event_time="2020-01-01T00:00:00Z"))]
        oc, _, _ = make_consumer(app_config, fake_minio, msgs)
        result = oc.run_microbatch()
        assert result.valid == 1 and result.late == 1
        assert any("dt=2020-01-01" in k for k in fake_minio.objects)
