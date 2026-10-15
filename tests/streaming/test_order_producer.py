"""Comprehensive tests for streaming order generator and order producer.

Tests schema validity, defect injection, statistical defect distribution,
mocked Kafka delivery callbacks, buffer handling, graceful shutdown, and CLI.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from retailpulse.common.config import RedpandaConfig
from retailpulse.ingestion.stream.order_generator import (
    GeneratedOrderEvent,
    OrderGenerator,
    generate_order_event,
)
from retailpulse.ingestion.stream.order_producer import OrderProducer


# ---------------------------------------------------------------------------
# Generator Tests
# ---------------------------------------------------------------------------


class TestOrderGenerator:
    """Tests for OrderGenerator schema, defects, and probability distribution."""

    def test_valid_event_schema_and_types(self) -> None:
        """Valid events must strictly adhere to the required schema and data types."""
        gen = OrderGenerator(seed=123)
        event = gen.generate_valid_event()

        assert event.event_type == "valid"
        assert event.defect_type is None
        assert isinstance(event.payload, bytes)
        assert event.data is not None

        data = event.data
        # Field presence
        required_fields = {
            "event_id",
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "event_time",
        }
        assert required_fields.issubset(data.keys())

        # UUID format
        parsed_uuid = uuid.UUID(data["event_id"])
        assert parsed_uuid.version == 4

        # Types and values
        assert isinstance(data["order_id"], int) and data["order_id"] > 0
        assert isinstance(data["customer_id"], int) and data["customer_id"] > 0
        assert isinstance(data["product_id"], int) and data["product_id"] > 0
        assert isinstance(data["quantity"], int) and data["quantity"] > 0
        assert isinstance(data["unit_price"], float) and data["unit_price"] > 0

        # ISO 8601 UTC timestamp
        event_dt = datetime.fromisoformat(data["event_time"])
        assert event_dt.tzinfo is not None
        now_utc = datetime.now(timezone.utc)
        assert abs((now_utc - event_dt).total_seconds()) < 60

        # JSON deserialization
        deserialized = json.loads(event.payload.decode("utf-8"))
        assert deserialized == data
        assert event.key == str(data["order_id"])

    def test_malformed_json_defect(self) -> None:
        """Malformed JSON must be emitted as raw corrupt bytes and fail json.loads."""
        gen = OrderGenerator(seed=456)
        event = gen.generate_invalid_event(defect_type="malformed_json")

        assert event.event_type == "invalid"
        assert event.defect_type == "malformed_json"
        assert event.data is None
        assert isinstance(event.payload, bytes)

        # Must fail JSON parsing
        with pytest.raises((json.JSONDecodeError, UnicodeDecodeError)):
            json.loads(event.payload.decode("utf-8"))

    def test_missing_field_defect(self) -> None:
        """Missing-field events must omit one required field and parse as JSON."""
        gen = OrderGenerator(seed=789)
        event = gen.generate_invalid_event(defect_type="missing_field")

        assert event.event_type == "invalid"
        assert event.defect_type == "missing_field"
        assert event.data is not None

        # Verify valid JSON
        parsed = json.loads(event.payload.decode("utf-8"))
        core_fields = {"order_id", "customer_id", "product_id", "quantity", "unit_price"}
        present_core = core_fields.intersection(parsed.keys())
        assert len(present_core) < len(core_fields), "One required field must be omitted"

    def test_non_positive_quantity_defect(self) -> None:
        """Non-positive quantity defect must have quantity <= 0."""
        gen = OrderGenerator(seed=101)
        event = gen.generate_invalid_event(defect_type="non_positive_quantity")

        assert event.event_type == "invalid"
        assert event.defect_type == "non_positive_quantity"
        assert event.data is not None
        assert event.data["quantity"] <= 0

        parsed = json.loads(event.payload.decode("utf-8"))
        assert parsed["quantity"] <= 0

    def test_non_positive_price_defect(self) -> None:
        """Non-positive price defect must have unit_price <= 0."""
        gen = OrderGenerator(seed=202)
        event = gen.generate_invalid_event(defect_type="non_positive_price")

        assert event.event_type == "invalid"
        assert event.defect_type == "non_positive_price"
        assert event.data is not None
        assert event.data["unit_price"] <= 0.0

        parsed = json.loads(event.payload.decode("utf-8"))
        assert parsed["unit_price"] <= 0.0

    def test_late_event_timestamp(self) -> None:
        """Late events must have event_time between 7 and 30 days in the past."""
        gen = OrderGenerator(seed=303)
        event = gen.generate_late_event()

        assert event.event_type == "late"
        assert event.defect_type is None
        assert event.data is not None

        event_dt = datetime.fromisoformat(event.data["event_time"])
        now_utc = datetime.now(timezone.utc)
        diff = now_utc - event_dt

        assert diff >= timedelta(days=6, hours=23), "Must be at least ~7 days in past"
        assert diff <= timedelta(days=31), "Must be no more than ~30 days in past"

    def test_statistical_defect_distribution(self) -> None:
        """Sample over 2,000 events must match ~95% valid, ~3% invalid, ~2% late."""
        gen = OrderGenerator(invalid_rate=0.03, late_rate=0.02, seed=999)
        sample_size = 2000

        counts: Dict[str, int] = {"valid": 0, "invalid": 0, "late": 0}
        defect_counts: Dict[str, int] = {}

        for _ in range(sample_size):
            event = gen.generate_event()
            counts[event.event_type] += 1
            if event.defect_type:
                defect_counts[event.defect_type] = defect_counts.get(event.defect_type, 0) + 1

        invalid_pct = counts["invalid"] / sample_size
        late_pct = counts["late"] / sample_size
        valid_pct = counts["valid"] / sample_size

        # Tolerances based on binomial standard errors for n=2000
        # Expected invalid ~0.03 (±0.015), late ~0.02 (±0.012), valid ~0.95 (±0.02)
        assert 0.015 <= invalid_pct <= 0.050, f"Invalid rate {invalid_pct:.3f} out of expected 0.03"
        assert 0.008 <= late_pct <= 0.040, f"Late rate {late_pct:.3f} out of expected 0.02"
        assert 0.920 <= valid_pct <= 0.970, f"Valid rate {valid_pct:.3f} out of expected 0.95"

        # All 4 defect types must have been generated
        expected_defects = {
            "malformed_json",
            "missing_field",
            "non_positive_quantity",
            "non_positive_price",
        }
        assert expected_defects.issubset(defect_counts.keys())

    def test_invalid_rate_validation(self) -> None:
        """OrderGenerator must reject negative rates or total rate > 1.0."""
        with pytest.raises(ValueError, match="invalid_rate must be between 0.0 and 1.0"):
            OrderGenerator(invalid_rate=-0.01)

        with pytest.raises(ValueError, match="late_rate must be between 0.0 and 1.0"):
            OrderGenerator(late_rate=1.05)

        with pytest.raises(ValueError, match="rates exceed 1.0"):
            OrderGenerator(invalid_rate=0.7, late_rate=0.4)

    def test_convenience_function(self) -> None:
        """Top-level generate_order_event helper function works as expected."""
        evt = generate_order_event(seed=42)
        assert isinstance(evt, GeneratedOrderEvent)
        assert evt.event_type in ("valid", "invalid", "late")


# ---------------------------------------------------------------------------
# Mock Kafka Producer Fixtures
# ---------------------------------------------------------------------------


class MockKafkaMessage:
    """Mock confluent_kafka Message."""

    def __init__(self, topic: str = "orders_stream", key: str = "101", offset: int = 1) -> None:
        self._topic = topic
        self._key = key.encode("utf-8") if isinstance(key, str) else key
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key

    def offset(self) -> int:
        return self._offset

    def partition(self) -> int:
        return 0


class MockKafkaError:
    """Mock confluent_kafka KafkaError."""

    def __init__(self, msg: str = "Broker not available", code: int = -1) -> None:
        self._msg = msg
        self._code = code

    def str(self) -> str:
        return self._msg

    def code(self) -> int:
        return self._code


class MockProducer:
    """Mock confluent_kafka Producer implementing produce, poll, and flush."""

    def __init__(self) -> None:
        self.produced_messages: List[Dict[str, Any]] = []
        self.callbacks: List[Callable[..., Any]] = []
        self.poll_calls: List[float] = []
        self.flush_calls: List[float] = []
        self.fail_produce_times = 0
        self.fail_error = BufferError("Local: Queue full")

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        callback: Optional[Callable[..., Any]] = None,
    ) -> None:
        if self.fail_produce_times > 0:
            self.fail_produce_times -= 1
            raise self.fail_error

        msg = {"topic": topic, "key": key, "value": value, "callback": callback}
        self.produced_messages.append(msg)
        if callback is not None:
            self.callbacks.append(callback)

    def poll(self, timeout: float = 0) -> int:
        self.poll_calls.append(timeout)
        # Execute callbacks immediately in mock
        while self.callbacks:
            cb = self.callbacks.pop(0)
            mock_msg = MockKafkaMessage()
            cb(None, mock_msg)
        return 0

    def flush(self, timeout: float = 10.0) -> int:
        self.flush_calls.append(timeout)
        self.poll(0)
        return 0


# ---------------------------------------------------------------------------
# OrderProducer Tests
# ---------------------------------------------------------------------------


class TestOrderProducer:
    """Tests for OrderProducer logic without requiring a live Kafka broker."""

    @pytest.fixture
    def mock_producer(self) -> MockProducer:
        return MockProducer()

    @pytest.fixture
    def test_config(self) -> RedpandaConfig:
        return RedpandaConfig(
            broker="mock-broker:9092",
            orders_topic="orders_stream",
            dlq_topic="orders_dlq",
            producer_rate_per_sec=100.0,
            producer_invalid_rate=0.05,
            producer_late_rate=0.05,
        )

    def test_producer_initialization(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """Producer configures broker, topic, generator and rate properly."""
        producer = OrderProducer(
            config=test_config,
            producer=mock_producer,
            topic="custom_orders",
            broker="custom_broker:9092",
            rate_per_sec=20.0,
        )
        assert producer.topic == "custom_orders"
        assert producer.broker == "custom_broker:9092"
        assert producer.rate_per_sec == 20.0
        assert producer.events_produced == 0
        assert producer.events_delivered == 0
        assert producer.delivery_errors == 0

    def test_produce_one_success(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """produce_one() should generate and send one message with callback registered."""
        producer = OrderProducer(config=test_config, producer=mock_producer)
        event = producer.produce_one()

        assert producer.events_produced == 1
        assert len(mock_producer.produced_messages) == 1
        sent = mock_producer.produced_messages[0]
        assert sent["topic"] == "orders_stream"
        assert sent["value"] == event.payload
        assert sent["key"] == event.key.encode("utf-8")
        assert sent["callback"] == producer._delivery_callback

    def test_delivery_callback_success(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """Successful delivery callback increments events_delivered."""
        producer = OrderProducer(config=test_config, producer=mock_producer)
        msg = MockKafkaMessage(topic="orders_stream", key="test_key", offset=42)

        producer._delivery_callback(None, msg)
        assert producer.events_delivered == 1
        assert producer.delivery_errors == 0

    def test_delivery_callback_error(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """Failed delivery callback increments delivery_errors."""
        producer = OrderProducer(config=test_config, producer=mock_producer)
        msg = MockKafkaMessage(topic="orders_stream", key="test_key")
        err = MockKafkaError(msg="Message timed out", code=1)

        producer._delivery_callback(err, msg)
        assert producer.delivery_errors == 1
        assert producer.events_delivered == 0

    def test_buffer_error_retry_and_recovery(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """When produce() raises BufferError once, it polls and retries successfully."""
        mock_producer.fail_produce_times = 1
        producer = OrderProducer(config=test_config, producer=mock_producer)

        event = producer.produce_one()
        assert producer.events_produced == 1
        assert len(mock_producer.produced_messages) == 1
        assert 0.5 in mock_producer.poll_calls

    def test_buffer_error_exhausted_retries(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """When BufferError persists past retry limit, it raises BufferError and records error."""
        mock_producer.fail_produce_times = 10
        producer = OrderProducer(config=test_config, producer=mock_producer)

        with pytest.raises(BufferError):
            producer.produce_one()

        assert producer.events_produced == 0
        assert producer.delivery_errors == 1

    def test_run_with_max_events(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """run() should terminate after producing max_events and flush buffer."""
        producer = OrderProducer(config=test_config, producer=mock_producer, rate_per_sec=500.0)

        stats = producer.run(max_events=15)
        assert stats["produced"] == 15
        assert producer.events_produced == 15
        assert len(mock_producer.flush_calls) >= 1

    def test_graceful_shutdown_via_stop(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """Calling stop() halts the loop and triggers flush."""
        producer = OrderProducer(config=test_config, producer=mock_producer, rate_per_sec=100.0)

        def stop_after_produce() -> None:
            if producer.events_produced >= 3:
                producer.stop()

        original_produce = producer.produce_one

        def wrapped_produce() -> GeneratedOrderEvent:
            evt = original_produce()
            stop_after_produce()
            return evt

        producer.produce_one = wrapped_produce  # type: ignore[assignment]
        stats = producer.run(max_events=100)

        assert stats["produced"] >= 3
        assert len(mock_producer.flush_calls) >= 1

    def test_rate_validation(self, mock_producer: MockProducer, test_config: RedpandaConfig) -> None:
        """Negative or zero rate_per_sec must raise ValueError."""
        with pytest.raises(ValueError, match="rate_per_sec must be positive"):
            OrderProducer(config=test_config, producer=mock_producer, rate_per_sec=0)

        with pytest.raises(ValueError, match="rate_per_sec must be positive"):
            OrderProducer(config=test_config, producer=mock_producer, rate_per_sec=-5.0)

    def test_cli_execution_with_max_events(self, test_config: RedpandaConfig) -> None:
        """CLI main() should run cleanly with mock producer."""
        from retailpulse.ingestion.stream import order_producer

        mock_instance = MockProducer()
        with patch.object(order_producer, "Producer", return_value=mock_instance):
            with patch(
                "sys.argv",
                [
                    "order_producer",
                    "--max-events",
                    "3",
                    "--rate",
                    "100.0",
                    "--topic",
                    "orders_stream",
                ],
            ):
                order_producer.main()

        assert len(mock_instance.produced_messages) == 3
        assert len(mock_instance.flush_calls) >= 1
