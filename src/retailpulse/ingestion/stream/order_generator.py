"""Order event generator for simulated streaming ingestion.

Generates simulated online-store order events with configurable defect rates
(valid, invalid, late) for downstream validation and DLQ testing.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, NamedTuple, Optional, Union

from retailpulse.common.config import RedpandaConfig
from retailpulse.generators.demo import CUSTOMER_COUNT, PRODUCT_COUNT


class GeneratedOrderEvent(NamedTuple):
    """Container for a generated order event.

    Attributes:
        payload: Serialized raw bytes to send to the broker.
        event_type: Category of the event ('valid', 'invalid', 'late').
        defect_type: Specific defect category if invalid, else None.
        data: Parsed dictionary for structured events, None for malformed JSON.
        key: Suggested Kafka partition key (order_id as string or event_id).
    """

    payload: bytes
    event_type: str
    defect_type: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    key: Optional[str] = None


class OrderGenerator:
    """Configurable generator for simulated order events."""

    def __init__(
        self,
        invalid_rate: float = 0.03,
        late_rate: float = 0.02,
        seed: Optional[int] = None,
    ) -> None:
        """Initialize OrderGenerator.

        Args:
            invalid_rate: Fraction of events that are defective (0.0 to 1.0).
            late_rate: Fraction of events that have late timestamps (0.0 to 1.0).
            seed: Optional seed for reproducible pseudo-random generation.
        """
        if invalid_rate < 0 or invalid_rate > 1:
            raise ValueError(f"invalid_rate must be between 0.0 and 1.0, got {invalid_rate}")
        if late_rate < 0 or late_rate > 1:
            raise ValueError(f"late_rate must be between 0.0 and 1.0, got {late_rate}")
        if invalid_rate + late_rate > 1.0:
            raise ValueError(
                f"Combined invalid ({invalid_rate}) and late ({late_rate}) rates exceed 1.0"
            )

        self.invalid_rate = invalid_rate
        self.late_rate = late_rate
        self.rng = random.Random(seed)
        self._next_order_id = 10000

    @classmethod
    def from_config(cls, config: RedpandaConfig, seed: Optional[int] = None) -> OrderGenerator:
        """Create an OrderGenerator from RedpandaConfig."""
        return cls(
            invalid_rate=config.producer_invalid_rate,
            late_rate=config.producer_late_rate,
            seed=seed,
        )

    def _next_id(self) -> int:
        self._next_order_id += 1
        return self._next_order_id

    def generate_valid_event(
        self,
        event_time: Optional[datetime] = None,
        order_id: Optional[int] = None,
    ) -> GeneratedOrderEvent:
        """Generate a fully valid order event.

        Args:
            event_time: Optional explicit timestamp (defaults to current UTC).
            order_id: Optional explicit order_id.

        Returns:
            GeneratedOrderEvent with valid payload and metadata.
        """
        oid = order_id if order_id is not None else self._next_id()
        now = event_time if event_time is not None else datetime.now(timezone.utc)
        event_id = str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

        data = {
            "event_id": event_id,
            "order_id": oid,
            "customer_id": self.rng.randint(1, CUSTOMER_COUNT),
            "product_id": self.rng.randint(1, PRODUCT_COUNT),
            "quantity": self.rng.randint(1, 10),
            "unit_price": round(self.rng.uniform(5.0, 499.0), 2),
            "event_time": now.isoformat(),
        }

        payload = json.dumps(data).encode("utf-8")
        return GeneratedOrderEvent(
            payload=payload,
            event_type="valid",
            defect_type=None,
            data=data,
            key=str(oid),
        )

    def generate_late_event(self) -> GeneratedOrderEvent:
        """Generate an event with a late timestamp (7 to 30 days in the past).

        Returns:
            GeneratedOrderEvent marked as 'late'.
        """
        days_ago = self.rng.randint(7, 30)
        seconds_ago = self.rng.randint(0, 86399)
        late_time = datetime.now(timezone.utc) - timedelta(days=days_ago, seconds=seconds_ago)

        valid_evt = self.generate_valid_event(event_time=late_time)
        return GeneratedOrderEvent(
            payload=valid_evt.payload,
            event_type="late",
            defect_type=None,
            data=valid_evt.data,
            key=valid_evt.key,
        )

    def generate_invalid_event(
        self,
        defect_type: Optional[str] = None,
    ) -> GeneratedOrderEvent:
        """Generate an invalid event covering specified or randomly selected defect.

        Defect types:
            - 'malformed_json': Corrupted raw string/bytes not parsable as JSON.
            - 'missing_field': Omits one of the required fields.
            - 'non_positive_quantity': Sets quantity <= 0.
            - 'non_positive_price': Sets unit_price <= 0.

        Args:
            defect_type: Optional defect subtype. If None, chosen randomly.

        Returns:
            GeneratedOrderEvent marked as 'invalid'.
        """
        valid_defects = [
            "malformed_json",
            "missing_field",
            "non_positive_quantity",
            "non_positive_price",
        ]
        chosen_defect = defect_type if defect_type else self.rng.choice(valid_defects)

        if chosen_defect not in valid_defects:
            raise ValueError(
                f"Unknown defect_type '{chosen_defect}'. Must be one of {valid_defects}"
            )

        oid = self._next_id()
        event_id = str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
        now = datetime.now(timezone.utc)

        if chosen_defect == "malformed_json":
            # Must be sent as raw string/bytes payload, not through json.dumps()
            corruptions = [
                f'{{"event_id": "{event_id}", "order_id": {oid}, "customer_id": 42, "malformed_json_unclosed',
                f'{{"event_id": "{event_id}", order_id: {oid}, customer_id: 42}}',  # unquoted key
                f'{{"event_id": "{event_id}", "order_id": {oid},,,}}',  # invalid comma syntax
                f'NOT_JSON_AT_ALL_EVENT_{event_id}',
            ]
            raw_str = self.rng.choice(corruptions)
            return GeneratedOrderEvent(
                payload=raw_str.encode("utf-8"),
                event_type="invalid",
                defect_type="malformed_json",
                data=None,
                key=str(oid),
            )

        # Base dictionary for field/value defects
        data: Dict[str, Any] = {
            "event_id": event_id,
            "order_id": oid,
            "customer_id": self.rng.randint(1, CUSTOMER_COUNT),
            "product_id": self.rng.randint(1, PRODUCT_COUNT),
            "quantity": self.rng.randint(1, 10),
            "unit_price": round(self.rng.uniform(5.0, 499.0), 2),
            "event_time": now.isoformat(),
        }

        if chosen_defect == "missing_field":
            fields_to_omit = ["order_id", "customer_id", "product_id", "quantity", "unit_price"]
            field_omitted = self.rng.choice(fields_to_omit)
            del data[field_omitted]
            payload = json.dumps(data).encode("utf-8")
            return GeneratedOrderEvent(
                payload=payload,
                event_type="invalid",
                defect_type="missing_field",
                data=data,
                key=str(oid),
            )

        if chosen_defect == "non_positive_quantity":
            bad_quantity = self.rng.choice([0, -1, -2, -5, -10])
            data["quantity"] = bad_quantity
            payload = json.dumps(data).encode("utf-8")
            return GeneratedOrderEvent(
                payload=payload,
                event_type="invalid",
                defect_type="non_positive_quantity",
                data=data,
                key=str(oid),
            )

        if chosen_defect == "non_positive_price":
            bad_price = self.rng.choice([0.0, -0.01, -15.50, -100.0])
            data["unit_price"] = bad_price
            payload = json.dumps(data).encode("utf-8")
            return GeneratedOrderEvent(
                payload=payload,
                event_type="invalid",
                defect_type="non_positive_price",
                data=data,
                key=str(oid),
            )

        raise RuntimeError(f"Unhandled defect type: {chosen_defect}")

    def generate_event(self) -> GeneratedOrderEvent:
        """Generate the next order event according to configured probabilities.

        Returns:
            GeneratedOrderEvent (valid, invalid, or late).
        """
        r = self.rng.random()
        if r < self.invalid_rate:
            return self.generate_invalid_event()
        if r < self.invalid_rate + self.late_rate:
            return self.generate_late_event()
        return self.generate_valid_event()


def generate_order_event(
    invalid_rate: float = 0.03,
    late_rate: float = 0.02,
    seed: Optional[int] = None,
) -> GeneratedOrderEvent:
    """Convenience function to generate a single order event."""
    gen = OrderGenerator(invalid_rate=invalid_rate, late_rate=late_rate, seed=seed)
    return gen.generate_event()
