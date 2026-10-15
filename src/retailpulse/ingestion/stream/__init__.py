"""Streaming ingestion package for RetailPulse."""

from retailpulse.ingestion.stream.order_generator import (
    GeneratedOrderEvent,
    OrderGenerator,
    generate_order_event,
)

__all__ = [
    "GeneratedOrderEvent",
    "OrderGenerator",
    "OrderProducer",
    "generate_order_event",
]


def __getattr__(name: str):
    if name == "OrderProducer":
        from retailpulse.ingestion.stream.order_producer import OrderProducer

        return OrderProducer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
