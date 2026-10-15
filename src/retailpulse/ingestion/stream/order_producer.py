"""Order event producer publishing simulated stream events to Redpanda / Kafka.

Publishes orders to the configured orders topic with delivery callbacks,
graceful shutdown on SIGINT/SIGTERM, error handling, and rate limiting.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
from typing import Any, Callable, Dict, Optional

try:
    from confluent_kafka import KafkaError, Producer
except ImportError:  # pragma: no cover
    KafkaError = None  # type: ignore[assignment, misc]
    Producer = None  # type: ignore[assignment, misc]

from retailpulse.common.config import RedpandaConfig
from retailpulse.common.logging import get_logger, setup_logging
from retailpulse.ingestion.stream.order_generator import GeneratedOrderEvent, OrderGenerator

logger = get_logger("ingestion.stream.order_producer")


class OrderProducer:
    """Publishes simulated order events to Kafka/Redpanda."""

    def __init__(
        self,
        config: Optional[RedpandaConfig] = None,
        producer: Optional[Any] = None,
        generator: Optional[OrderGenerator] = None,
        topic: Optional[str] = None,
        broker: Optional[str] = None,
        rate_per_sec: Optional[float] = None,
        in_container: bool = False,
    ) -> None:
        """Initialize OrderProducer.

        Args:
            config: Optional RedpandaConfig. If None, builds from env vars.
            producer: Optional injected confluent_kafka.Producer or mock.
            generator: Optional injected OrderGenerator.
            topic: Optional topic override.
            broker: Optional broker address override.
            rate_per_sec: Optional rate override.
            in_container: True if executing in Docker container environment.
        """
        self.config = config or self._load_default_config()
        self.topic = topic or self.config.orders_topic
        self.broker = broker or self.config.get_broker(in_container=in_container)
        self.rate_per_sec = (
            rate_per_sec
            if rate_per_sec is not None
            else self.config.producer_rate_per_sec
        )

        if self.rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec must be positive, got {self.rate_per_sec}")

        self.generator = generator or OrderGenerator.from_config(self.config)

        # Producer client initialization
        if producer is not None:
            self.producer = producer
        else:
            if Producer is None:
                raise RuntimeError(
                    "confluent-kafka is not installed. Install confluent-kafka==2.8.2 or supply a mock producer."
                )
            producer_conf = {
                "bootstrap.servers": self.broker,
                "client.id": "retailpulse-order-producer",
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 200,
                "queue.buffering.max.messages": 100000,
            }
            logger.info("Initializing Kafka Producer connecting to %s", self.broker)
            self.producer = Producer(producer_conf)

        self._running = False
        self.events_produced = 0
        self.events_delivered = 0
        self.delivery_errors = 0
        self._stats_lock = threading.Lock()

    @staticmethod
    def _load_default_config() -> RedpandaConfig:
        """Build RedpandaConfig from environment variables without requiring DB configs."""
        broker = os.getenv("REDPANDA_BROKER", "localhost:19092")
        container_broker = os.getenv("REDPANDA_CONTAINER_BROKER", "redpanda:9092")
        orders_topic = os.getenv("REDPANDA_ORDERS_TOPIC", "orders_stream")
        dlq_topic = os.getenv("REDPANDA_DLQ_TOPIC", "orders_dlq")

        try:
            rate = float(os.getenv("PRODUCER_RATE_PER_SEC", "5.0"))
        except ValueError as exc:
            raise ValueError(f"PRODUCER_RATE_PER_SEC must be float: {exc}") from exc

        try:
            invalid_rate = float(os.getenv("PRODUCER_INVALID_RATE", "0.03"))
        except ValueError as exc:
            raise ValueError(f"PRODUCER_INVALID_RATE must be float: {exc}") from exc

        try:
            late_rate = float(os.getenv("PRODUCER_LATE_RATE", "0.02"))
        except ValueError as exc:
            raise ValueError(f"PRODUCER_LATE_RATE must be float: {exc}") from exc

        return RedpandaConfig(
            broker=broker,
            container_broker=container_broker,
            orders_topic=orders_topic,
            dlq_topic=dlq_topic,
            producer_rate_per_sec=rate,
            producer_invalid_rate=invalid_rate,
            producer_late_rate=late_rate,
        )

    def _delivery_callback(self, err: Optional[KafkaError], msg: Any) -> None:
        """Callback triggered on broker acknowledgement or delivery failure."""
        with self._stats_lock:
            if err is not None:
                self.delivery_errors += 1
                key_repr = msg.key() if hasattr(msg, "key") else "unknown"
                logger.error(
                    "Delivery error for message key %s: %s (code=%s)",
                    key_repr,
                    err.str() if hasattr(err, "str") else err,
                    err.code() if hasattr(err, "code") else "unknown",
                )
            else:
                self.events_delivered += 1
                if hasattr(msg, "topic") and hasattr(msg, "offset"):
                    logger.debug(
                        "Delivered message to %s [%s] @ offset %s",
                        msg.topic(),
                        msg.partition() if hasattr(msg, "partition") else 0,
                        msg.offset(),
                    )

    def produce_one(self) -> GeneratedOrderEvent:
        """Generate and produce a single order event.

        Returns:
            The GeneratedOrderEvent produced.

        Raises:
            BufferError: If the local producer queue remains saturated after retrying.
            Exception: On fatal producer errors.
        """
        event = self.generator.generate_event()
        key_bytes = event.key.encode("utf-8") if event.key is not None else None

        # Handle local buffer queue saturation with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=event.payload,
                    callback=self._delivery_callback,
                )
                with self._stats_lock:
                    self.events_produced += 1
                break
            except BufferError as b_err:
                logger.warning(
                    "Producer buffer full (attempt %d/%d). Polling to drain queue...",
                    attempt + 1,
                    max_retries,
                )
                self.producer.poll(0.5)
                if attempt == max_retries - 1:
                    with self._stats_lock:
                        self.delivery_errors += 1
                    logger.error("Failed to enqueue message after %d attempts", max_retries)
                    raise b_err

        # Service delivery callbacks
        self.producer.poll(0)
        return event

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages in the producer queue.

        Args:
            timeout: Maximum seconds to wait for flush.

        Returns:
            Number of messages still left unflushed.
        """
        logger.info("Flushing producer buffer (timeout=%.1fs)...", timeout)
        remaining = self.producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("%d messages remained unflushed after timeout", remaining)
        else:
            logger.info("All producer messages flushed successfully")
        return remaining

    def stop(self) -> None:
        """Signal producer loop to stop."""
        self._running = False

    def run(self, max_events: Optional[int] = None) -> Dict[str, int]:
        """Run the producer loop until max_events or termination signal.

        Args:
            max_events: Optional maximum number of events to publish.

        Returns:
            Dictionary with producer statistics.
        """
        self._running = True
        sleep_sec = 1.0 / self.rate_per_sec

        logger.info(
            "Starting OrderProducer: topic=%s, rate=%.1f msg/sec, max_events=%s, broker=%s",
            self.topic,
            self.rate_per_sec,
            max_events if max_events is not None else "unlimited",
            self.broker,
        )

        original_sigint = None
        original_sigterm = None
        try:
            if threading.current_thread() is threading.main_thread():
                original_sigint = signal.signal(
                    signal.SIGINT, lambda s, f: self._signal_handler(s)
                )
                original_sigterm = signal.signal(
                    signal.SIGTERM, lambda s, f: self._signal_handler(s)
                )
        except (ValueError, AttributeError):
            # Happens if signal handling is unsupported in the current context
            pass

        try:
            while self._running:
                if max_events is not None and self.events_produced >= max_events:
                    logger.info("Reached target max_events (%d); terminating", max_events)
                    break

                start_t = time.monotonic()
                self.produce_one()

                # Pacing
                elapsed = time.monotonic() - start_t
                remaining_sleep = sleep_sec - elapsed
                if remaining_sleep > 0:
                    time.sleep(remaining_sleep)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt detected; halting producer...")
        finally:
            self._running = False
            # Restore signal handlers
            try:
                if original_sigint is not None:
                    signal.signal(signal.SIGINT, original_sigint)
                if original_sigterm is not None:
                    signal.signal(signal.SIGTERM, original_sigterm)
            except (ValueError, AttributeError):
                pass

            self.flush()

        stats = {
            "produced": self.events_produced,
            "delivered": self.events_delivered,
            "errors": self.delivery_errors,
        }
        logger.info(
            "OrderProducer finished. Stats: produced=%d, delivered=%d, errors=%d",
            stats["produced"],
            stats["delivered"],
            stats["errors"],
        )
        return stats

    def _signal_handler(self, signum: int) -> None:
        signame = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.info("Received shutdown signal %s (%d); stopping gracefully...", signame, signum)
        self.stop()


def main() -> None:
    """CLI entrypoint for running the order producer."""
    parser = argparse.ArgumentParser(description="RetailPulse Simulated Order Producer")
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Events per second (overrides PRODUCER_RATE_PER_SEC)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Optional maximum number of events to publish before exiting",
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=None,
        help="Kafka/Redpanda broker address (e.g. localhost:19092 or redpanda:9092)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Destination topic name (default: orders_stream)",
    )
    parser.add_argument(
        "--invalid-rate",
        type=float,
        default=None,
        help="Defective event rate (0.0 to 1.0)",
    )
    parser.add_argument(
        "--late-rate",
        type=float,
        default=None,
        help="Late timestamp event rate (0.0 to 1.0)",
    )
    parser.add_argument(
        "--in-container",
        action="store_true",
        help="Flag indicating execution inside Docker container network",
    )
    args = parser.parse_args()

    setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))

    # Determine execution context (detect if running inside container or flag set)
    is_container = args.in_container or os.path.exists("/.dockerenv")

    # Load configuration
    config = OrderProducer._load_default_config()

    invalid_rate = args.invalid_rate if args.invalid_rate is not None else config.producer_invalid_rate
    late_rate = args.late_rate if args.late_rate is not None else config.producer_late_rate
    rate = args.rate if args.rate is not None else config.producer_rate_per_sec

    # Update config object with overrides
    config = RedpandaConfig(
        broker=config.broker,
        container_broker=config.container_broker,
        orders_topic=config.orders_topic,
        dlq_topic=config.dlq_topic,
        producer_rate_per_sec=rate,
        producer_invalid_rate=invalid_rate,
        producer_late_rate=late_rate,
    )

    generator = OrderGenerator.from_config(config)

    producer = OrderProducer(
        config=config,
        generator=generator,
        topic=args.topic,
        broker=args.broker,
        rate_per_sec=rate,
        in_container=is_container,
    )

    stats = producer.run(max_events=args.max_events)
    if stats["errors"] > 0 and stats["produced"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
