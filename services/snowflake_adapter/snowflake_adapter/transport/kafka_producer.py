"""Resilient Kafka event producer with exponential backoff retries."""

from __future__ import annotations

import time
from typing import Any, Optional
from snowflake_adapter.logging import get_logger
from snowflake_adapter.models.envelope import EventEnvelope

logger = get_logger(__name__)

try:
    from confluent_kafka import Producer, KafkaError, KafkaException
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False
    Producer = None
    KafkaError = None
    KafkaException = Exception


class KafkaEventProducer:
    """Publishes validated EventEnvelope records to Kafka with retry and ordering guarantees."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "switch.orders.events",
        client_id: str = "daisy-streams-adapter",
        max_retries: int = 5,
        initial_backoff_sec: float = 0.5,
        backoff_multiplier: float = 2.0,
        producer_instance: Optional[Any] = None,
    ) -> None:
        self.topic = topic
        self.max_retries = max_retries
        self.initial_backoff_sec = initial_backoff_sec
        self.backoff_multiplier = backoff_multiplier

        if producer_instance is not None:
            self._producer = producer_instance
        else:
            if not CONFLUENT_KAFKA_AVAILABLE:
                raise RuntimeError("confluent-kafka is not installed.")
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "retries": 10,
                "enable.idempotence": True,
            }
            self._producer = Producer(conf)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback triggered by Kafka delivery report."""
        if err is not None:
            logger.error("Failed to deliver message %s: %s", msg.key(), err)
        else:
            logger.debug(
                "Message delivered to %s [%s] @ offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def publish(self, envelope: EventEnvelope, topic: Optional[str] = None) -> None:
        """Publish a single envelope with exponential backoff on broker buffer/connection errors."""
        target_topic = topic or self.topic
        routing_key = envelope.partition_key if envelope.partition_key is not None else envelope.entity_key
        key_bytes = str(routing_key).encode("utf-8")
        value_bytes = envelope.model_dump_json().encode("utf-8")

        delay = self.initial_backoff_sec
        for attempt in range(1, self.max_retries + 1):
            try:
                self._producer.produce(
                    topic=target_topic,
                    key=key_bytes,
                    value=value_bytes,
                    on_delivery=self._delivery_callback,
                )
                self._producer.poll(0)
                return
            except BufferError as e:
                logger.warning(
                    "Local producer queue full (attempt %d/%d). Polling and backing off for %.2fs: %s",
                    attempt,
                    self.max_retries,
                    delay,
                    e,
                )
                self._producer.poll(1.0)
                time.sleep(delay)
                delay *= self.backoff_multiplier
            except Exception as e:
                logger.warning(
                    "Broker produce error (attempt %d/%d). Retrying in %.2fs: %s",
                    attempt,
                    self.max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)
                delay *= self.backoff_multiplier

        raise RuntimeError(
            f"Failed to publish event {envelope.event_id} after {self.max_retries} attempts."
        )

    def publish_batch(self, envelopes: list[EventEnvelope], topic: Optional[str] = None) -> int:
        """Publish a batch of envelopes, returning number of events submitted."""
        count = 0
        for envelope in envelopes:
            self.publish(envelope, topic=topic)
            count += 1
        self.flush()
        return count

    def flush(self, timeout: float = 10.0) -> None:
        """Wait for all outstanding messages to be delivered."""
        if hasattr(self._producer, "flush"):
            self._producer.flush(timeout)

    def close(self) -> None:
        """Flush and terminate producer."""
        self.flush(timeout=5.0)
