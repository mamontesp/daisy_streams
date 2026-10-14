"""Kafka consumer service ingesting streaming events into PostgreSQL idempotently."""

from __future__ import annotations

import json
import signal
from typing import Any, Optional
from uuid import UUID

from services.consumer.consumer.logging import (
    correlation_batch_id,
    correlation_event_id,
    get_logger,
)
from services.postgres.models import IngestionResult
from services.postgres.repository import PostgresRepository
from services.consumer.consumer.config import ConsumerSettings
from services.consumer.consumer.replay import rewind_topic_to_offset_zero

logger = get_logger(__name__)

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False
    Consumer = None
    KafkaError = None
    KafkaException = Exception


class KafkaConsumerService:
    """
    Subscribes dynamically to KAFKA_TOPIC, consumes records, runs DQ checks,
    and materializes events into PostgreSQL with manual offset commits.
    """

    def __init__(
        self,
        settings: Optional[ConsumerSettings] = None,
        repo: Optional[PostgresRepository] = None,
        consumer_instance: Optional[Any] = None,
    ) -> None:
        self.settings = settings or ConsumerSettings()
        self.repo = repo if repo is not None else PostgresRepository()
        self._running = True

        if consumer_instance is not None:
            self._consumer = consumer_instance
            # Subscribe mock or provided instance to the configured topic
            if hasattr(self._consumer, "subscribe"):
                self._consumer.subscribe([self.settings.kafka_topic])
        else:
            if not CONFLUENT_KAFKA_AVAILABLE:
                raise RuntimeError("confluent-kafka is not installed.")

            conf = {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "group.id": self.settings.kafka_consumer_group,
                "client.id": self.settings.kafka_client_id,
                "auto.offset.reset": self.settings.kafka_auto_offset_reset,
                "enable.auto.commit": False,  # Manual commit for at-least-once delivery
                "enable.partition.eof": False,
            }
            self._consumer = Consumer(conf)
            self._consumer.subscribe([self.settings.kafka_topic])
            logger.info(
                "Subscribed to Kafka topic '%s' (group: '%s', broker: '%s')",
                self.settings.kafka_topic,
                self.settings.kafka_consumer_group,
                self.settings.kafka_bootstrap_servers,
            )

    @property
    def subscribed_topic(self) -> str:
        """Return the active topic configured via KAFKA_TOPIC."""
        return self.settings.kafka_topic

    def poll_and_process(self, timeout: Optional[float] = None) -> Optional[IngestionResult]:
        """
        Poll a single message from Kafka, ingest it into PostgreSQL,
        and commit the offset synchronously on success.
        """
        poll_timeout = timeout if timeout is not None else self.settings.consumer_poll_timeout_sec
        msg = self._consumer.poll(timeout=poll_timeout)

        if msg is None:
            return None

        if hasattr(msg, "error") and msg.error():
            err = msg.error()
            if hasattr(KafkaError, "_PARTITION_EOF") and err.code() == KafkaError._PARTITION_EOF:
                logger.debug("Reached end of partition: %s [%d] @ offset %d", msg.topic(), msg.partition(), msg.offset())
                return None
            logger.error("Kafka consumer error: %s", err)
            return None

        raw_bytes = msg.value()
        if raw_bytes is None:
            logger.warning("Received tombstone / empty message at offset %s", getattr(msg, "offset", lambda: "?")())
            return None

        try:
            raw_event = json.loads(raw_bytes.decode("utf-8"))
        except Exception as exc:
            logger.error("JSON deserialization error for Kafka message: %s", exc)
            raw_event = {"raw_malformed_string": str(raw_bytes)}

        # Extract correlation IDs from payload or Kafka headers
        ev_id_str = raw_event.get("event_id") if isinstance(raw_event, dict) else None
        batch_id_str = raw_event.get("batch_id") if isinstance(raw_event, dict) else None

        if hasattr(msg, "headers") and msg.headers():
            for h_key, h_val in msg.headers():
                if h_key == "event_id" and not ev_id_str and h_val:
                    ev_id_str = h_val.decode("utf-8") if isinstance(h_val, bytes) else str(h_val)
                elif h_key == "batch_id" and not batch_id_str and h_val:
                    batch_id_str = h_val.decode("utf-8") if isinstance(h_val, bytes) else str(h_val)

        token_ev = correlation_event_id.set(str(ev_id_str)) if ev_id_str else None
        token_batch = correlation_batch_id.set(str(batch_id_str)) if batch_id_str else None

        try:
            # Atomic PostgreSQL ingestion
            result = self.repo.ingest_event(raw_event)

            # Commit Kafka offset after successful write to Postgres
            try:
                if hasattr(self._consumer, "commit"):
                    self._consumer.commit(message=msg, asynchronous=False)
            except Exception as commit_err:
                logger.error("Failed to commit offset for event %s: %s", result.event_id, commit_err)
                raise

            logger.info(
                "Processed message from %s [%s] @ offset %s -> status=%s (event_id=%s, batch_id=%s)",
                getattr(msg, "topic", lambda: self.settings.kafka_topic)(),
                getattr(msg, "partition", lambda: 0)(),
                getattr(msg, "offset", lambda: 0)(),
                result.status,
                result.event_id,
                batch_id_str,
            )
            return result
        finally:
            if token_ev is not None:
                correlation_event_id.reset(token_ev)
            if token_batch is not None:
                correlation_batch_id.reset(token_batch)

    def consume_loop(
        self,
        max_messages: Optional[int] = None,
        stop_event: Optional[Any] = None,
    ) -> int:
        """
        Continuously poll and process events until stopped or max_messages reached.
        """
        processed_count = 0
        logger.info("Starting consumer loop for topic '%s'...", self.settings.kafka_topic)

        while self._running:
            if stop_event is not None and stop_event.is_set():
                break

            result = self.poll_and_process()
            if result is not None:
                processed_count += 1
                if max_messages is not None and processed_count >= max_messages:
                    logger.info("Reached maximum message count (%d). Stopping loop.", max_messages)
                    break

        return processed_count

    def replay_offset_zero(
        self,
        max_messages: Optional[int] = None,
        empty_poll_limit: int = 5,
    ) -> int:
        """
        Rewind all partitions of KAFKA_TOPIC to offset 0 and replay the stream into PostgreSQL.
        Guarantees AC-C02: identical current_state row count and column values.
        """
        logger.info("Executing offset 0 replay for topic '%s'...", self.settings.kafka_topic)
        rewind_topic_to_offset_zero(self._consumer, self.settings.kafka_topic)

        replayed_count = 0
        consecutive_empty = 0

        while self._running:
            result = self.poll_and_process(timeout=1.0)
            if result is not None:
                replayed_count += 1
                consecutive_empty = 0
                if max_messages is not None and replayed_count >= max_messages:
                    break
            else:
                consecutive_empty += 1
                if consecutive_empty >= empty_poll_limit:
                    logger.info("Reached topic end during replay (%d consecutive empty polls).", consecutive_empty)
                    break

        logger.info("Offset 0 replay completed. Replayed %d messages.", replayed_count)
        return replayed_count

    def stop(self) -> None:
        """Signal consumer to exit polling loop."""
        self._running = False

    def close(self) -> None:
        """Flush and close Kafka consumer."""
        self.stop()
        if hasattr(self._consumer, "close"):
            self._consumer.close()
        logger.info("Kafka consumer closed.")
