#!/usr/bin/env python3
"""CLI entrypoint for running the Kafka Consumer Service (Part C)."""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from typing import Any, List, Optional
from uuid import uuid4

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR / "services" / "snowflake_adapter"))
sys.path.insert(0, str(ROOT_DIR / "services" / "postgres"))

from snowflake_adapter.logging import configure_logging, get_logger
from services.consumer.consumer.config import ConsumerSettings
from services.consumer.consumer.consumer import KafkaConsumerService
from services.postgres.repository import PostgresRepository

logger = get_logger("daisy.consumer")


class MockKafkaMessage:
    """Mock Kafka message for offline dry-run verification."""

    def __init__(self, value: bytes, topic: str, partition: int = 0, offset: int = 0) -> None:
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> Optional[Any]:
        return None


class MockKafkaConsumer:
    """In-memory mock consumer supporting message queuing and offset 0 replay."""

    def __init__(self, initial_messages: Optional[List[MockKafkaMessage]] = None) -> None:
        self.all_messages: List[MockKafkaMessage] = initial_messages or []
        self._index = 0
        self.subscribed_topics: List[str] = []
        self.committed_messages: List[Any] = []

    def subscribe(self, topics: List[str]) -> None:
        self.subscribed_topics = list(topics)

    def poll(self, timeout: float = 1.0) -> Optional[MockKafkaMessage]:
        if self._index < len(self.all_messages):
            msg = self.all_messages[self._index]
            self._index += 1
            return msg
        return None

    def commit(self, message: Any = None, asynchronous: bool = False) -> None:
        if message:
            self.committed_messages.append(message)

    def seek_to_beginning(self, topic: str) -> List[Any]:
        self._index = 0
        return [0]

    def close(self) -> None:
        pass


def _generate_mock_messages(topic: str, count: int = 5) -> List[MockKafkaMessage]:
    """Generate realistic mock order messages for offline run."""
    import json
    from datetime import datetime, timezone

    messages: List[MockKafkaMessage] = []
    base_id = 50000

    for i in range(count):
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "event_id": str(uuid4()),
            "event_type": "insert",
            "source": "snowflake.tpch.orders",
            "entity_key": f"ord-{base_id + i}",
            "partition_key": f"cust-{100 + i}",
            "payload": {
                "O_ORDERKEY": base_id + i,
                "O_CUSTKEY": 100 + i,
                "O_TOTALPRICE": 150.0 + (i * 25.5),
                "O_ORDERSTATUS": "O",
                "O_ORDERDATE": "2026-10-12",
            },
            "occurred_at": now_iso,
            "captured_at": now_iso,
            "schema_version": "1.0.0",
        }
        raw_bytes = json.dumps(payload).encode("utf-8")
        messages.append(MockKafkaMessage(value=raw_bytes, topic=topic, offset=i))

    return messages


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Daisy Streams — Kafka to PostgreSQL Ingestion Consumer"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuous message ingestion loop",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Process a maximum of N messages and then exit",
    )
    parser.add_argument(
        "--replay-offset-0",
        action="store_true",
        help="Rewind consumer to offset 0 and replay topic stream",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Override topic name (defaults to KAFKA_TOPIC environment variable)",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Run with offline mock messages without requiring live Kafka broker",
    )

    args = parser.parse_args()
    configure_logging()

    # Load settings from environment (.env)
    settings = ConsumerSettings()
    if args.topic:
        settings.kafka_topic = args.topic

    logger.info("Initializing Kafka Consumer Service for topic '%s'", settings.kafka_topic)

    if args.mock:
        logger.info("Running in mock mode with synthetic messages.")
        count = args.max_messages or 5
        mock_messages = _generate_mock_messages(topic=settings.kafka_topic, count=count)
        mock_consumer_instance = MockKafkaConsumer(initial_messages=mock_messages)
        from services.postgres.db import SQLiteTestDB
        repo = PostgresRepository(db=SQLiteTestDB(":memory:"))
    else:
        repo = PostgresRepository()

    try:
        consumer_service = KafkaConsumerService(
            settings=settings,
            repo=repo,
            consumer_instance=mock_consumer_instance,
        )
    except Exception as exc:
        logger.error("Failed to initialize Kafka Consumer: %s", exc)
        return 1

    # Graceful shutdown handler
    def handle_signal(sig: int, frame: Any) -> None:
        logger.info("Received termination signal (%s). Stopping consumer gracefully...", sig)
        consumer_service.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        if args.replay_offset_0:
            logger.info("Triggering offset 0 replay mode (REQ-C02)...")
            replayed = consumer_service.replay_offset_zero(max_messages=args.max_messages)
            logger.info("Replay completed successfully. Replayed %d events.", replayed)
            return 0

        # Normal ingestion
        max_msgs = args.max_messages if not args.loop else None
        processed = consumer_service.consume_loop(max_messages=max_msgs)
        logger.info("Consumer finished. Processed %d messages.", processed)
        return 0

    except Exception as exc:
        logger.error("Consumer loop encountered fatal exception: %s", exc, exc_info=True)
        return 1
    finally:
        consumer_service.close()


if __name__ == "__main__":
    sys.exit(main())
