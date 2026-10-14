"""Unit and integration tests for Kafka Consumer Service (Part C).

Covers:
  - Dynamic subscription strictly from KAFKA_TOPIC in .env.
  - Manual at-least-once offset commits upon successful Postgres writes.
  - REQ-C01 / AC-C01: Dual-table materialization via consumer polling.
  - REQ-C02 / AC-C02: Offset 0 replay state convergence.
  - REQ-C03 / AC-C03: Negative price and malformed payload DLQ routing.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4
import pytest

from services.consumer.consumer.config import ConsumerSettings
from services.consumer.consumer.consumer import KafkaConsumerService
from services.consumer.scripts.run_consumer import MockKafkaConsumer, MockKafkaMessage
from services.postgres.repository import PostgresRepository


def _make_raw_message(
    entity_key: str = "ord-901",
    price: float = 120.0,
    event_type: str = "insert",
    topic: str = "switch.orders.events",
    offset: int = 0,
    event_id: str = None,
) -> MockKafkaMessage:
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "event_id": event_id or str(uuid4()),
        "event_type": event_type,
        "source": "snowflake.orders",
        "entity_key": entity_key,
        "partition_key": f"cust-{entity_key}",
        "payload": {
            "O_ORDERKEY": entity_key,
            "O_CUSTKEY": 100,
            "O_TOTALPRICE": price,
            "O_ORDERSTATUS": "O",
            "O_ORDERDATE": "2026-10-12",
        },
        "occurred_at": now_iso,
        "captured_at": now_iso,
        "schema_version": "1.0.0",
    }
    raw_bytes = json.dumps(payload).encode("utf-8")
    return MockKafkaMessage(value=raw_bytes, topic=topic, offset=offset)


def test_consumer_subscribes_to_env_topic(test_repo: PostgresRepository) -> None:
    """Verify consumer subscribes strictly to the topic configured via KAFKA_TOPIC in environment/.env."""
    custom_topic = "switch.custom_tenant.orders"
    settings = ConsumerSettings(KAFKA_TOPIC=custom_topic)
    mock_consumer = MockKafkaConsumer()

    consumer_service = KafkaConsumerService(
        settings=settings,
        repo=test_repo,
        consumer_instance=mock_consumer,
    )

    assert consumer_service.subscribed_topic == custom_topic
    assert mock_consumer.subscribed_topics == [custom_topic]


def test_consumer_processes_valid_message_and_commits(
    test_repo: PostgresRepository,
    mock_kafka_consumer: MockKafkaConsumer,
) -> None:
    """AC-C01: Given a valid event, consumer writes to event_log, current_state, and commits offset."""
    topic = "switch.orders.events"
    settings = ConsumerSettings(KAFKA_TOPIC=topic)
    msg = _make_raw_message(entity_key="ord-1001", price=300.0, topic=topic, offset=1)
    mock_kafka_consumer.all_messages = [msg]

    consumer_service = KafkaConsumerService(
        settings=settings,
        repo=test_repo,
        consumer_instance=mock_kafka_consumer,
    )

    result = consumer_service.poll_and_process()
    assert result is not None
    assert result.status == "processed"
    assert result.entity_key == "ord-1001"

    # Verify database materialization
    entity_info = test_repo.get_entity_state_and_history("ord-1001")
    assert entity_info is not None
    assert entity_info.current_state.state["O_TOTALPRICE"] == 300.0
    assert len(entity_info.history) == 1

    # Verify at-least-once offset commit was executed
    assert len(mock_kafka_consumer.committed_messages) == 1
    assert mock_kafka_consumer.committed_messages[0] == msg


def test_consumer_routes_negative_price_to_dlq_and_commits(
    test_repo: PostgresRepository,
    mock_kafka_consumer: MockKafkaConsumer,
) -> None:
    """AC-C03: Given a negative price event, record is quarantined in DLQ and offset is committed."""
    topic = "switch.orders.events"
    settings = ConsumerSettings(KAFKA_TOPIC=topic)
    bad_msg = _make_raw_message(entity_key="ord-bad-99", price=-50.0, topic=topic, offset=2)
    mock_kafka_consumer.all_messages = [bad_msg]

    consumer_service = KafkaConsumerService(
        settings=settings,
        repo=test_repo,
        consumer_instance=mock_kafka_consumer,
    )

    result = consumer_service.poll_and_process()
    assert result is not None
    assert result.status == "quarantined"
    assert result.error_code == "BUSINESS_RULE_VIOLATION"

    # Omitted from current_state
    assert test_repo.get_entity_state_and_history("ord-bad-99") is None

    # Persisted in quarantined_events
    stats = test_repo.get_stats()
    assert stats.quarantined_count == 1

    # Offset must be committed to avoid infinite poisoned pill loop
    assert len(mock_kafka_consumer.committed_messages) == 1


def test_consumer_offset_zero_replay(
    test_repo: PostgresRepository,
    mock_kafka_consumer: MockKafkaConsumer,
) -> None:
    """AC-C02: Given offset 0 replay, current_state row count and column values remain identical."""
    topic = "switch.orders.events"
    settings = ConsumerSettings(KAFKA_TOPIC=topic)

    m1 = _make_raw_message("ord-rep-1", 100.0, "insert", topic, offset=0)
    m2 = _make_raw_message("ord-rep-2", 200.0, "insert", topic, offset=1)
    m3 = _make_raw_message("ord-rep-1", 150.0, "update", topic, offset=2)

    mock_kafka_consumer.all_messages = [m1, m2, m3]

    consumer_service = KafkaConsumerService(
        settings=settings,
        repo=test_repo,
        consumer_instance=mock_kafka_consumer,
    )

    # Initial consumption run
    count1 = consumer_service.consume_loop(max_messages=3)
    assert count1 == 3

    state_before_1 = test_repo.get_entity_state_and_history("ord-rep-1").current_state
    state_before_2 = test_repo.get_entity_state_and_history("ord-rep-2").current_state
    assert state_before_1.state["O_TOTALPRICE"] == 150.0
    assert state_before_2.state["O_TOTALPRICE"] == 200.0

    # Execute offset 0 replay
    replayed_count = consumer_service.replay_offset_zero(max_messages=3)
    assert replayed_count == 3

    # Assert exact state convergence
    state_after_1 = test_repo.get_entity_state_and_history("ord-rep-1").current_state
    state_after_2 = test_repo.get_entity_state_and_history("ord-rep-2").current_state
    assert state_after_1.entity_key == state_before_1.entity_key
    assert state_after_1.state == state_before_1.state
    assert state_after_2.entity_key == state_before_2.entity_key
    assert state_after_2.state == state_before_2.state


def test_consumer_consume_loop_max_messages(
    test_repo: PostgresRepository,
    mock_kafka_consumer: MockKafkaConsumer,
) -> None:
    """Verify consume_loop terminates cleanly after max_messages count."""
    topic = "switch.orders.events"
    settings = ConsumerSettings(KAFKA_TOPIC=topic)
    mock_kafka_consumer.all_messages = [
        _make_raw_message(f"ord-{i}", 50.0 + i, topic=topic, offset=i)
        for i in range(10)
    ]

    consumer_service = KafkaConsumerService(
        settings=settings,
        repo=test_repo,
        consumer_instance=mock_kafka_consumer,
    )

    processed = consumer_service.consume_loop(max_messages=4)
    assert processed == 4
    assert len(mock_kafka_consumer.committed_messages) == 4


def test_consumer_batch_id_correlation_and_context_reset(
    test_repo: PostgresRepository,
) -> None:
    """Verify consumer extracts batch_id from message and resets ContextVars after processing."""
    from services.consumer.consumer.logging import correlation_batch_id, correlation_event_id

    topic = "switch.orders.events"
    settings = ConsumerSettings(KAFKA_TOPIC=topic)
    batch_uuid = str(uuid4())
    event_uuid = str(uuid4())

    payload = {
        "event_id": event_uuid,
        "batch_id": batch_uuid,
        "event_type": "insert",
        "source": "snowflake.orders",
        "entity_key": "ord-corr-1",
        "partition_key": "cust-1",
        "payload": {
            "O_ORDERKEY": 9999,
            "O_CUSTKEY": 1,
            "O_TOTALPRICE": 350.0,
            "O_ORDERSTATUS": "O",
            "O_ORDERDATE": "2026-10-14",
        },
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0.0",
    }
    raw_bytes = json.dumps(payload).encode("utf-8")
    mock_msg = MockKafkaMessage(value=raw_bytes, topic=topic, offset=0)
    mock_consumer = MockKafkaConsumer(initial_messages=[mock_msg])

    consumer_service = KafkaConsumerService(
        settings=settings,
        repo=test_repo,
        consumer_instance=mock_consumer,
    )

    result = consumer_service.poll_and_process()
    assert result is not None
    assert result.status == "processed"
    assert result.event_id == event_uuid

    # Assert ContextVars are reset after processing (no leakage)
    assert correlation_event_id.get() is None
    assert correlation_batch_id.get() is None

    # Assert database recorded batch_id
    rows = test_repo.db.fetchall("SELECT batch_id FROM event_log WHERE event_id = %s", (event_uuid,))
    assert len(rows) == 1
    assert rows[0]["batch_id"] == batch_uuid
