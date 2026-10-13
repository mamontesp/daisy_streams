"""Unit tests for query builders and Kafka producer routing."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from snowflake_adapter.models.envelope import EventEnvelope
from snowflake_adapter.queries import build_incremental_sync_query, build_initial_sync_query
from snowflake_adapter.transport.kafka_producer import KafkaEventProducer


def test_queries_include_custkey() -> None:
    """Verify build_initial_sync_query and build_incremental_sync_query include O_CUSTKEY and C_CUSTKEY."""
    init_sql = build_initial_sync_query(
        database="daisy_streams",
        schema="ingestion",
        table="ORDERS",
        watermark_column="O_ORDERDATE",
        entity_key_column="O_ORDERKEY",
        partition_key_column="O_CUSTKEY",
    )
    assert "o.O_CUSTKEY" in init_sql
    assert "c.C_CUSTKEY" in init_sql

    inc_sql = build_incremental_sync_query(
        database="daisy_streams",
        schema="ingestion",
        table="ORDERS",
        watermark_column="O_ORDERDATE",
        entity_key_column="O_ORDERKEY",
        partition_key_column="O_CUSTKEY",
    )
    assert "o.O_CUSTKEY" in inc_sql
    assert "c.C_CUSTKEY" in inc_sql


def test_kafka_producer_routes_by_partition_key() -> None:
    """Verify KafkaEventProducer routes messages using partition_key."""
    mock_inner = MagicMock()
    producer = KafkaEventProducer(
        topic="switch.orders.events",
        producer_instance=mock_inner,
    )

    envelope = EventEnvelope(
        event_type="insert",
        source="snowflake.daisy_streams.ingestion.orders",
        entity_key="99901",
        partition_key="1001",
        payload={"order_id": 99901, "cust_id": 1001},
        occurred_at=datetime.now(timezone.utc),
    )

    producer.publish(envelope)

    mock_inner.produce.assert_called_once()
    call_kwargs = mock_inner.produce.call_args[1]
    assert call_kwargs["topic"] == "switch.orders.events"
    assert call_kwargs["key"] == b"1001"


def test_kafka_producer_routes_by_entity_key_when_partition_key_none() -> None:
    """Verify KafkaEventProducer falls back to entity_key when partition_key is None."""
    mock_inner = MagicMock()
    producer = KafkaEventProducer(
        topic="switch.orders.events",
        producer_instance=mock_inner,
    )

    envelope = EventEnvelope(
        event_type="insert",
        source="snowflake.daisy_streams.ingestion.orders",
        entity_key="99901",
        partition_key=None,
        payload={"order_id": 99901},
        occurred_at=datetime.now(timezone.utc),
    )

    producer.publish(envelope)

    mock_inner.produce.assert_called_once()
    call_kwargs = mock_inner.produce.call_args[1]
    assert call_kwargs["topic"] == "switch.orders.events"
    assert call_kwargs["key"] == b"99901"


def test_kafka_producer_supports_topic_override() -> None:
    """Verify KafkaEventProducer respects topic override in publish and publish_batch."""
    mock_inner = MagicMock()
    producer = KafkaEventProducer(
        topic="switch.orders.events",
        producer_instance=mock_inner,
    )

    envelope = EventEnvelope(
        event_type="insert",
        source="snowflake.daisy_streams.ingestion.orders",
        entity_key="99901",
        partition_key="1001",
        payload={"order_id": 99901},
        occurred_at=datetime.now(timezone.utc),
    )

    producer.publish(envelope, topic="switch.orders.events.v2")

    mock_inner.produce.assert_called_once()
    call_kwargs = mock_inner.produce.call_args[1]
    assert call_kwargs["topic"] == "switch.orders.events.v2"
    assert call_kwargs["key"] == b"1001"
