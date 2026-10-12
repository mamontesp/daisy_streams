"""Transport layer components and producer factory."""

from __future__ import annotations

from typing import Optional
from snowflake_adapter.config import Settings, get_settings
from snowflake_adapter.transport.base import EventProducer
from snowflake_adapter.transport.memory_producer import InMemoryEventProducer
from snowflake_adapter.transport.kafka_producer import KafkaEventProducer

__all__ = ["EventProducer", "InMemoryEventProducer", "KafkaEventProducer", "get_event_producer"]


def get_event_producer(settings: Optional[Settings] = None, in_memory: bool = False) -> EventProducer:
    """Factory to get configured EventProducer."""
    if in_memory:
        return InMemoryEventProducer()

    cfg = settings or get_settings()
    try:
        return KafkaEventProducer(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            topic=cfg.kafka_topic,
            client_id=cfg.kafka_client_id,
        )
    except Exception:
        return InMemoryEventProducer()
