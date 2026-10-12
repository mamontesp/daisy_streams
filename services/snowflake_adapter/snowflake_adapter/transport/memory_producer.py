"""In-memory event producer for unit testing and local verification."""

from __future__ import annotations

from snowflake_adapter.logging import get_logger
from snowflake_adapter.models.envelope import EventEnvelope

logger = get_logger(__name__)


class InMemoryEventProducer:
    """Collects published events in memory."""

    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []

    def publish(self, envelope: EventEnvelope) -> None:
        """Record an event envelope in memory."""
        self.events.append(envelope)
        logger.debug("Captured event %s for entity %s", envelope.event_id, envelope.entity_key)

    def publish_batch(self, envelopes: list[EventEnvelope]) -> int:
        """Record batch of event envelopes in memory."""
        self.events.extend(envelopes)
        logger.debug("Captured batch of %d events in memory", len(envelopes))
        return len(envelopes)

    def flush(self, timeout: float = 10.0) -> None:
        """No-op for in-memory producer."""
        pass

    def close(self) -> None:
        """No-op for in-memory producer."""
        pass

    def clear(self) -> None:
        """Clear recorded events."""
        self.events.clear()
