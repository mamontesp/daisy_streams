"""Abstract event producer protocol for streaming transport."""

from __future__ import annotations

from typing import Optional, Protocol
from snowflake_adapter.models.envelope import EventEnvelope


class EventProducer(Protocol):
    """Protocol for event transport producers."""

    def publish(self, envelope: EventEnvelope, topic: Optional[str] = None) -> None:
        """Publish a single validated event envelope."""
        ...

    def publish_batch(self, envelopes: list[EventEnvelope], topic: Optional[str] = None) -> int:
        """Publish a batch of event envelopes, returning count of successfully emitted events."""
        ...

    def flush(self, timeout: float = 10.0) -> None:
        """Flush any buffered messages to the broker."""
        ...

    def close(self) -> None:
        """Close producer connections."""
        ...
