"""Event envelope schema adhering strictly to REQ-B03."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class EventEnvelope(BaseModel):
    """Explicit standard event envelope for streaming transport."""

    event_id: UUID = Field(default_factory=uuid4, description="Unique event identifier (UUIDv4)")
    event_type: Literal["insert", "update"] = Field(description="Operation type")
    source: str = Field(description="Source system identifier (e.g., snowflake.tpch.orders)")
    entity_key: str = Field(description="Entity primary key identifier (e.g., order_id)")
    payload: dict[str, Any] = Field(description="Row payload contents")
    occurred_at: datetime = Field(description="Timestamp when event occurred at source")
    captured_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when event was captured by adapter",
    )
    schema_version: str = Field(default="1.0.0", description="Semantic envelope version")
