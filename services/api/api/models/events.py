"""Independent request and response models for the GET /events endpoint."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EventItem(BaseModel):
    """Public representation of an ingested event from the append-only ledger."""

    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = Field(default=None, description="Internal ledger sequence identifier")
    event_id: UUID = Field(..., description="Unique event identifier (UUIDv4)")
    event_type: str = Field(..., description="Event action type (e.g., insert, update)")
    source: str = Field(..., description="Source system origin (e.g., snowflake.orders)")
    entity_key: str = Field(..., description="Entity identifier (e.g., order key)")
    partition_key: Optional[str] = Field(default=None, description="Partition routing key")
    payload: dict[str, Any] = Field(default_factory=dict, description="Event data payload")
    occurred_at: datetime = Field(..., description="Timestamp when event occurred at source")
    captured_at: datetime = Field(..., description="Timestamp when event was captured by adapter")
    processed_at: datetime = Field(..., description="Timestamp when event was persisted to PostgreSQL")
    schema_version: str = Field(default="1.0.0", description="Envelope schema version")


class EventListResponse(BaseModel):
    """Paginated response containing matching stream events."""

    model_config = ConfigDict(from_attributes=True)

    total: int = Field(..., description="Total number of records matching query filters", ge=0)
    limit: int = Field(..., description="Maximum records returned in this page", ge=1)
    offset: int = Field(..., description="Record offset skipped", ge=0)
    events: list[EventItem] = Field(default_factory=list, description="Array of matching event items")
