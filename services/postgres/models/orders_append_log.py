"""Representation of a row in the append-only event ledger (REQ-C01)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field


class OrdersAppendLog(BaseModel):
    """Representation of a row in the append-only `event_log` table (REQ-C01)."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    id: Optional[int] = Field(default=None, description="Database auto-increment sequence ID")
    event_id: UUID = Field(description="Unique event UUID")
    batch_id: Optional[str] = Field(default=None, description="Sync run batch correlation ID")
    event_type: Literal["insert", "update"] = Field(description="Event operation type")
    source: str = Field(description="Event data source system")
    entity_key: str = Field(description="Primary entity key")
    partition_key: Optional[str] = Field(default=None, description="Streaming partition key")
    payload: dict[str, Any] = Field(description="Raw event payload dictionary")
    occurred_at: datetime = Field(description="Source occurrence timestamp")
    captured_at: datetime = Field(description="Adapter ingestion capture timestamp")
    processed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="PostgreSQL persistence timestamp",
    )
    schema_version: str = Field(default="1.0.0", description="Envelope schema version")


# Backwards compatibility alias
EventLogRecord = OrdersAppendLog
