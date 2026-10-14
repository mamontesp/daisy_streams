"""Representation of a row in bronze.quarantined_event_log DLQ table (REQ-C03)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field
from services.postgres.models.enums import ErrorCode


class QuarantinedEventLog(BaseModel):
    """Representation of a row in the `bronze.quarantined_event_log` DLQ table (REQ-C03)."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    id: Optional[int] = Field(default=None, description="Auto-increment sequence ID")
    event_id: Optional[UUID] = Field(default=None, description="Event UUID if parseable")
    entity_key: Optional[str] = Field(default=None, description="Entity key if present")
    error_code: ErrorCode = Field(description="Machine-readable error category")
    error_message: str = Field(description="Human-readable description of error")
    raw_payload: dict[str, Any] = Field(description="Original unmutated payload")
    quarantined_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when record was quarantined",
    )


# Backwards compatibility alias
QuarantinedEventRecord = QuarantinedEventLog
