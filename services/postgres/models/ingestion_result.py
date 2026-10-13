"""Result model for atomic ingestion operations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field


class IngestionResult(BaseModel):
    """Result of an atomic event ingestion attempt."""

    model_config = ConfigDict(extra="ignore")

    status: Literal["processed", "quarantined"] = Field(description="Final disposition of the event")
    event_id: Optional[str] = Field(default=None, description="Event UUID string")
    entity_key: Optional[str] = Field(default=None, description="Entity key")
    event_type: Optional[str] = Field(default=None, description="Event type if processed")
    error_code: Optional[str] = Field(default=None, description="Error code if quarantined")
    error_message: Optional[str] = Field(default=None, description="Error explanation if quarantined")
    processed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Ingestion processing timestamp",
    )
