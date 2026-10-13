"""Aggregated stream statistics response model (REQ-C04)."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, ConfigDict, Field
from services.postgres.models.lag_metrics import LagMetrics


class StatsResponse(BaseModel):
    """Aggregated metrics returned by GET /stats."""

    model_config = ConfigDict(from_attributes=True)

    total_events: int = Field(default=0, description="Total events in event_log")
    counts_by_type: dict[str, int] = Field(
        default_factory=dict, description="Event counts grouped by event_type"
    )
    quarantined_count: int = Field(default=0, description="Total quarantined DLQ events")
    unique_entities_count: int = Field(
        default=0, description="Total active entities in current_state"
    )
    lag: LagMetrics = Field(default_factory=LagMetrics, description="Stream processing lag metrics")
    last_watermark: Optional[Union[str, datetime]] = Field(
        default=None, description="Latest occurred_at watermark seen"
    )
    last_processed_at: Optional[Union[str, datetime]] = Field(
        default=None, description="Timestamp of most recently ingested event"
    )
