"""Independent response models for the GET /stats stream aggregation endpoint."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class LagMetrics(BaseModel):
    """Processing lag metrics measuring latency between occurred_at and processed_at."""

    model_config = ConfigDict(from_attributes=True)

    avg_lag_seconds: float = Field(
        default=0.0,
        description="Average elapsed seconds between source occurrence and sink materialization",
    )
    max_lag_seconds: float = Field(
        default=0.0,
        description="Maximum elapsed seconds between source occurrence and sink materialization",
    )


class StreamStatsResponse(BaseModel):
    """Aggregated stream processing and health metrics returned by GET /stats."""

    model_config = ConfigDict(from_attributes=True)

    total_events: int = Field(
        default=0,
        description="Total count of events stored in the append-only event_log table",
        ge=0,
    )
    counts_by_type: dict[str, int] = Field(
        default_factory=dict,
        description="Distribution of ingested events grouped by event_type (e.g. insert, update)",
    )
    quarantined_count: int = Field(
        default=0,
        description="Total events routed to dead-letter queue (quarantined_events table) due to DQ failure",
        ge=0,
    )
    unique_entities_count: int = Field(
        default=0,
        description="Count of active unique entities materialized in current_state",
        ge=0,
    )
    lag: LagMetrics = Field(
        default_factory=LagMetrics,
        description="Pipeline ingestion latency metrics",
    )
    last_watermark: Optional[Union[datetime, str]] = Field(
        default=None,
        description="Timestamp of the most recent event occurred_at watermark observed in event_log",
    )
    last_processed_at: Optional[Union[datetime, str]] = Field(
        default=None,
        description="Timestamp when the most recent event was processed into PostgreSQL",
    )
