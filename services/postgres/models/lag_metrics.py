"""Lag metrics model representing delay between source and Postgres ingestion."""

from pydantic import BaseModel, Field


class LagMetrics(BaseModel):
    """Lag between source occurrence and Postgres ingestion in seconds."""

    avg_lag_seconds: float = Field(default=0.0, description="Average lag across processed events")
    max_lag_seconds: float = Field(default=0.0, description="Peak lag across processed events")
