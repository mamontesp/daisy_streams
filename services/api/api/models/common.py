"""Common schemas and error response models for the API."""

from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    """Standard error response envelope."""

    detail: str = Field(..., description="Human-readable error description")
    error_code: Optional[str] = Field(default=None, description="Machine-readable error identifier")


class HealthResponse(BaseModel):
    """Liveness and readiness health probe response."""

    status: str = Field(..., description="Overall service status (healthy/degraded)")
    database: str = Field(..., description="Database connectivity status (connected/disconnected)")
    environment: str = Field(default="development", description="Current execution environment")
