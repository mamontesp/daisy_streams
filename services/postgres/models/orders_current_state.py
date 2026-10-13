"""Representation of a row in the materialized current state table (REQ-C01, REQ-C02)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field


class OrdersCurrentState(BaseModel):
    """Representation of a row in the materialized `current_state` table (REQ-C01, REQ-C02)."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    entity_key: str = Field(description="Primary entity key")
    latest_event_id: UUID = Field(description="Event UUID that produced this latest state")
    event_type: Literal["insert", "update"] = Field(description="Latest event operation type")
    state: dict[str, Any] = Field(description="Latest materialized entity state payload")
    last_occurred_at: datetime = Field(description="Timestamp of latest event occurrence")
    last_updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when state was last updated in Postgres",
    )


# Backwards compatibility alias
CurrentStateRecord = OrdersCurrentState
