"""Entity current state and chronological history response model (REQ-C04)."""

from __future__ import annotations

from typing import Any, Union
from pydantic import BaseModel, Field
from services.postgres.models.orders_current_state import OrdersCurrentState
from services.postgres.models.orders_append_log import OrdersAppendLog


class EntityHistoryResponse(BaseModel):
    """Materialized state plus full audit trail returned by GET /entities/{key}."""

    entity_key: str = Field(description="Entity primary key identifier")
    current_state: Union[OrdersCurrentState, dict[str, Any]] = Field(
        description="Current materialized snapshot"
    )
    history: list[Union[OrdersAppendLog, dict[str, Any]]] = Field(
        default_factory=list, description="Chronological list of all events for this entity"
    )
