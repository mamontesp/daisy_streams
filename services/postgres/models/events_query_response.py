"""Paginated events response model (REQ-C04)."""

from __future__ import annotations

from typing import Any, Union
from pydantic import BaseModel, Field
from services.postgres.models.orders_append_log import OrdersAppendLog


class EventsQueryResponse(BaseModel):
    """Paginated events response returned by GET /events."""

    total: int = Field(description="Total matching events across all pages")
    limit: int = Field(description="Requested page size limit")
    offset: int = Field(description="Requested offset")
    events: list[Union[OrdersAppendLog, dict[str, Any]]] = Field(
        default_factory=list, description="List of event records"
    )
