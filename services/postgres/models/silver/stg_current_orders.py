"""Representation of a row in silver.stg_current_orders (REQ-C01, REQ-C02)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal, Optional
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field


class StgCurrentOrders(BaseModel):
    """Representation of a row in the materialized `silver.stg_current_orders` table (REQ-C01, REQ-C02)."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    entity_key: str = Field(description="Primary entity key")
    order_key: Optional[int] = Field(default=None, description="Unpacked order key")
    cust_key: Optional[int] = Field(default=None, description="Customer foreign key")
    order_status: Optional[str] = Field(default=None, description="Latest order status")
    total_price: Optional[float] = Field(default=None, description="Latest order total price")
    order_date: Optional[date] = Field(default=None, description="Order date")
    order_priority: Optional[str] = Field(default=None, description="Order priority")
    clerk: Optional[str] = Field(default=None, description="Clerk ID")
    ship_priority: Optional[int] = Field(default=None, description="Shipping priority")
    comment: Optional[str] = Field(default=None, description="Order comment")

    # Joined customer fields if present
    customer_name: Optional[str] = Field(default=None, description="Customer name")
    customer_phone: Optional[str] = Field(default=None, description="Customer phone")
    customer_acct_bal: Optional[float] = Field(default=None, description="Customer account balance")
    customer_market_segment: Optional[str] = Field(default=None, description="Market segment")

    latest_event_id: UUID = Field(description="Event UUID that produced this latest state")
    event_type: Literal["insert", "update"] = Field(description="Latest event operation type")
    state: dict[str, Any] = Field(description="Latest materialized entity state payload")
    last_occurred_at: datetime = Field(description="Timestamp of latest event occurrence")
    last_updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when state was last updated in Postgres",
    )


# Backwards compatibility aliases
OrdersCurrentState = StgCurrentOrders
CurrentStateRecord = StgCurrentOrders
