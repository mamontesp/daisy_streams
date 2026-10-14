"""Representation of a row in bronze.raw_orders_log materialized view."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field


class RawOrdersLog(BaseModel):
    """Representation of an unpacked order event from `bronze.raw_orders_log`."""

    model_config = ConfigDict(from_attributes=True, extra="ignore")

    log_id: int = Field(description="Auto-increment sequence ID from raw_event_log")
    event_id: UUID = Field(description="Event UUID")
    batch_id: Optional[str] = Field(default=None, description="Sync batch correlation ID")
    event_type: str = Field(description="Operation type (insert / update)")
    source: str = Field(description="Event source system")
    entity_key: str = Field(description="Primary entity key")
    partition_key: Optional[str] = Field(default=None, description="Partition key")

    # Unpacked domain order fields
    order_key: Optional[int] = Field(default=None, description="Unpacked order key")
    cust_key: Optional[int] = Field(default=None, description="Customer foreign key")
    order_status: Optional[str] = Field(default=None, description="Order status code")
    total_price: Optional[float] = Field(default=None, description="Total order price")
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

    occurred_at: datetime = Field(description="Timestamp when event occurred at source")
    captured_at: datetime = Field(description="Timestamp when event was captured")
    processed_at: datetime = Field(description="Timestamp when event was processed into Postgres")
    schema_version: str = Field(default="1.0.0", description="Envelope schema version")


# Aliases
RawOrderEvents = RawOrdersLog
RawOrderEvent = RawOrdersLog
