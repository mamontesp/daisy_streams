"""Validated domain order payload model (REQ-C03 DQ-2)."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderPayload(BaseModel):
    """
    Validated domain payload for orders.
    Enforces business logic: total price must be non-null and strictly positive (> 0).
    Supports both uppercase Snowflake aliases and lowercase snake_case fields.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    order_key: Optional[Union[str, int]] = Field(
        default=None,
        alias="O_ORDERKEY",
        description="Primary order key identifier",
    )
    cust_key: Optional[Union[str, int]] = Field(
        default=None,
        alias="O_CUSTKEY",
        description="Customer foreign key identifier",
    )
    order_status: Optional[str] = Field(
        default=None,
        alias="O_ORDERSTATUS",
        description="Order status code (e.g., 'O', 'F', 'P')",
    )
    total_price: float = Field(
        alias="O_TOTALPRICE",
        description="Total monetary price of the order",
    )
    order_date: Optional[Union[str, datetime]] = Field(
        default=None,
        alias="O_ORDERDATE",
        description="Date when the order was placed",
    )
    order_priority: Optional[str] = Field(
        default=None,
        alias="O_ORDERPRIORITY",
        description="Priority ranking of order",
    )
    clerk: Optional[str] = Field(
        default=None,
        alias="O_CLERK",
        description="Clerk identifier",
    )
    ship_priority: Optional[int] = Field(
        default=None,
        alias="O_SHIPPRIORITY",
        description="Shipping priority",
    )
    comment: Optional[str] = Field(
        default=None,
        alias="O_COMMENT",
        description="Optional freeform comment",
    )

    @field_validator("total_price")
    @classmethod
    def validate_positive_price(cls, val: float) -> float:
        """DQ-2 check: Order total price must be strictly positive (> 0)."""
        if val is None:
            raise ValueError("Order total price cannot be null.")
        if val <= 0:
            raise ValueError(f"Order total price must be strictly positive (> 0), got {val}.")
        return val
