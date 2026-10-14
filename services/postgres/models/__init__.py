"""Pydantic models for PostgreSQL streaming ingestion, validation, and storage across Bronze and Silver schemas."""

from __future__ import annotations

from services.postgres.models.bronze import (
    EventLogRecord,
    OrdersAppendLog,
    QuarantinedEventLog,
    QuarantinedEventRecord,
    RawEventLog,
    RawOrderEvent,
    RawOrderEvents,
    RawOrdersLog,
)
from services.postgres.models.enums import ErrorCode
from services.postgres.models.event_envelope import EventEnvelope
from services.postgres.models.ingestion_result import IngestionResult
from services.postgres.models.order_payload import OrderPayload
from services.postgres.models.silver import (
    CurrentStateRecord,
    OrdersCurrentState,
    StgCurrentOrders,
)

__all__ = [
    # Bronze layer storage models
    "RawEventLog",
    "RawOrdersLog",
    "RawOrderEvents",
    "RawOrderEvent",
    "QuarantinedEventLog",
    # Silver layer storage models
    "StgCurrentOrders",
    # Event envelope & domain payload models
    "EventEnvelope",
    "OrderPayload",
    "ErrorCode",
    "IngestionResult",
    # Backwards compatibility aliases
    "OrdersAppendLog",
    "OrdersCurrentState",
    "EventLogRecord",
    "CurrentStateRecord",
    "QuarantinedEventRecord",
]
