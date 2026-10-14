"""PostgreSQL Storage and Sink Package (Part C Materialization & Ingestion)."""

from __future__ import annotations

from services.postgres.config import PostgresSettings
from services.postgres.db import BaseDB, PostgresDB, SQLiteTestDB, get_db
from services.postgres.models import (
    CurrentStateRecord,
    ErrorCode,
    EventEnvelope,
    EventLogRecord,
    IngestionResult,
    OrderPayload,
    OrdersAppendLog,
    OrdersCurrentState,
    QuarantinedEventLog,
    QuarantinedEventRecord,
    RawEventLog,
    RawOrderEvent,
    RawOrderEvents,
    RawOrdersLog,
    StgCurrentOrders,
)
from services.postgres.repository import (
    EntityHistoryResponse,
    EventsQueryResponse,
    LagMetrics,
    PostgresRepository,
    StatsResponse,
)
from services.postgres.validator import ValidationResult, validate_event

__all__ = [
    "PostgresSettings",
    "BaseDB",
    "PostgresDB",
    "SQLiteTestDB",
    "get_db",
    "PostgresRepository",
    "ValidationResult",
    "validate_event",
    # Event Processing Models
    "RawEventLog",
    "RawOrdersLog",
    "RawOrderEvents",
    "RawOrderEvent",
    "QuarantinedEventLog",
    "StgCurrentOrders",
    "EventEnvelope",
    "OrderPayload",
    "ErrorCode",
    "IngestionResult",
    # Backward Compatibility Aliases
    "OrdersAppendLog",
    "OrdersCurrentState",
    "EventLogRecord",
    "CurrentStateRecord",
    "QuarantinedEventRecord",
    # Query Dataclasses
    "LagMetrics",
    "StatsResponse",
    "EventsQueryResponse",
    "EntityHistoryResponse",
]
