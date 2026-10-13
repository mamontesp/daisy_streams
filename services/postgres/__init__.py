"""PostgreSQL Storage and Sink Package (Part C Materialization & Inspection Queries)."""

from __future__ import annotations

from services.postgres.config import PostgresSettings
from services.postgres.db import BaseDB, PostgresDB, SQLiteTestDB, get_db
from services.postgres.models import (
    CurrentStateRecord,
    EntityHistoryResponse,
    ErrorCode,
    EventLogRecord,
    EventsQueryResponse,
    IngestionResult,
    LagMetrics,
    OrderPayload,
    OrdersAppendLog,
    OrdersCurrentState,
    QuarantinedEventRecord,
    StatsResponse,
)
from services.postgres.repository import PostgresRepository
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
    "OrderPayload",
    "OrdersAppendLog",
    "OrdersCurrentState",
    "EventLogRecord",
    "CurrentStateRecord",
    "QuarantinedEventRecord",
    "IngestionResult",
    "ErrorCode",
    "LagMetrics",
    "StatsResponse",
    "EventsQueryResponse",
    "EntityHistoryResponse",
]
