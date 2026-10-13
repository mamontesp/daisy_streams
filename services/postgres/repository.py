"""PostgreSQL Repository implementing dual-table materialization, DQ routing, and query contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union
from uuid import UUID

from snowflake_adapter.logging import get_logger
from snowflake_adapter.models.envelope import EventEnvelope
from services.postgres.db import BaseDB, PostgresDB, SQLiteTestDB, get_db
from services.postgres.models import (
    CurrentStateRecord,
    EntityHistoryResponse,
    ErrorCode,
    EventLogRecord,
    EventsQueryResponse,
    IngestionResult,
    LagMetrics,
    OrdersAppendLog,
    OrdersCurrentState,
    QuarantinedEventRecord,
    StatsResponse,
)
from services.postgres.validator import validate_event, ValidationResult

logger = get_logger(__name__)


class PostgresRepository:
    """Encapsulates all PostgreSQL data persistence, idempotent materialization, and queries."""

    def __init__(self, db: Optional[BaseDB] = None) -> None:
        self.db = db if db is not None else get_db()

    def init_schema(self, ddl_path: Optional[str] = None) -> None:
        """Execute init.sql DDL to create tables and indexes."""
        if isinstance(self.db, SQLiteTestDB):
            return  # SQLite schema initialized automatically

        if ddl_path is None:
            ddl_file = Path(__file__).parent / "init.sql"
        else:
            ddl_file = Path(ddl_path)

        if not ddl_file.exists():
            raise FileNotFoundError(f"DDL file not found at {ddl_file}")

        sql = ddl_file.read_text(encoding="utf-8")
        self.db.execute(sql)
        logger.info("Executed schema DDL from %s", ddl_file)

    def check_event_exists(self, event_id: UUID) -> bool:
        """Check whether an event_id already exists in event_log (for DQ-3)."""
        row = self.db.fetchone(
            "SELECT 1 AS exists_flag FROM event_log WHERE event_id = %s LIMIT 1",
            (event_id,),
        )
        return row is not None

    def append_event_log(
        self,
        envelope: EventEnvelope,
        processed_at: Optional[datetime] = None,
    ) -> OrdersAppendLog:
        """Insert a validated event into append-only event_log (REQ-C01)."""
        now = processed_at or datetime.now(timezone.utc)
        self.db.execute(
            """
            INSERT INTO event_log (
                event_id, event_type, source, entity_key, partition_key,
                payload, occurred_at, captured_at, processed_at, schema_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                envelope.event_id,
                envelope.event_type,
                envelope.source,
                envelope.entity_key,
                envelope.partition_key,
                envelope.payload,
                envelope.occurred_at,
                envelope.captured_at,
                now,
                envelope.schema_version,
            ),
        )
        return OrdersAppendLog(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            source=envelope.source,
            entity_key=envelope.entity_key,
            partition_key=envelope.partition_key,
            payload=envelope.payload,
            occurred_at=envelope.occurred_at,
            captured_at=envelope.captured_at,
            processed_at=now,
            schema_version=envelope.schema_version,
        )

    def upsert_current_state(
        self,
        envelope: EventEnvelope,
        processed_at: Optional[datetime] = None,
    ) -> OrdersCurrentState:
        """
        Monotonically update materialized current_state (REQ-C01, REQ-C02).
        Guarantees that replaying the stream from offset 0 converges to identical state.
        """
        now = processed_at or datetime.now(timezone.utc)
        self.db.execute(
            """
            INSERT INTO current_state (
                entity_key, latest_event_id, event_type, state, last_occurred_at, last_updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_key) DO UPDATE SET
                latest_event_id = EXCLUDED.latest_event_id,
                event_type = EXCLUDED.event_type,
                state = EXCLUDED.state,
                last_occurred_at = EXCLUDED.last_occurred_at,
                last_updated_at = EXCLUDED.last_updated_at
            WHERE EXCLUDED.last_occurred_at >= current_state.last_occurred_at
            """,
            (
                envelope.entity_key,
                envelope.event_id,
                envelope.event_type,
                envelope.payload,
                envelope.occurred_at,
                now,
            ),
        )
        return OrdersCurrentState(
            entity_key=envelope.entity_key,
            latest_event_id=envelope.event_id,
            event_type=envelope.event_type,
            state=envelope.payload,
            last_occurred_at=envelope.occurred_at,
            last_updated_at=now,
        )

    def record_quarantined_event(
        self,
        error_code: Union[ErrorCode, str],
        error_message: str,
        raw_payload: dict[str, Any],
        event_id: Optional[UUID] = None,
        entity_key: Optional[str] = None,
    ) -> QuarantinedEventRecord:
        """Persist failed records into quarantined_events DLQ (REQ-C03)."""
        err_str = error_code.value if isinstance(error_code, ErrorCode) else str(error_code)
        now = datetime.now(timezone.utc)
        self.db.execute(
            """
            INSERT INTO quarantined_events (
                event_id, entity_key, error_code, error_message, raw_payload, quarantined_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                event_id,
                entity_key,
                err_str,
                error_message,
                raw_payload,
                now,
            ),
        )
        parsed_err = ErrorCode(err_str) if err_str in ErrorCode._value2member_map_ else ErrorCode.UNKNOWN_ERROR
        return QuarantinedEventRecord(
            event_id=event_id,
            entity_key=entity_key,
            error_code=parsed_err,
            error_message=error_message,
            raw_payload=raw_payload,
            quarantined_at=now,
        )

    def ingest_event(
        self,
        raw_event: Union[dict[str, Any], EventEnvelope],
        processed_at: Optional[datetime] = None,
    ) -> IngestionResult:
        """
        Atomic consumer ingestion pipeline:
          1. Run Data Quality checks (DQ-1, DQ-2, DQ-3).
          2. If invalid: quarantine to DLQ without updating current_state.
          3. If valid: append to event_log and monotonically upsert to current_state.
        """
        res: ValidationResult = validate_event(
            raw_event,
            event_id_checker=self.check_event_exists,
        )

        now = processed_at or datetime.now(timezone.utc)

        if not res.is_valid:
            self.record_quarantined_event(
                error_code=res.error_code or ErrorCode.UNKNOWN_ERROR.value,
                error_message=res.error_message or "Data quality check failed",
                raw_payload=res.raw_payload or {},
                event_id=res.event_id,
                entity_key=res.entity_key,
            )
            return IngestionResult(
                status="quarantined",
                error_code=res.error_code,
                error_message=res.error_message,
                event_id=str(res.event_id) if res.event_id else None,
                entity_key=res.entity_key,
                processed_at=now,
            )

        assert res.envelope is not None
        envelope = res.envelope

        # Ingest atomically into event_log and current_state
        self.append_event_log(envelope, processed_at=now)
        self.upsert_current_state(envelope, processed_at=now)

        return IngestionResult(
            status="processed",
            event_id=str(envelope.event_id),
            entity_key=envelope.entity_key,
            event_type=envelope.event_type,
            processed_at=now,
        )

    # =========================================================================
    # REQ-C04: Queries supporting FastAPI Endpoints
    # =========================================================================

    def get_events(
        self,
        entity_key: Optional[str] = None,
        event_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> EventsQueryResponse:
        """Query paginated events filtered by entity_key, type, and occurred_at range."""
        conditions: list[str] = []
        params: list[Any] = []

        if entity_key:
            conditions.append("entity_key = %s")
            params.append(entity_key)

        if event_type:
            conditions.append("event_type = %s")
            params.append(event_type)

        if start_time:
            conditions.append("occurred_at >= %s")
            params.append(start_time)

        if end_time:
            conditions.append("occurred_at <= %s")
            params.append(end_time)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # Total count query
        count_sql = f"SELECT COUNT(*) AS total FROM event_log {where_clause}"
        count_row = self.db.fetchone(count_sql, tuple(params))
        total_count = count_row["total"] if count_row else 0

        # Paginated items query
        data_sql = f"""
        SELECT
            id, event_id, event_type, source, entity_key, partition_key,
            payload, occurred_at, captured_at, processed_at, schema_version
        FROM event_log
        {where_clause}
        ORDER BY occurred_at DESC, id DESC
        LIMIT %s OFFSET %s
        """
        data_params = list(params) + [limit, offset]
        rows = self.db.fetchall(data_sql, tuple(data_params))

        # Convert rows to OrdersAppendLog models
        event_records: list[OrdersAppendLog] = []
        for r in rows:
            try:
                event_records.append(OrdersAppendLog.model_validate(r))
            except Exception:
                event_records.append(r)  # type: ignore

        return EventsQueryResponse(
            total=total_count,
            limit=limit,
            offset=offset,
            events=event_records,
        )

    def get_entity_state_and_history(self, entity_key: str) -> Optional[EntityHistoryResponse]:
        """Return materialized entity state along with full chronological change history."""
        # 1. Fetch current materialized state
        current = self.db.fetchone(
            """
            SELECT entity_key, latest_event_id, event_type, state, last_occurred_at, last_updated_at
            FROM current_state
            WHERE entity_key = %s
            """,
            (entity_key,),
        )
        if current is None:
            return None

        # 2. Fetch full history from event_log in chronological order
        history = self.db.fetchall(
            """
            SELECT
                id, event_id, event_type, source, entity_key, partition_key,
                payload, occurred_at, captured_at, processed_at, schema_version
            FROM event_log
            WHERE entity_key = %s
            ORDER BY occurred_at ASC, id ASC
            """,
            (entity_key,),
        )

        try:
            current_model = OrdersCurrentState.model_validate(current)
        except Exception:
            current_model = current  # type: ignore

        history_models: list[OrdersAppendLog] = []
        for h in history:
            try:
                history_models.append(OrdersAppendLog.model_validate(h))
            except Exception:
                history_models.append(h)  # type: ignore

        return EntityHistoryResponse(
            entity_key=entity_key,
            current_state=current_model,
            history=history_models,
        )

    def get_stats(self) -> StatsResponse:
        """Aggregate event counts by type, lag metrics, and last processed watermark."""
        # Counts by event type
        type_rows = self.db.fetchall(
            "SELECT event_type, COUNT(*) AS cnt FROM event_log GROUP BY event_type"
        )
        counts_by_type = {r["event_type"]: r["cnt"] for r in type_rows}

        # Quarantined count
        dlq_row = self.db.fetchone("SELECT COUNT(*) AS total FROM quarantined_events")
        quarantined_count = dlq_row["total"] if dlq_row else 0

        # Unique entities in current_state
        state_row = self.db.fetchone("SELECT COUNT(*) AS total FROM current_state")
        unique_entities_count = state_row["total"] if state_row else 0

        # Watermark and lag metrics
        metrics_sql = """
        SELECT
            COUNT(*) AS total_events,
            AVG(EXTRACT(EPOCH FROM (processed_at - occurred_at))) AS avg_lag_seconds,
            MAX(EXTRACT(EPOCH FROM (processed_at - occurred_at))) AS max_lag_seconds,
            MAX(occurred_at) AS last_watermark,
            MAX(processed_at) AS last_processed_at
        FROM event_log
        """
        metrics = self.db.fetchone(metrics_sql)

        total_events = 0
        avg_lag = 0.0
        max_lag = 0.0
        last_watermark = None
        last_processed_at = None

        if metrics:
            total_events = metrics.get("total_events") or 0
            avg_lag = float(metrics.get("avg_lag_seconds") or 0.0)
            max_lag = float(metrics.get("max_lag_seconds") or 0.0)
            last_watermark = metrics.get("last_watermark")
            last_processed_at = metrics.get("last_processed_at")

        return StatsResponse(
            total_events=total_events,
            counts_by_type=counts_by_type,
            quarantined_count=quarantined_count,
            unique_entities_count=unique_entities_count,
            lag=LagMetrics(
                avg_lag_seconds=round(avg_lag, 4),
                max_lag_seconds=round(max_lag, 4),
            ),
            last_watermark=last_watermark,
            last_processed_at=last_processed_at,
        )
