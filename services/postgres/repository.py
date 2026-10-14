"""PostgreSQL Repository implementing Medallion (Bronze/Silver) materialization, DQ routing, and query contracts."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union
from uuid import UUID

from dataclasses import dataclass, field
from services.postgres.db import BaseDB, PostgresDB, SQLiteTestDB, get_db
from services.postgres.models import (
    CurrentStateRecord,
    ErrorCode,
    EventEnvelope,
    EventLogRecord,
    IngestionResult,
    OrdersAppendLog,
    OrdersCurrentState,
    QuarantinedEventLog,
    QuarantinedEventRecord,
    RawEventLog,
    RawOrdersLog,
    StgCurrentOrders,
)
from services.postgres.validator import validate_event, ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class LagMetrics:
    """Stream processing lag metrics."""
    avg_lag_seconds: float = 0.0
    max_lag_seconds: float = 0.0


@dataclass
class StatsResponse:
    """Aggregated metrics returned by get_stats()."""
    total_events: int = 0
    counts_by_type: dict[str, int] = field(default_factory=dict)
    quarantined_count: int = 0
    unique_entities_count: int = 0
    lag: LagMetrics = field(default_factory=LagMetrics)
    last_watermark: Optional[Any] = None
    last_processed_at: Optional[Any] = None


@dataclass
class EventsQueryResponse:
    """Paginated events response returned by get_events()."""
    total: int = 0
    limit: int = 50
    offset: int = 0
    events: list[Any] = field(default_factory=list)


@dataclass
class EntityHistoryResponse:
    """Current state and change history returned by get_entity_state_and_history()."""
    entity_key: str = ""
    current_state: Any = None
    history: list[Any] = field(default_factory=list)



class PostgresRepository:
    """Encapsulates all PostgreSQL data persistence, idempotent materialization, and queries across Bronze and Silver schemas."""

    def __init__(self, db: Optional[BaseDB] = None, auto_init_schema: bool = True) -> None:
        self.db = db if db is not None else get_db()
        if auto_init_schema and not isinstance(self.db, SQLiteTestDB):
            try:
                self.init_schema()
            except Exception as exc:
                logger.warning("Database schema auto-init warning: %s", exc)

    def init_schema(self, ddl_path: Optional[str] = None) -> None:
        """Execute init.sql DDL to create bronze/silver schemas, tables, and views."""
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
        """Check whether an event_id already exists in bronze.raw_event_log (for DQ-3)."""
        try:
            row = self.db.fetchone(
                "SELECT 1 AS exists_flag FROM bronze.raw_event_log WHERE event_id = %s LIMIT 1",
                (event_id,),
            )
            return row is not None
        except Exception as exc:
            logger.warning("Error checking duplicate event_id %s: %s", event_id, exc)
            return False

    def append_event_log(
        self,
        envelope: EventEnvelope,
        processed_at: Optional[datetime] = None,
    ) -> RawEventLog:
        """Insert a validated event into append-only bronze.raw_event_log (REQ-C01)."""
        now = processed_at or datetime.now(timezone.utc)
        self.db.execute(
            """
            INSERT INTO bronze.raw_event_log (
                event_id, batch_id, event_type, source, entity_key, partition_key,
                payload, occurred_at, captured_at, processed_at, schema_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                envelope.event_id,
                envelope.batch_id,
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
        return RawEventLog(
            event_id=envelope.event_id,
            batch_id=envelope.batch_id,
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
    ) -> StgCurrentOrders:
        """
        Monotonically update materialized silver.stg_current_orders (REQ-C01, REQ-C02).
        Guarantees that replaying the stream from offset 0 converges to identical state.
        """
        now = processed_at or datetime.now(timezone.utc)
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}

        # Extract structured domain fields from payload
        order_key_raw = payload.get("O_ORDERKEY") or payload.get("order_key") or envelope.entity_key
        cust_key_raw = payload.get("O_CUSTKEY") or payload.get("cust_key") or envelope.partition_key
        order_status = payload.get("O_ORDERSTATUS") or payload.get("order_status")
        total_price_raw = payload.get("O_TOTALPRICE") or payload.get("total_price")
        order_date_raw = payload.get("O_ORDERDATE") or payload.get("order_date")
        order_priority = payload.get("O_ORDERPRIORITY") or payload.get("order_priority")
        clerk = payload.get("O_CLERK") or payload.get("clerk")
        ship_priority_raw = payload.get("O_SHIPPRIORITY") or payload.get("ship_priority")
        comment = payload.get("O_COMMENT") or payload.get("comment")

        # Joined customer fields
        customer_name = payload.get("C_NAME")
        customer_phone = payload.get("C_PHONE")
        customer_acct_bal_raw = payload.get("C_ACCTBAL")
        customer_market_segment = payload.get("C_MKTSEGMENT")

        try:
            order_key = int(order_key_raw) if order_key_raw is not None else None
        except Exception:
            order_key = None

        try:
            cust_key = int(cust_key_raw) if cust_key_raw is not None else None
        except Exception:
            cust_key = None

        try:
            total_price = float(total_price_raw) if total_price_raw is not None else None
        except Exception:
            total_price = None

        try:
            ship_priority = int(ship_priority_raw) if ship_priority_raw is not None else None
        except Exception:
            ship_priority = None

        try:
            customer_acct_bal = float(customer_acct_bal_raw) if customer_acct_bal_raw is not None else None
        except Exception:
            customer_acct_bal = None

        parsed_order_date = None
        if order_date_raw:
            if isinstance(order_date_raw, date):
                parsed_order_date = order_date_raw
            elif isinstance(order_date_raw, str):
                try:
                    parsed_order_date = date.fromisoformat(order_date_raw[:10])
                except Exception:
                    parsed_order_date = None

        self.db.execute(
            """
            INSERT INTO silver.stg_current_orders (
                entity_key, order_key, cust_key, order_status, total_price, order_date,
                order_priority, clerk, ship_priority, comment,
                customer_name, customer_phone, customer_acct_bal, customer_market_segment,
                latest_event_id, event_type, state, last_occurred_at, last_updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_key) DO UPDATE SET
                order_key = EXCLUDED.order_key,
                cust_key = EXCLUDED.cust_key,
                order_status = EXCLUDED.order_status,
                total_price = EXCLUDED.total_price,
                order_date = EXCLUDED.order_date,
                order_priority = EXCLUDED.order_priority,
                clerk = EXCLUDED.clerk,
                ship_priority = EXCLUDED.ship_priority,
                comment = EXCLUDED.comment,
                customer_name = EXCLUDED.customer_name,
                customer_phone = EXCLUDED.customer_phone,
                customer_acct_bal = EXCLUDED.customer_acct_bal,
                customer_market_segment = EXCLUDED.customer_market_segment,
                latest_event_id = EXCLUDED.latest_event_id,
                event_type = EXCLUDED.event_type,
                state = EXCLUDED.state,
                last_occurred_at = EXCLUDED.last_occurred_at,
                last_updated_at = EXCLUDED.last_updated_at
            WHERE EXCLUDED.last_occurred_at >= silver.stg_current_orders.last_occurred_at
            """,
            (
                envelope.entity_key,
                order_key,
                cust_key,
                order_status,
                total_price,
                str(parsed_order_date) if parsed_order_date else None,
                order_priority,
                clerk,
                ship_priority,
                comment,
                customer_name,
                customer_phone,
                customer_acct_bal,
                customer_market_segment,
                envelope.event_id,
                envelope.event_type,
                envelope.payload,
                envelope.occurred_at,
                now,
            ),
        )
        return StgCurrentOrders(
            entity_key=envelope.entity_key,
            order_key=order_key,
            cust_key=cust_key,
            order_status=order_status,
            total_price=total_price,
            order_date=parsed_order_date,
            order_priority=order_priority,
            clerk=clerk,
            ship_priority=ship_priority,
            comment=comment,
            customer_name=customer_name,
            customer_phone=customer_phone,
            customer_acct_bal=customer_acct_bal,
            customer_market_segment=customer_market_segment,
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
    ) -> QuarantinedEventLog:
        """Persist failed records into bronze.quarantined_event_log DLQ (REQ-C03)."""
        err_str = error_code.value if isinstance(error_code, ErrorCode) else str(error_code)
        now = datetime.now(timezone.utc)
        self.db.execute(
            """
            INSERT INTO bronze.quarantined_event_log (
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
        return QuarantinedEventLog(
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
          2. If invalid: quarantine to bronze.quarantined_event_log without updating silver.stg_current_orders.
          3. If valid: append to bronze.raw_event_log and monotonically upsert to silver.stg_current_orders.
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

        # Ingest atomically into bronze.raw_event_log and silver.stg_current_orders
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
        """Query paginated events from bronze.raw_event_log filtered by entity_key, type, and occurred_at range."""
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
        count_sql = f"SELECT COUNT(*) AS total FROM bronze.raw_event_log {where_clause}"
        count_row = self.db.fetchone(count_sql, tuple(params))
        total_count = count_row["total"] if count_row else 0

        # Paginated items query
        data_sql = f"""
        SELECT
            id, event_id, event_type, source, entity_key, partition_key,
            payload, occurred_at, captured_at, processed_at, schema_version
        FROM bronze.raw_event_log
        {where_clause}
        ORDER BY occurred_at DESC, id DESC
        LIMIT %s OFFSET %s
        """
        data_params = list(params) + [limit, offset]
        rows = self.db.fetchall(data_sql, tuple(data_params))

        # Convert rows to RawEventLog models
        event_records: list[RawEventLog] = []
        for r in rows:
            try:
                event_records.append(RawEventLog.model_validate(r))
            except Exception:
                event_records.append(r)  # type: ignore

        return EventsQueryResponse(
            total=total_count,
            limit=limit,
            offset=offset,
            events=event_records,
        )

    def get_entity_state_and_history(self, entity_key: str) -> Optional[EntityHistoryResponse]:
        """Return materialized silver.stg_current_orders along with full chronological change history."""
        # 1. Fetch current materialized state from silver
        current = self.db.fetchone(
            """
            SELECT entity_key, order_key, cust_key, order_status, total_price, order_date,
                   order_priority, clerk, ship_priority, comment, customer_name, customer_phone,
                   customer_acct_bal, customer_market_segment, latest_event_id, event_type, state,
                   last_occurred_at, last_updated_at
            FROM silver.stg_current_orders
            WHERE entity_key = %s
            """,
            (entity_key,),
        )
        if current is None:
            return None

        # 2. Fetch full history from bronze.raw_event_log in chronological order
        history = self.db.fetchall(
            """
            SELECT
                id, event_id, event_type, source, entity_key, partition_key,
                payload, occurred_at, captured_at, processed_at, schema_version
            FROM bronze.raw_event_log
            WHERE entity_key = %s
            ORDER BY occurred_at ASC, id ASC
            """,
            (entity_key,),
        )

        try:
            current_model = StgCurrentOrders.model_validate(current)
        except Exception:
            current_model = current  # type: ignore

        history_models: list[RawEventLog] = []
        for h in history:
            try:
                history_models.append(RawEventLog.model_validate(h))
            except Exception:
                history_models.append(h)  # type: ignore

        return EntityHistoryResponse(
            entity_key=entity_key,
            current_state=current_model,
            history=history_models,
        )

    def get_stats(self) -> StatsResponse:
        """Aggregate event counts by type, lag metrics, and last processed watermark."""
        # Counts by event type from bronze.raw_event_log
        type_rows = self.db.fetchall(
            "SELECT event_type, COUNT(*) AS cnt FROM bronze.raw_event_log GROUP BY event_type"
        )
        counts_by_type = {r["event_type"]: r["cnt"] for r in type_rows}

        # Quarantined count from bronze.quarantined_event_log
        dlq_row = self.db.fetchone("SELECT COUNT(*) AS total FROM bronze.quarantined_event_log")
        quarantined_count = dlq_row["total"] if dlq_row else 0

        # Unique entities in silver.stg_current_orders
        state_row = self.db.fetchone("SELECT COUNT(*) AS total FROM silver.stg_current_orders")
        unique_entities_count = state_row["total"] if state_row else 0

        # Watermark and lag metrics from bronze.raw_event_log
        metrics_sql = """
        SELECT
            COUNT(*) AS total_events,
            AVG(EXTRACT(EPOCH FROM (processed_at - occurred_at))) AS avg_lag_seconds,
            MAX(EXTRACT(EPOCH FROM (processed_at - occurred_at))) AS max_lag_seconds,
            MAX(occurred_at) AS last_watermark,
            MAX(processed_at) AS last_processed_at
        FROM bronze.raw_event_log
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

    # =========================================================================
    # Bronze & Silver Specific Query and Refresh Methods
    # =========================================================================

    def get_raw_orders_log(
        self,
        entity_key: Optional[str] = None,
        order_key: Optional[int] = None,
        cust_key: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[RawOrdersLog]:
        """Query unpacked order events from bronze.raw_orders_log."""
        conditions: list[str] = []
        params: list[Any] = []

        if entity_key:
            conditions.append("entity_key = %s")
            params.append(entity_key)

        if order_key is not None:
            conditions.append("order_key = %s")
            params.append(order_key)

        if cust_key is not None:
            conditions.append("cust_key = %s")
            params.append(cust_key)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
        SELECT *
        FROM bronze.raw_orders_log
        {where_clause}
        ORDER BY occurred_at DESC, log_id DESC
        LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        rows = self.db.fetchall(sql, tuple(params))
        result: list[RawOrdersLog] = []
        for r in rows:
            try:
                result.append(RawOrdersLog.model_validate(r))
            except Exception:
                result.append(r)  # type: ignore
        return result

    def refresh_raw_orders_log(self, concurrently: bool = True) -> None:
        """Refresh the bronze.raw_orders_log materialized view."""
        if isinstance(self.db, SQLiteTestDB):
            return  # SQLite bronze.raw_orders_log is a dynamic VIEW
        clause = "CONCURRENTLY " if concurrently else ""
        self.db.execute(f"REFRESH MATERIALIZED VIEW {clause}bronze.raw_orders_log")

    def get_stg_current_orders(
        self,
        order_key: Optional[int] = None,
        cust_key: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StgCurrentOrders]:
        """Query cleaned latest order states from silver.stg_current_orders."""
        conditions: list[str] = []
        params: list[Any] = []

        if order_key is not None:
            conditions.append("order_key = %s")
            params.append(order_key)

        if cust_key is not None:
            conditions.append("cust_key = %s")
            params.append(cust_key)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
        SELECT *
        FROM silver.stg_current_orders
        {where_clause}
        ORDER BY last_occurred_at DESC
        LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        rows = self.db.fetchall(sql, tuple(params))
        result: list[StgCurrentOrders] = []
        for r in rows:
            try:
                result.append(StgCurrentOrders.model_validate(r))
            except Exception:
                result.append(r)  # type: ignore
        return result
