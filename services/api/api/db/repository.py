"""PostgreSQL read-only query repository returning decoupled API models."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional, Sequence
from uuid import UUID

from services.api.api.config import APISettings
from services.api.api.models import (
    EventItem,
    EventListResponse,
    LagMetrics,
    StreamStatsResponse,
)

logger = logging.getLogger(__name__)


class APIRepository:
    """Read-only database query repository for the inspection API."""

    def __init__(
        self,
        db: Optional[Any] = None,
        settings: Optional[APISettings] = None,
    ) -> None:
        self.settings = settings or APISettings()
        if db is not None:
            self.db = db
        else:
            from services.postgres.db import get_db
            self.db = get_db()

    def check_health(self) -> bool:
        """Check PostgreSQL database connectivity."""
        try:
            row = self.db.fetchone("SELECT 1 AS ok")
            return row is not None and row.get("ok") == 1
        except Exception as exc:
            logger.error("Database health check failed: %s", exc)
            return False

    def get_events(
        self,
        entity_key: Optional[str] = None,
        event_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> EventListResponse:
        """Query paginated events from event_log with optional filters."""
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
        total_count = int(count_row["total"]) if count_row and "total" in count_row else 0

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

        events: list[EventItem] = []
        for r in rows:
            row_dict = dict(r)
            # Parse payload if stored as string (e.g. SQLite test backend)
            if isinstance(row_dict.get("payload"), str):
                try:
                    row_dict["payload"] = json.loads(row_dict["payload"])
                except Exception:
                    pass

            # Convert string UUID if necessary
            if isinstance(row_dict.get("event_id"), str):
                try:
                    row_dict["event_id"] = UUID(row_dict["event_id"])
                except Exception:
                    pass

            events.append(EventItem.model_validate(row_dict))

        return EventListResponse(
            total=total_count,
            limit=limit,
            offset=offset,
            events=events,
        )

    def get_stats(self) -> StreamStatsResponse:
        """Aggregate stream metrics: event counts, lag calculations, and watermarks."""
        # 1. Counts grouped by event_type
        type_rows = self.db.fetchall(
            "SELECT event_type, COUNT(*) AS cnt FROM event_log GROUP BY event_type"
        )
        counts_by_type = {r["event_type"]: int(r["cnt"]) for r in type_rows}

        # 2. Total DLQ quarantined count
        dlq_row = self.db.fetchone("SELECT COUNT(*) AS total FROM quarantined_events")
        quarantined_count = int(dlq_row["total"]) if dlq_row and "total" in dlq_row else 0

        # 3. Unique entities count from materialized current_state
        state_row = self.db.fetchone("SELECT COUNT(*) AS total FROM current_state")
        unique_entities_count = int(state_row["total"]) if state_row and "total" in state_row else 0

        # 4. Latency and watermark metrics
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
            total_events = int(metrics.get("total_events") or 0)
            avg_lag = float(metrics.get("avg_lag_seconds") or 0.0)
            max_lag = float(metrics.get("max_lag_seconds") or 0.0)
            last_watermark = metrics.get("last_watermark")
            last_processed_at = metrics.get("last_processed_at")

        return StreamStatsResponse(
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
