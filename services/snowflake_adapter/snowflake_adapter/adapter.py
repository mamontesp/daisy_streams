"""Snowflake Ingestion Adapter with incremental sync and MinIO state persistence."""

from __future__ import annotations

import decimal
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from snowflake_adapter.client import SnowflakeClient, SnowflakeConnectorClient
from snowflake_adapter.queries import build_incremental_sync_query, build_initial_sync_query
from snowflake_adapter.config import Settings, get_settings
from snowflake_adapter.logging import correlation_batch_id, get_logger
from snowflake_adapter.models.envelope import EventEnvelope
from snowflake_adapter.models.state import SyncState
from snowflake_adapter.state.base import StateStore
from snowflake_adapter.state import get_state_store
from snowflake_adapter.transport.base import EventProducer
from snowflake_adapter.transport import get_event_producer

logger = get_logger(__name__)


@dataclass
class SyncResult:
    """Outcome summary of a synchronization run."""
    source_table: str
    batch_id: str
    records_captured: int
    total_records_synced: int
    prior_watermark: Optional[str]
    current_watermark: Optional[str]
    is_initial_sync: bool


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Serialize database row values into JSON-safe python primitives."""
    serialized: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            serialized[k] = v.isoformat()
        elif isinstance(v, decimal.Decimal):
            serialized[k] = float(v)
        elif isinstance(v, (bytes, bytearray)):
            serialized[k] = v.hex()
        else:
            serialized[k] = v
    return serialized


def _parse_watermark_to_datetime(val: Any) -> datetime:
    """Convert a watermark column value to timezone-aware datetime."""
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val
    elif isinstance(val, date):
        return datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
    elif isinstance(val, str):
        try:
            if len(val) == 10 and val.count("-") == 2:
                d = date.fromisoformat(val)
                return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


class SnowflakeAdapter:
    """Snowflake Ingestion Adapter with monotonic watermark incremental synchronization."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        client: Optional[SnowflakeClient] = None,
        state_store: Optional[StateStore] = None,
        producer: Optional[EventProducer] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.state_store = state_store or get_state_store(self.settings)
        self.producer = producer or get_event_producer(self.settings)

        if client is not None:
            self.client = client
        else:
            self.client = SnowflakeConnectorClient(
                account=self.settings.snowflake_account,
                user=self.settings.snowflake_user,
                authenticator=self.settings.snowflake_authenticator,
                private_key_path=self.settings.snowflake_private_key_path,
                private_key_content=self.settings.snowflake_private_key_content,
                private_key_passphrase=self.settings.snowflake_private_key_passphrase,
                role=self.settings.snowflake_role,
                warehouse=self.settings.snowflake_warehouse,
                database=self.settings.snowflake_database,
                schema=self.settings.snowflake_schema,
            )

    def run_sync(
        self,
        table: Optional[str] = None,
        batch_size: Optional[int] = None,
        join_customer: bool = True,
        full_refresh: bool = False,
        topic: Optional[str] = None,
        state_key: Optional[str] = None,
    ) -> SyncResult:
        """Execute a synchronization run (initial bounded, incremental delta, or full refresh)."""
        source_table = table or self.settings.snowflake_table
        batch_id = str(uuid4())
        correlation_batch_id.set(batch_id)

        database = self.settings.snowflake_database
        schema = self.settings.snowflake_schema
        watermark_col = self.settings.snowflake_watermark_column
        entity_key_col = self.settings.snowflake_entity_key_column
        partition_col = self.settings.snowflake_partition_key_column

        effective_state_key = (state_key or source_table).lower()

        # 1. Load prior state (or bypass for full refresh)
        if full_refresh:
            logger.info("Full refresh requested for %s. Bypassing prior state checkpoint.", source_table)
            prior_state = None
            prior_watermark = None
            cumulative_synced = 0
            is_initial = True
        else:
            prior_state = self.state_store.load_state(effective_state_key)
            prior_watermark = prior_state.last_watermark if prior_state else None
            cumulative_synced = prior_state.records_synced if prior_state else 0
            is_initial = prior_watermark is None

        logger.info(
            "Initiating sync for table %s (batch_id: %s, mode: %s, prior_watermark: %s, topic: %s)",
            source_table,
            batch_id,
            "FULL_REFRESH" if full_refresh else ("INITIAL" if is_initial else "INCREMENTAL"),
            prior_watermark,
            topic or self.settings.kafka_topic,
        )

        # 2. Build and execute SQL query
        if is_initial:
            limit = batch_size or self.settings.sync_initial_bound
            limit = max(10000, min(50000, limit)) if limit > 50000 or limit < 10000 else limit

            sql = build_initial_sync_query(
                database=database,
                schema=schema,
                table=source_table,
                watermark_column=watermark_col,
                entity_key_column=entity_key_col,
                partition_key_column=partition_col,
                limit=limit,
                join_customer=join_customer,
            )
            raw_rows = self.client.execute_query(sql)
        else:
            limit = batch_size or self.settings.sync_batch_size
            sql = build_incremental_sync_query(
                database=database,
                schema=schema,
                table=source_table,
                watermark_column=watermark_col,
                entity_key_column=entity_key_col,
                partition_key_column=partition_col,
                limit=limit,
                join_customer=join_customer,
            )
            raw_rows = self.client.execute_query(sql, {"watermark": prior_watermark})

        records_count = len(raw_rows)
        logger.info(
            "Extracted %d records from Snowflake %s.%s.%s",
            records_count,
            database,
            schema,
            source_table,
        )

        # 3. AC-A02: Return cleanly with zero events if unmodified
        if records_count == 0:
            logger.info("No delta records found above watermark %s. Zero events emitted.", prior_watermark)
            return SyncResult(
                source_table=source_table,
                batch_id=batch_id,
                records_captured=0,
                total_records_synced=cumulative_synced,
                prior_watermark=prior_watermark,
                current_watermark=prior_watermark,
                is_initial_sync=is_initial,
            )

        # 4. Transform records into EventEnvelope instances
        source_id = f"snowflake.{database.lower()}.{schema.lower()}.{source_table.lower()}"
        envelopes: list[EventEnvelope] = []
        max_watermark_dt: Optional[datetime] = None
        last_entity_key: Optional[str] = None

        for row in raw_rows:
            key_val = (
                row.get(entity_key_col)
                or row.get(entity_key_col.lower())
                or row.get("O_CUSTKEY")
                or row.get("o_custkey")
                or row.get("C_CUSTKEY")
                or row.get("c_custkey")
                or row.get("ORDER_ID")
            )
            entity_key = str(key_val) if key_val is not None else str(uuid4())

            part_val = (
                row.get(partition_col)
                or row.get(partition_col.lower())
                or row.get("O_CUSTKEY")
                or row.get("o_custkey")
                or row.get("C_CUSTKEY")
                or row.get("c_custkey")
                or row.get("CUSTOMER_ID")
            )
            partition_key = str(part_val) if part_val is not None else entity_key

            wm_raw = row.get(watermark_col) or row.get(watermark_col.lower()) or row.get("UPDATED_AT")
            occurred_dt = _parse_watermark_to_datetime(wm_raw)

            if max_watermark_dt is None or occurred_dt > max_watermark_dt:
                max_watermark_dt = occurred_dt
                last_entity_key = entity_key

            event_type = "insert" if is_initial else "update"
            if "EVENT_TYPE" in row:
                event_type = str(row["EVENT_TYPE"]).lower()
            elif "event_type" in row:
                event_type = str(row["event_type"]).lower()

            payload = _serialize_row(row)

            envelope = EventEnvelope(
                event_type=event_type,  # type: ignore[arg-type]
                source=source_id,
                entity_key=entity_key,
                partition_key=partition_key,
                payload=payload,
                occurred_at=occurred_dt,
            )
            envelopes.append(envelope)

        # 5. Publish batch to streaming transport
        self.producer.publish_batch(envelopes, topic=topic)
        self.producer.flush()

        # 6. Format new watermark
        current_watermark_str = max_watermark_dt.isoformat() if max_watermark_dt else prior_watermark
        new_cumulative_synced = cumulative_synced + records_count

        # 7. Checkpoint state
        new_state = SyncState(
            source_table=effective_state_key,
            last_watermark=current_watermark_str,
            last_batch_id=batch_id,
            last_synced_at=datetime.now(timezone.utc).isoformat(),
            records_synced=new_cumulative_synced,
            last_entity_key=last_entity_key,
        )
        self.state_store.save_state(new_state)

        logger.info(
            "Sync batch %s completed successfully. Advanced watermark from %s to %s. Emitted %d events (Total synced: %d).",
            batch_id,
            prior_watermark,
            current_watermark_str,
            records_count,
            new_cumulative_synced,
        )

        return SyncResult(
            source_table=source_table,
            batch_id=batch_id,
            records_captured=records_count,
            total_records_synced=new_cumulative_synced,
            prior_watermark=prior_watermark,
            current_watermark=current_watermark_str,
            is_initial_sync=is_initial,
        )
