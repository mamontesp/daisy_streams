"""Unit and integration tests for PostgreSQL storage layer (Part C).

Covers:
  - AC-C01: Valid events materialize in event_log and current_state.
  - AC-C02: Offset 0 replay guarantees identical current_state.
  - AC-C03: Data Quality (negative prices, schema issues, duplicate event IDs) quarantine in DLQ.
  - REQ-C04: Queries for FastAPI inspection contracts (GET /events, GET /entities/{key}, GET /stats).
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from services.postgres.models import EventEnvelope
from services.postgres.repository import PostgresRepository


def _make_envelope(
    entity_key: str = "order-1001",
    event_type: str = "insert",
    price: float = 150.75,
    occurred_at: datetime = None,
    event_id: str = None,
) -> EventEnvelope:
    """Helper creating a valid EventEnvelope instance."""
    now = occurred_at or datetime.now(timezone.utc)
    return EventEnvelope(
        event_id=uuid4() if event_id is None else event_id,
        event_type=event_type,
        source="snowflake.daisy.ingestion.orders",
        entity_key=entity_key,
        payload={
            "O_ORDERKEY": entity_key,
            "O_CUSTKEY": "cust-55",
            "O_TOTALPRICE": price,
            "O_ORDERSTATUS": "O",
            "O_ORDERDATE": now.strftime("%Y-%m-%d"),
        },
        occurred_at=now,
    )


def test_ac_c01_valid_event_populates_both_tables(test_repo: PostgresRepository) -> None:
    """AC-C01: Given valid events, both event_log and current_state are populated."""
    envelope = _make_envelope(entity_key="ord-101", price=250.0)
    result = test_repo.ingest_event(envelope)

    assert result.status == "processed"
    assert result.event_id == str(envelope.event_id)
    assert result.entity_key == "ord-101"

    # Verify event_log
    events = test_repo.get_events(entity_key="ord-101")
    assert events.total == 1
    assert len(events.events) == 1
    logged_event = events.events[0]
    assert logged_event.event_id == envelope.event_id
    assert logged_event.entity_key == "ord-101"
    assert logged_event.payload["O_TOTALPRICE"] == 250.0

    # Verify current_state
    entity_details = test_repo.get_entity_state_and_history("ord-101")
    assert entity_details is not None
    assert entity_details.current_state.entity_key == "ord-101"
    assert entity_details.current_state.latest_event_id == envelope.event_id
    assert entity_details.current_state.state["O_TOTALPRICE"] == 250.0
    assert len(entity_details.history) == 1


def test_ac_c02_offset_zero_replay_idempotency(test_repo: PostgresRepository) -> None:
    """
    AC-C02: Given full offset 0 replay, current_state row count and column values remain identical.
    Also verifies monotonic upsert avoids regressions on out-of-order events.
    """
    base_time = datetime(2026, 10, 12, 10, 0, 0, tzinfo=timezone.utc)

    # Initial stream processing: 3 events across 2 entities
    e1 = _make_envelope("ord-201", "insert", 100.0, occurred_at=base_time)
    e2 = _make_envelope("ord-202", "insert", 200.0, occurred_at=base_time + timedelta(minutes=1))
    e3 = _make_envelope("ord-201", "update", 120.0, occurred_at=base_time + timedelta(minutes=5))

    test_repo.ingest_event(e1)
    test_repo.ingest_event(e2)
    test_repo.ingest_event(e3)

    # Snapshot state prior to replay
    initial_stats = test_repo.get_stats()
    assert initial_stats.unique_entities_count == 2
    assert initial_stats.total_events == 3

    state_ord_201_before = test_repo.get_entity_state_and_history("ord-201").current_state
    state_ord_202_before = test_repo.get_entity_state_and_history("ord-202").current_state
    assert state_ord_201_before.state["O_TOTALPRICE"] == 120.0
    assert state_ord_202_before.state["O_TOTALPRICE"] == 200.0

    # Simulate Offset 0 Replay: Re-apply stream into current_state
    test_repo.upsert_current_state(e1)
    test_repo.upsert_current_state(e2)
    test_repo.upsert_current_state(e3)

    state_ord_201_after = test_repo.get_entity_state_and_history("ord-201").current_state
    state_ord_202_after = test_repo.get_entity_state_and_history("ord-202").current_state

    # Assert exact row count and column value convergence
    assert state_ord_201_after.entity_key == state_ord_201_before.entity_key
    assert state_ord_201_after.latest_event_id == state_ord_201_before.latest_event_id
    assert state_ord_201_after.state == state_ord_201_before.state
    assert state_ord_201_after.last_occurred_at == state_ord_201_before.last_occurred_at

    assert state_ord_202_after.entity_key == state_ord_202_before.entity_key
    assert state_ord_202_after.latest_event_id == state_ord_202_before.latest_event_id
    assert state_ord_202_after.state == state_ord_202_before.state

    # Verify older event does NOT overwrite newer state (out-of-order safety)
    older_event = _make_envelope("ord-201", "update", 999.0, occurred_at=base_time - timedelta(hours=1))
    test_repo.upsert_current_state(older_event)

    state_ord_201_protected = test_repo.get_entity_state_and_history("ord-201").current_state
    assert state_ord_201_protected.state["O_TOTALPRICE"] == 120.0  # Kept newer state!


def test_ac_c03_negative_price_quarantined_in_dlq(test_repo: PostgresRepository) -> None:
    """AC-C03: Given a negative price event, record is quarantined in DLQ table and excluded from current_state."""
    bad_event = _make_envelope(entity_key="ord-bad-1", price=-45.50)
    result = test_repo.ingest_event(bad_event)

    assert result.status == "quarantined"
    assert result.error_code == "BUSINESS_RULE_VIOLATION"

    # Excluded from current_state
    assert test_repo.get_entity_state_and_history("ord-bad-1") is None

    # Excluded from event_log
    events = test_repo.get_events(entity_key="ord-bad-1")
    assert events.total == 0

    # Persisted in quarantined_events
    stats = test_repo.get_stats()
    assert stats.quarantined_count == 1


def test_dq1_schema_violation_quarantined(test_repo: PostgresRepository) -> None:
    """DQ-1: Malformed envelope (missing fields, invalid types) routed to quarantined_events."""
    malformed_event = {
        "event_id": "not-a-valid-uuid",
        "event_type": "invalid_operation",
        "source": "snowflake.orders",
        # missing entity_key, occurred_at, etc.
    }
    result = test_repo.ingest_event(malformed_event)

    assert result.status == "quarantined"
    assert result.error_code == "SCHEMA_VIOLATION"

    stats = test_repo.get_stats()
    assert stats.quarantined_count == 1


def test_dq3_duplicate_event_id_quarantined(test_repo: PostgresRepository) -> None:
    """DQ-3: Duplicate event ID detection routed to quarantined_events with no silent drops."""
    envelope = _make_envelope(entity_key="ord-dup-1", price=50.0)

    # First ingestion succeeds
    res1 = test_repo.ingest_event(envelope)
    assert res1.status == "processed"

    # Duplicate ingestion with same event_id
    res2 = test_repo.ingest_event(envelope)
    assert res2.status == "quarantined"
    assert res2.error_code == "DUPLICATE_EVENT_ID"

    # current_state still valid and intact
    state = test_repo.get_entity_state_and_history("ord-dup-1")
    assert state is not None
    assert len(state.history) == 1  # event_log has only the 1 original event


def test_req_c04_fastapi_queries(test_repo: PostgresRepository) -> None:
    """REQ-C04 / AC-C04: Verify queries backing /events, /entities/{key}, and /stats."""
    t0 = datetime(2026, 10, 12, 12, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=10)
    t2 = t0 + timedelta(minutes=20)

    test_repo.ingest_event(_make_envelope("ord-501", "insert", 10.0, occurred_at=t0))
    test_repo.ingest_event(_make_envelope("ord-502", "insert", 20.0, occurred_at=t1))
    test_repo.ingest_event(_make_envelope("ord-501", "update", 15.0, occurred_at=t2))

    # 1. GET /events filtering by entity_key
    ev_501 = test_repo.get_events(entity_key="ord-501")
    assert ev_501.total == 2
    assert len(ev_501.events) == 2

    # 2. GET /events filtering by event_type
    ev_updates = test_repo.get_events(event_type="update")
    assert ev_updates.total == 1
    assert ev_updates.events[0].entity_key == "ord-501"

    # 3. GET /events pagination
    page1 = test_repo.get_events(limit=1, offset=0)
    assert page1.total == 3
    assert len(page1.events) == 1

    # 4. GET /entities/{key}
    entity_info = test_repo.get_entity_state_and_history("ord-501")
    assert entity_info is not None
    assert entity_info.current_state.state["O_TOTALPRICE"] == 15.0
    assert len(entity_info.history) == 2
    # Verify chronological order (t0 before t2)
    assert str(entity_info.history[0].occurred_at) <= str(entity_info.history[1].occurred_at)

    # 5. Non-existent entity
    assert test_repo.get_entity_state_and_history("ord-999") is None

    # 6. GET /stats
    stats = test_repo.get_stats()
    assert stats.total_events == 3
    assert stats.counts_by_type == {"insert": 2, "update": 1}
    assert stats.unique_entities_count == 2
    assert stats.quarantined_count == 0
    assert stats.last_watermark is not None
    assert stats.lag.avg_lag_seconds >= 0.0


def test_init_sql_ddl_and_settings() -> None:
    """Verify init.sql exists, defines all required tables and indexes, and PostgresSettings parses."""
    from services.postgres.config import PostgresSettings
    settings = PostgresSettings(
        POSTGRES_HOST="db.internal",
        POSTGRES_PORT=5433,
        POSTGRES_DB="test_db",
        POSTGRES_USER="test_user",
        POSTGRES_PASSWORD="secret_password",
    )
    assert "postgresql://test_user:secret_password@db.internal:5433/test_db" == settings.connection_uri

    ddl_path = Path(__file__).resolve().parent.parent / "init.sql"
    assert ddl_path.exists()
    content = ddl_path.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS event_log" in content
    assert "CREATE TABLE IF NOT EXISTS current_state" in content
    assert "CREATE TABLE IF NOT EXISTS quarantined_events" in content
    assert "idx_event_log_event_id" in content
    assert "idx_current_state_last_occurred" in content
    assert "idx_quarantined_error_code" in content


def test_postgres_db_adapts_dict_params_to_jsonb() -> None:
    """Verify PostgresDB._format_params adapts dict parameters to Jsonb for JSONB columns."""
    from services.postgres.db import PostgresDB
    from psycopg.types.json import Jsonb

    db = PostgresDB()
    test_params = ("order-123", {"O_ORDERKEY": 123, "O_TOTALPRICE": 500.0}, "2026-10-12T00:00:00Z")
    formatted = db._format_params(test_params)
    assert formatted is not None
    assert formatted[0] == "order-123"
    assert isinstance(formatted[1], Jsonb)
    assert formatted[1].obj == {"O_ORDERKEY": 123, "O_TOTALPRICE": 500.0}
    assert formatted[2] == "2026-10-12T00:00:00Z"
