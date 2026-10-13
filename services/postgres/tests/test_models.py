"""Unit tests for PostgreSQL Pydantic models."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4
import pytest
from pydantic import ValidationError

from services.postgres.models import (
    EntityHistoryResponse,
    ErrorCode,
    EventsQueryResponse,
    IngestionResult,
    LagMetrics,
    OrderPayload,
    OrdersAppendLog,
    OrdersCurrentState,
    QuarantinedEventRecord,
    StatsResponse,
)


def test_order_payload_valid_and_aliasing() -> None:
    """Verify OrderPayload parses both Snowflake uppercase aliases and lowercase names."""
    # Snowflake TPCH format
    data = {
        "O_ORDERKEY": 1001,
        "O_CUSTKEY": 50,
        "O_TOTALPRICE": 199.50,
        "O_ORDERSTATUS": "O",
        "O_ORDERDATE": "2026-10-12",
        "CUSTOM_ATTR": "extra_value",
    }
    order = OrderPayload.model_validate(data)
    assert order.order_key == 1001
    assert order.cust_key == 50
    assert order.total_price == 199.50
    assert order.order_status == "O"

    # Lowercase format
    data_lower = {"total_price": 50.25, "order_key": "ord-2"}
    order_lower = OrderPayload.model_validate(data_lower)
    assert order_lower.total_price == 50.25
    assert order_lower.order_key == "ord-2"


def test_order_payload_negative_or_zero_price_fails() -> None:
    """Verify OrderPayload strictly rejects prices <= 0."""
    with pytest.raises(ValidationError) as exc:
        OrderPayload.model_validate({"O_TOTALPRICE": -10.0})
    assert "strictly positive" in str(exc.value)

    with pytest.raises(ValidationError) as exc2:
        OrderPayload.model_validate({"O_TOTALPRICE": 0.0})
    assert "strictly positive" in str(exc2.value)


def test_orders_append_log_model() -> None:
    """Verify OrdersAppendLog validates fields and serialization."""
    now = datetime.now(timezone.utc)
    ev_id = uuid4()
    record = OrdersAppendLog(
        id=1,
        event_id=ev_id,
        event_type="insert",
        source="snowflake.orders",
        entity_key="ord-1",
        payload={"price": 100.0},
        occurred_at=now,
        captured_at=now,
    )
    assert record.event_id == ev_id
    assert record.entity_key == "ord-1"
    assert record.payload["price"] == 100.0


def test_orders_current_state_model() -> None:
    """Verify OrdersCurrentState validates fields and serialization."""
    now = datetime.now(timezone.utc)
    ev_id = uuid4()
    state_rec = OrdersCurrentState(
        entity_key="ord-1",
        latest_event_id=ev_id,
        event_type="insert",
        state={"price": 100.0},
        last_occurred_at=now,
    )
    assert state_rec.entity_key == "ord-1"
    assert state_rec.latest_event_id == ev_id
    assert state_rec.state["price"] == 100.0


def test_quarantined_event_record_and_error_codes() -> None:
    """Verify QuarantinedEventRecord parses ErrorCode enum."""
    record = QuarantinedEventRecord(
        error_code=ErrorCode.BUSINESS_RULE_VIOLATION,
        error_message="Price cannot be negative",
        raw_payload={"price": -5.0},
    )
    assert record.error_code == ErrorCode.BUSINESS_RULE_VIOLATION
    assert record.error_code.value == "BUSINESS_RULE_VIOLATION"


def test_stats_and_query_response_models() -> None:
    """Verify StatsResponse and EventsQueryResponse model serialization and attribute access."""
    stats = StatsResponse(
        total_events=10,
        counts_by_type={"insert": 7, "update": 3},
        quarantined_count=1,
        unique_entities_count=5,
        lag=LagMetrics(avg_lag_seconds=0.125, max_lag_seconds=0.5),
        last_watermark="2026-10-12T12:00:00Z",
    )
    # Typed attribute access
    assert stats.total_events == 10
    assert stats.lag.avg_lag_seconds == 0.125
    assert stats.lag.max_lag_seconds == 0.5
    assert stats.counts_by_type["insert"] == 7

    # JSON export
    json_dump = stats.model_dump_json()
    assert "avg_lag_seconds" in json_dump
    assert stats.model_dump()["total_events"] == 10
