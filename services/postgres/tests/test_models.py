"""Unit tests for PostgreSQL Pydantic models across Bronze and Silver schemas."""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4
import pytest
from pydantic import ValidationError

from services.postgres.models import (
    ErrorCode,
    EventEnvelope,
    IngestionResult,
    OrderPayload,
    OrdersAppendLog,
    OrdersCurrentState,
    QuarantinedEventLog,
    QuarantinedEventRecord,
    RawEventLog,
    RawOrderEvents,
    RawOrdersLog,
    StgCurrentOrders,
)
from services.postgres.models.bronze import (
    QuarantinedEventLog as BronzeQuarantinedEventLog,
    RawEventLog as BronzeRawEventLog,
    RawOrdersLog as BronzeRawOrdersLog,
)
from services.postgres.models.silver import StgCurrentOrders as SilverStgCurrentOrders


def test_bronze_and_silver_subpackage_imports() -> None:
    """Verify clean importing directly from bronze and silver modules."""
    assert BronzeRawEventLog is RawEventLog
    assert BronzeRawOrdersLog is RawOrdersLog
    assert BronzeQuarantinedEventLog is QuarantinedEventLog
    assert SilverStgCurrentOrders is StgCurrentOrders


def test_order_payload_valid_and_aliasing() -> None:
    """Verify OrderPayload parses both Snowflake uppercase aliases and lowercase names."""
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


def test_raw_event_log_model() -> None:
    """Verify RawEventLog validates fields, serialization, and legacy alias."""
    now = datetime.now(timezone.utc)
    ev_id = uuid4()
    record = RawEventLog(
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

    # Test legacy alias
    assert OrdersAppendLog is RawEventLog


def test_raw_orders_log_model() -> None:
    """Verify RawOrdersLog validates unpacked order fields and aliases."""
    now = datetime.now(timezone.utc)
    ev_id = uuid4()
    order_log = RawOrdersLog(
        log_id=1,
        event_id=ev_id,
        event_type="insert",
        source="snowflake.orders",
        entity_key="1001",
        order_key=1001,
        cust_key=50,
        order_status="O",
        total_price=250.75,
        order_date=date(2026, 10, 12),
        customer_name="Customer#000000050",
        occurred_at=now,
        captured_at=now,
        processed_at=now,
    )
    assert order_log.log_id == 1
    assert order_log.order_key == 1001
    assert order_log.cust_key == 50
    assert order_log.total_price == 250.75
    assert order_log.customer_name == "Customer#000000050"

    # Test alias
    assert RawOrderEvents is RawOrdersLog


def test_stg_current_orders_model() -> None:
    """Verify StgCurrentOrders validates structured fields and legacy alias."""
    now = datetime.now(timezone.utc)
    ev_id = uuid4()
    state_rec = StgCurrentOrders(
        entity_key="ord-1",
        order_key=1001,
        cust_key=50,
        order_status="O",
        total_price=150.00,
        order_date=date(2026, 10, 12),
        latest_event_id=ev_id,
        event_type="insert",
        state={"price": 150.0},
        last_occurred_at=now,
    )
    assert state_rec.entity_key == "ord-1"
    assert state_rec.order_key == 1001
    assert state_rec.total_price == 150.00
    assert state_rec.latest_event_id == ev_id

    # Test legacy alias
    assert OrdersCurrentState is StgCurrentOrders


def test_quarantined_event_log_and_error_codes() -> None:
    """Verify QuarantinedEventLog parses ErrorCode enum and legacy alias."""
    record = QuarantinedEventLog(
        error_code=ErrorCode.BUSINESS_RULE_VIOLATION,
        error_message="Price cannot be negative",
        raw_payload={"price": -5.0},
    )
    assert record.error_code == ErrorCode.BUSINESS_RULE_VIOLATION
    assert record.error_code.value == "BUSINESS_RULE_VIOLATION"

    # Test legacy alias
    assert QuarantinedEventRecord is QuarantinedEventLog


def test_event_envelope_and_ingestion_result_models() -> None:
    """Verify EventEnvelope and IngestionResult models for event processing."""
    now = datetime.now(timezone.utc)
    ev_id = uuid4()
    envelope = EventEnvelope(
        event_id=ev_id,
        event_type="insert",
        source="snowflake.orders",
        entity_key="ord-100",
        payload={"O_ORDERKEY": 100, "O_TOTALPRICE": 50.0},
        occurred_at=now,
    )
    assert envelope.event_id == ev_id
    assert envelope.entity_key == "ord-100"

    result = IngestionResult(
        status="processed",
        event_id=str(ev_id),
        entity_key="ord-100",
        message="Stored in bronze and silver",
    )
    assert result.status == "processed"
    assert result.event_id == str(ev_id)

