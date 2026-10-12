"""Unit tests for EventEnvelope and SyncState data models."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4
import pytest
from pydantic import ValidationError

from snowflake_adapter.models.envelope import EventEnvelope
from snowflake_adapter.models.state import SyncState


def test_event_envelope_valid_creation() -> None:
    """Verify EventEnvelope validates and sets correct defaults per REQ-B03."""
    now = datetime.now(timezone.utc)
    envelope = EventEnvelope(
        event_type="insert",
        source="snowflake.tpch.orders",
        entity_key="1001",
        payload={"order_id": 1001, "total_price": 250.0},
        occurred_at=now,
    )

    assert isinstance(envelope.event_id, UUID)
    assert envelope.event_type == "insert"
    assert envelope.source == "snowflake.tpch.orders"
    assert envelope.entity_key == "1001"
    assert envelope.payload == {"order_id": 1001, "total_price": 250.0}
    assert envelope.occurred_at == now
    assert isinstance(envelope.captured_at, datetime)
    assert envelope.schema_version == "1.0.0"


def test_event_envelope_invalid_type_raises_error() -> None:
    """Verify EventEnvelope rejects invalid event_type values."""
    with pytest.raises(ValidationError):
        EventEnvelope(
            event_type="delete",  # type: ignore[arg-type]
            source="snowflake.tpch.orders",
            entity_key="1001",
            payload={},
            occurred_at=datetime.now(timezone.utc),
        )


def test_event_envelope_serialization() -> None:
    """Verify EventEnvelope serializes to JSON string."""
    envelope = EventEnvelope(
        event_type="update",
        source="snowflake.tpch.orders",
        entity_key="1002",
        payload={"order_id": 1002, "status": "F"},
        occurred_at=datetime(2026, 10, 12, 12, 0, 0, tzinfo=timezone.utc),
    )
    raw = envelope.model_dump_json()
    parsed = json.loads(raw)

    assert parsed["event_type"] == "update"
    assert parsed["entity_key"] == "1002"
    assert parsed["schema_version"] == "1.0.0"
    assert "event_id" in parsed
    assert "captured_at" in parsed


def test_sync_state_contract_compliance() -> None:
    """Verify SyncState adheres strictly to Section 3 State Store Contract."""
    batch_id = str(uuid4())
    state = SyncState(
        source_table="orders",
        last_watermark="2026-10-12T00:00:00Z",
        last_batch_id=batch_id,
        last_synced_at="2026-10-12T11:00:00Z",
        records_synced=25000,
    )

    contract_dict = state.to_contract_dict()
    assert set(contract_dict.keys()) == {
        "source_table",
        "last_watermark",
        "last_batch_id",
        "last_synced_at",
        "records_synced",
    }
    assert contract_dict["source_table"] == "orders"
    assert contract_dict["last_watermark"] == "2026-10-12T00:00:00Z"
    assert contract_dict["last_batch_id"] == batch_id
    assert contract_dict["records_synced"] == 25000


def test_sync_state_serialization_roundtrip() -> None:
    """Verify SyncState JSON serialization and deserialization."""
    state = SyncState(
        source_table="orders",
        last_watermark="1995-03-15T00:00:00Z",
        last_batch_id=str(uuid4()),
        last_synced_at="2026-10-12T12:00:00Z",
        records_synced=10000,
        last_entity_key="9999",
    )
    json_str = state.to_json()
    recovered = SyncState.from_json(json_str)

    assert recovered.source_table == state.source_table
    assert recovered.last_watermark == state.last_watermark
    assert recovered.last_batch_id == state.last_batch_id
    assert recovered.records_synced == state.records_synced
    assert recovered.last_entity_key == state.last_entity_key
