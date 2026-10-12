"""Unit tests for change simulation script (REQ-A06)."""

from __future__ import annotations

from datetime import datetime, timezone
from snowflake_adapter.client import MockSnowflakeClient
from scripts.simulate_changes import (
    execute_simulation,
    generate_simulated_inserts,
    generate_simulated_updates,
)


def test_generate_simulated_inserts() -> None:
    """Verify insert generator produces valid records with correct fields and types."""
    now = datetime(2026, 10, 12, 12, 0, 0, tzinfo=timezone.utc)
    inserts = generate_simulated_inserts(start_key=50000, count=5, now=now)

    assert len(inserts) == 5
    for i, row in enumerate(inserts):
        assert row["O_ORDERKEY"] == 50000 + i
        assert row["O_ORDERDATE"] == "2026-10-12"
        assert row["EVENT_TYPE"] == "insert"
        assert isinstance(row["O_TOTALPRICE"], float)


def test_generate_simulated_updates() -> None:
    """Verify update generator produces valid modifications."""
    now = datetime(2026, 10, 12, 12, 0, 0, tzinfo=timezone.utc)
    updates = generate_simulated_updates(target_keys=[1001, 1002, 1003], now=now)

    assert len(updates) == 3
    assert updates[0]["O_ORDERKEY"] == 1001
    assert updates[0]["EVENT_TYPE"] == "update"
    assert updates[0]["O_ORDERSTATUS"] == "F"


def test_execute_simulation_dry_run() -> None:
    """Verify dry-run mode returns receipt without executing statements."""
    receipt = execute_simulation(
        client=None,
        table="ORDERS",
        database="TEST_DB",
        schema="TEST_SCHEMA",
        num_inserts=3,
        num_updates=2,
        dry_run=True,
    )

    assert receipt["dry_run"] is True
    assert receipt["inserts"]["count"] == 3
    assert receipt["updates"]["count"] == 2
    assert len(receipt["inserts"]["keys"]) == 3
    assert len(receipt["updates"]["keys"]) == 2


def test_execute_simulation_with_mock_client() -> None:
    """Verify simulation executes INSERT and UPDATE queries on client."""
    mock_client = MockSnowflakeClient()
    receipt = execute_simulation(
        client=mock_client,
        table="ORDERS",
        database="TEST_DB",
        schema="TEST_SCHEMA",
        num_inserts=4,
        num_updates=3,
        dry_run=False,
    )

    assert receipt["dry_run"] is False
    assert receipt["inserts"]["count"] == 4
    assert receipt["updates"]["count"] == 3
    assert len(mock_client.statements_executed) == 7
