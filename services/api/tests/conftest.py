"""Pytest fixtures for API testing."""

from __future__ import annotations

import json
from typing import Generator
import pytest
from starlette.testclient import TestClient

from services.api.api.app import create_app
from services.api.api.db.repository import APIRepository
from services.api.api.dependencies import set_repository_override
from services.postgres.db import SQLiteTestDB


@pytest.fixture
def seeded_db() -> SQLiteTestDB:
    """Create an in-memory SQLite database seeded with deterministic test data."""
    db = SQLiteTestDB(":memory:")

    # Seed event_log table
    events = [
        (
            "3fa85f64-5717-4562-b3fc-2c963f66afa1",
            "insert",
            "snowflake.orders",
            "ord-101",
            "ord-101",
            json.dumps({"O_ORDERKEY": "ord-101", "O_TOTALPRICE": 150.0}),
            "2026-10-12T10:00:00Z",
            "2026-10-12T10:00:02Z",
            "2026-10-12T10:00:04Z",
            "1.0.0",
        ),
        (
            "3fa85f64-5717-4562-b3fc-2c963f66afa2",
            "update",
            "snowflake.orders",
            "ord-101",
            "ord-101",
            json.dumps({"O_ORDERKEY": "ord-101", "O_TOTALPRICE": 175.5}),
            "2026-10-12T10:10:00Z",
            "2026-10-12T10:10:02Z",
            "2026-10-12T10:10:05Z",
            "1.0.0",
        ),
        (
            "3fa85f64-5717-4562-b3fc-2c963f66afa3",
            "insert",
            "snowflake.orders",
            "ord-102",
            "ord-102",
            json.dumps({"O_ORDERKEY": "ord-102", "O_TOTALPRICE": 220.0}),
            "2026-10-12T10:20:00Z",
            "2026-10-12T10:20:02Z",
            "2026-10-12T10:20:06Z",
            "1.0.0",
        ),
    ]

    for ev in events:
        db.execute(
            """
            INSERT INTO event_log (
                event_id, event_type, source, entity_key, partition_key,
                payload, occurred_at, captured_at, processed_at, schema_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            ev,
        )

    # Seed current_state table
    states = [
        (
            "ord-101",
            "3fa85f64-5717-4562-b3fc-2c963f66afa2",
            "update",
            json.dumps({"O_TOTALPRICE": 175.5}),
            "2026-10-12T10:10:00Z",
            "2026-10-12T10:10:05Z",
        ),
        (
            "ord-102",
            "3fa85f64-5717-4562-b3fc-2c963f66afa3",
            "insert",
            json.dumps({"O_TOTALPRICE": 220.0}),
            "2026-10-12T10:20:00Z",
            "2026-10-12T10:20:06Z",
        ),
    ]

    for st in states:
        db.execute(
            """
            INSERT INTO current_state (
                entity_key, latest_event_id, event_type, state, last_occurred_at, last_updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            st,
        )

    # Seed quarantined_events table
    db.execute(
        """
        INSERT INTO quarantined_events (
            event_id, entity_key, error_code, error_message, raw_payload, quarantined_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            "3fa85f64-5717-4562-b3fc-2c963f66af99",
            "ord-999",
            "DQ2_NEGATIVE_PRICE",
            "Total price must be positive",
            json.dumps({"O_TOTALPRICE": -50.0}),
            "2026-10-12T10:30:00Z",
        ),
    )

    return db


@pytest.fixture
def test_repo(seeded_db: SQLiteTestDB) -> APIRepository:
    """Create APIRepository attached to the seeded test database."""
    return APIRepository(db=seeded_db)


@pytest.fixture
def client(test_repo: APIRepository) -> Generator[TestClient, None, None]:
    """TestClient configured with overridden repository."""
    set_repository_override(test_repo)
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    set_repository_override(None)
