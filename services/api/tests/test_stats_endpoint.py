"""Integration tests for the GET /stats endpoint (REQ-C04 & AC-C04)."""

from __future__ import annotations

from starlette.testclient import TestClient

from services.api.api.app import create_app
from services.api.api.db.repository import APIRepository
from services.api.api.dependencies import set_repository_override
from services.postgres.db import SQLiteTestDB


def test_get_stats_seeded_metrics(client: TestClient) -> None:
    """Verify stats computation aggregates counts, lag, and watermarks correctly."""
    response = client.get("/stats")
    assert response.status_code == 200

    data = response.json()
    assert data["total_events"] == 3
    assert data["counts_by_type"] == {"insert": 2, "update": 1}
    assert data["quarantined_count"] == 1
    assert data["unique_entities_count"] == 2

    # Lag metrics
    assert "lag" in data
    assert "avg_lag_seconds" in data["lag"]
    assert "max_lag_seconds" in data["lag"]
    assert data["lag"]["avg_lag_seconds"] >= 0.0
    assert data["lag"]["max_lag_seconds"] >= 0.0

    # High watermark timestamps
    assert data["last_watermark"] is not None
    assert data["last_processed_at"] is not None


def test_get_stats_empty_database() -> None:
    """Verify stats endpoint returns valid empty defaults when no events exist."""
    empty_db = SQLiteTestDB(":memory:")
    repo = APIRepository(db=empty_db)
    set_repository_override(repo)

    app = create_app()
    with TestClient(app) as test_client:
        response = test_client.get("/stats")
        assert response.status_code == 200
        data = response.json()

        assert data["total_events"] == 0
        assert data["counts_by_type"] == {}
        assert data["quarantined_count"] == 0
        assert data["unique_entities_count"] == 0
        assert data["lag"]["avg_lag_seconds"] == 0.0
        assert data["lag"]["max_lag_seconds"] == 0.0
        assert data["last_watermark"] is None
        assert data["last_processed_at"] is None

    set_repository_override(None)
