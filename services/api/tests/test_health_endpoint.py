"""Integration tests for the GET /healthz endpoint."""

from __future__ import annotations

from unittest.mock import MagicMock
from starlette.testclient import TestClient

from services.api.api.app import create_app
from services.api.api.db.repository import APIRepository
from services.api.api.dependencies import set_repository_override


def test_healthz_healthy(client: TestClient) -> None:
    """Verify health endpoint returns 200 when database responds."""
    response = client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"


def test_healthz_degraded() -> None:
    """Verify health endpoint returns 503 when database is unavailable."""
    failing_db = MagicMock()
    failing_db.fetchone.side_effect = Exception("DB Connection refused")

    repo = APIRepository(db=failing_db)
    set_repository_override(repo)

    app = create_app()
    with TestClient(app) as test_client:
        response = test_client.get("/healthz")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["database"] == "disconnected"

    set_repository_override(None)
