"""Integration tests for the GET /events endpoint (REQ-C04 & AC-C04)."""

from __future__ import annotations

from starlette.testclient import TestClient


def test_get_events_default_pagination(client: TestClient) -> None:
    """Verify default query returns all seeded events ordered newest first."""
    response = client.get("/events")
    assert response.status_code == 200

    data = response.json()
    assert data["total"] == 3
    assert data["limit"] == 50
    assert data["offset"] == 0
    assert len(data["events"]) == 3

    # Check structure of first event item
    first = data["events"][0]
    assert "event_id" in first
    assert "event_type" in first
    assert "source" in first
    assert "entity_key" in first
    assert "payload" in first
    assert "occurred_at" in first
    assert "captured_at" in first
    assert "processed_at" in first
    assert "schema_version" in first

    # Check ordering (newest first: ord-102 at 10:20:00Z)
    assert first["entity_key"] == "ord-102"
    assert first["payload"]["O_TOTALPRICE"] == 220.0


def test_get_events_filter_by_entity_key(client: TestClient) -> None:
    """Verify filtering by entity_key returns only matching records."""
    response = client.get("/events?entity_key=ord-101")
    assert response.status_code == 200

    data = response.json()
    assert data["total"] == 2
    assert len(data["events"]) == 2
    for ev in data["events"]:
        assert ev["entity_key"] == "ord-101"


def test_get_events_filter_by_type_and_alias(client: TestClient) -> None:
    """Verify filtering by 'type' and alias 'event_type' produces identical results."""
    # Using 'type'
    resp1 = client.get("/events?type=insert")
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert data1["total"] == 2
    for ev in data1["events"]:
        assert ev["event_type"] == "insert"

    # Using 'event_type' alias
    resp2 = client.get("/events?event_type=insert")
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["total"] == 2
    assert data2["events"] == data1["events"]

    # Filter for 'update'
    resp_update = client.get("/events?type=update")
    assert resp_update.status_code == 200
    data_update = resp_update.json()
    assert data_update["total"] == 1
    assert data_update["events"][0]["event_type"] == "update"


def test_get_events_filter_by_time_range(client: TestClient) -> None:
    """Verify filtering by start and end temporal boundaries."""
    # Filter for the event at 10:10:00Z
    response = client.get(
        "/events?start=2026-10-12T10:05:00Z&end=2026-10-12T10:15:00Z"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["events"]) == 1
    assert data["events"][0]["event_type"] == "update"
    assert data["events"][0]["entity_key"] == "ord-101"

    # Also test start_time and end_time aliases
    resp_alias = client.get(
        "/events?start_time=2026-10-12T10:05:00Z&end_time=2026-10-12T10:15:00Z"
    )
    assert resp_alias.status_code == 200
    assert resp_alias.json()["total"] == 1


def test_get_events_pagination(client: TestClient) -> None:
    """Verify limit and offset pagination controls."""
    response = client.get("/events?limit=1&offset=1")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["limit"] == 1
    assert data["offset"] == 1
    assert len(data["events"]) == 1

    # Second item in descending order (10:10:00Z)
    assert data["events"][0]["event_type"] == "update"


def test_get_events_invalid_parameters(client: TestClient) -> None:
    """Verify FastAPI automatic parameter validation rejects invalid bounds."""
    # Limit less than 1
    resp_bad_limit = client.get("/events?limit=0")
    assert resp_bad_limit.status_code == 422

    # Malformed datetime string
    resp_bad_date = client.get("/events?start=invalid-date")
    assert resp_bad_date.status_code == 422


def test_correlation_id_header(client: TestClient) -> None:
    """Verify correlation ID is set or preserved in response headers."""
    # Custom request ID
    custom_id = "req-custom-abc-123"
    resp = client.get("/events", headers={"X-Request-ID": custom_id})
    assert resp.status_code == 200
    assert resp.headers.get("X-Request-ID") == custom_id

    # Auto-generated request ID
    resp_auto = client.get("/events")
    assert resp_auto.status_code == 200
    assert "X-Request-ID" in resp_auto.headers
