# Daisy Streams — FastAPI Event-Inspection Service (Part C)

A high-performance inspection and query API built with **FastAPI**, serving paginated access to the PostgreSQL raw event ledger (`event_log`) and real-time stream processing metrics (`GET /stats`), complying with **REQ-C04** and **AC-C04**.

---

## Model Decoupling & Architecture

This service follows strict domain decoupling principles:
- **Independent API Models**: All request parameters, response DTOs, and error envelopes reside exclusively within `services/api/api/models/`.
- **Zero Cross-Service Model Imports**: The API contains zero imports from `services.postgres.models`, `services.consumer`, or `services.snowflake_adapter`.
- **Verified via Automated AST Isolation Tests**: A dedicated test suite (`tests/test_models_isolation.py`) inspects all module imports to guarantee no dependency leakage.

---

## Endpoints

### 1. `GET /events`
Query paginated stream events with optional entity, type, and temporal filters.

- **Query Parameters**:
  - `entity_key` (*optional*): Target entity key (e.g., `order-101`).
  - `type` / `event_type` (*optional*): Action type (`insert`, `update`).
  - `start` / `start_time` (*optional*): ISO 8601 timestamp lower bound (`occurred_at >= start`).
  - `end` / `end_time` (*optional*): ISO 8601 timestamp upper bound (`occurred_at <= end`).
  - `limit` (*default: 50, max: 1000*): Number of records per page.
  - `offset` (*default: 0*): Offset of records to skip.

```bash
# Fetch recent events
curl -s "http://localhost:8000/events?limit=10" | jq

# Filter by entity key and event type
curl -s "http://localhost:8000/events?entity_key=ord-101&type=insert" | jq

# Filter by time range
curl -s "http://localhost:8000/events?start=2026-10-12T00:00:00Z&end=2026-10-14T23:59:59Z" | jq
```

### 2. `GET /stats`
Return real-time stream aggregation counts, processing lag, and watermark timestamps.

```bash
curl -s "http://localhost:8000/stats" | jq
```

**Example Response**:
```json
{
  "total_events": 25000,
  "counts_by_type": {
    "insert": 20000,
    "update": 5000
  },
  "quarantined_count": 3,
  "unique_entities_count": 18200,
  "lag": {
    "avg_lag_seconds": 1.152,
    "max_lag_seconds": 6.841
  },
  "last_watermark": "2026-10-14T12:00:00Z",
  "last_processed_at": "2026-10-14T12:00:02Z"
}
```

### 3. `GET /healthz`
Liveness and database connectivity health probe.

```bash
curl -s "http://localhost:8000/healthz" | jq
```

---

## Interactive Documentation

- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## Running Locally

```bash
# Run using Python script
python services/api/scripts/run_api.py --reload

# Or run via Makefile
make api

# Run test suite
pytest services/api/tests -v
```
