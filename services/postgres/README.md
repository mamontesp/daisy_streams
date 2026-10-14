# PostgreSQL Storage & Sink Layer (Part C)

Implements the PostgreSQL storage, idempotency, data quality quarantine, and query engine for **Part C** (`spec-tool-consumer-and-api.md`).

---

## 1. Schema Topology

The database schema (`services/postgres/init.sql`) defines three core tables:

### `event_log` (Append-Only Raw Ledger — REQ-C01)
Stores all raw, validated incoming stream events immutably with ingestion timestamps and metadata.
- **Columns**: `id`, `event_id` (UUID), `event_type` (`insert` | `update`), `source`, `entity_key`, `partition_key`, `payload` (JSONB), `occurred_at`, `captured_at`, `processed_at`, `schema_version`.
- **Key Index**: `(entity_key, occurred_at ASC, id ASC)` enabling instant retrieval of an entity's chronological history.

### `current_state` (Materialized State — REQ-C01, REQ-C02)
Maintains the latest state per `entity_key`.
- **Columns**: `entity_key` (PK), `latest_event_id` (UUID), `event_type`, `state` (JSONB), `last_occurred_at`, `last_updated_at`.
- **Idempotent Monotonic Upsert**:
  ```sql
  INSERT INTO current_state (
      entity_key, latest_event_id, event_type, state, last_occurred_at, last_updated_at
  ) VALUES (%s, %s, %s, %s, %s, %s)
  ON CONFLICT (entity_key) DO UPDATE SET
      latest_event_id = EXCLUDED.latest_event_id,
      event_type = EXCLUDED.event_type,
      state = EXCLUDED.state,
      last_occurred_at = EXCLUDED.last_occurred_at,
      last_updated_at = EXCLUDED.last_updated_at
  WHERE EXCLUDED.last_occurred_at >= current_state.last_occurred_at;
  ```
  This clause ensures that replaying the stream from offset 0—or handling out-of-order network retries—converges to the exact same state without regression.

### `quarantined_events` (Dead-Letter Queue / DLQ — REQ-C03)
Captures rejected records without silent dropping:
- **Columns**: `id`, `event_id`, `entity_key`, `error_code`, `error_message`, `raw_payload` (JSONB), `quarantined_at`.
- **Codes**: `SCHEMA_VIOLATION`, `BUSINESS_RULE_VIOLATION`, `DUPLICATE_EVENT_ID`.

---

## 2. Data Quality (DQ) Enforcement

Conforms strictly to **REQ-C03**:
1. **DQ-1 (Envelope Schema Conformance)**: Verifies presence of required fields, valid UUIDv4, valid operation types, and timestamp parsing.
2. **DQ-2 (Business Logic Validity)**: Enforces that order total prices/amounts (`O_TOTALPRICE`) are strictly positive (`> 0`) and non-null. Negative prices are immediately routed to `quarantined_events` and omitted from `current_state`.
3. **DQ-3 (Duplicate Event ID Detection)**: Checks whether `event_id` already exists in `event_log`. Duplicates are quarantined to preserve event log integrity and prevent double-processing.

---

## 3. Query Contracts (REQ-C04)

`PostgresRepository` provides optimized methods for the inspection API:
- `get_events(...)`: Filtered, paginated queries on `entity_key`, `event_type`, and time ranges.
- `get_entity_state_and_history(key)`: Returns current materialized state plus full chronological change history.
- `get_stats()`: Computes event counts by type, lag metrics (`processed_at - occurred_at`), total quarantined records, and the latest watermark.

---

## 4. Modular Pydantic Models (`services/postgres/models/`)

Event processing and storage models:
- `bronze/`:
  - `raw_event_log.py`: `RawEventLog` (`bronze.raw_event_log` table row).
  - `raw_orders_log.py`: `RawOrdersLog` (`bronze.raw_orders_log` structured table row).
  - `quarantined_event_log.py`: `QuarantinedEventLog` (`bronze.quarantined_event_log` DLQ row).
- `silver/`:
  - `stg_current_orders.py`: `StgCurrentOrders` (`silver.stg_current_orders` state table row).
- `event_envelope.py`: `EventEnvelope` (Kafka event ingestion wrapper).
- `order_payload.py`: `OrderPayload` (Snowflake payload validation and positive price check).
- `enums.py`: `ErrorCode` (`SCHEMA_VIOLATION`, `BUSINESS_RULE_VIOLATION`, `DUPLICATE_EVENT_ID`).
- `ingestion_result.py`: `IngestionResult` (atomic ingestion response).

---

## 5. Running Tests

```bash
# Run PostgreSQL storage tests
make test-postgres

# Or via pytest directly
pytest services/postgres/tests -v
```
