# Daisy Streams — End-to-End Streaming & Intelligence Platform

A production-grade, event-driven data streaming pipeline built to showcase portfolio capabilities in architecting, designing, and implementing an end-to-end data solution.

---

## Architecture Overview

```
                      +-------------------------------------------------+
                      |                 Snowflake Cloud                 |
                      | (TPCH_SF1.ORDERS + CUSTOMER or Mutable Orders)  |
                      +------------------------+------------------------+
                                               |
                                               | SQL (Bounded Slice / Incremental Delta)
                                               v
+-----------------------------------------------------------------------------------------------+
| Docker Service: `snowflake-adapter`                                                           |
| - High-watermark tracking & delta extraction                                                  |
| - Monotonic progression (O_ORDERDATE / UPDATED_AT)                                             |
| - Structured JSON correlation logging (batch_id, event_id)                                    |
| - Independent monitoring: `docker compose logs -f snowflake-adapter`                          |
+------------------------------+---------------------------------+------------------------------+
                               |                                 |
           Checkpoint State    |                                 | Publish EventEnvelope
           (S3 Object Put/Get) |                                 | (Keyed by entity_key)
                               v                                 v
+----------------------------------------------+  +---------------------------------------------+
| Docker Service: `garage` (dxflrs/garage)     |  | Docker Service: `kafka` (Apache Kafka)      |
| - S3 API: :3900, Admin API: :3903            |  | - Topic: `switch.orders.events`             |
| - Bucket: `daisy-state`                      |  | - Entity-key partitioning                   |
| - Object: `orders/state.json`                |  | - At-least-once delivery with backoff retry |
| - Volumes: `garage_meta`, `garage_data`      |  +----------------------+----------------------+
+----------------------------------------------+                         |
                                                                         v
                                                  +---------------------------------------------+
                                                  | Docker Service: `consumer`                  |
                                                  | - Idempotent materialization                |
                                                  | - Data quality quarantine (DLQ)             |
                                                  +----------------------+----------------------+
                                                                         |
                                                                         v
                                                  +---------------------------------------------+
                                                  | Docker Service: `postgres`                  |
                                                  | - `event_log` (Append-only)                 |
                                                  | - `current_state` (Upsert latest)           |
                                                  +----------------------+----------------------+
                                                                         |
                                                                         | SQL Queries
                                                                         v
                                                  +---------------------------------------------+
                                                  | Docker Service: `api` (FastAPI)             |
                                                  | - GET /events (Paginated event ledger)      |
                                                  | - GET /stats (Latency lag & stream metrics) |
                                                  | - Port :8000 (Swagger docs at /docs)        |
                                                  +---------------------------------------------+
```

---

## Architectural Defense: Garage S3 Object Storage State Backend

In compliance with **REQ-A05**, the sync watermark and batch metadata are persisted across process and container restarts. We selected **Garage (`dxflrs/garage`)**—a lightweight, distributed, S3-compatible object storage server written in Rust by Deuxfleurs—as our primary state persistence engine (with an atomic local file fallback for offline test suites):

1. **Lightweight & High Performance**: Built in Rust with an extraordinarily small memory and CPU footprint compared to heavy Java/Go object stores.
2. **Minimal Attack Surface (Distroless)**: The official `dxflrs/garage` image is built `FROM scratch` with no shell or extraneous packages, minimizing vulnerability exposure in production environments.
3. **Decoupling from the Sink**: Checkpointing state to Garage ensures that the ingestion layer remains strictly decoupled from consumption databases (such as PostgreSQL).
4. **Cloud-Native S3 Protocol**: Integrates via standard S3-compatible protocol (`boto3` path-style addressing on port 3900) storing atomic JSON checkpoints at `s3://daisy-state/{table}/state.json`.
5. **Crash Resilience**: State checkpoints survive process crashes, container re-creations, and host restarts via persistent Docker volumes (`garage_meta` and `garage_data`).

### State Store Contract

Every state checkpoint strictly adheres to the JSON contract:

```json
{
  "source_table": "orders",
  "last_watermark": "2026-10-12T00:00:00Z",
  "last_batch_id": "uuid-v4",
  "last_synced_at": "2026-10-12T11:00:00Z",
  "records_synced": 25000
}
```

---

## Quickstart

```bash
# 1. Clone repository and initialize environment variables
cp .env.example .env

# 2. Start all services (Garage, Kafka, Kafka UI, PostgreSQL, Snowflake Adapter, Consumer, and FastAPI API)
docker compose up -d
# or explicitly: docker compose up -d garage kafka kafka-ui postgres snowflake-adapter consumer api
# or: make up

# 3. Monitor services independently (and access Kafka UI at http://localhost:8080)
docker compose logs -f snowflake-adapter
docker compose logs -f consumer
docker compose logs -f api

# 4. Run change simulation to generate verifiable INSERTs and UPDATEs
python services/snowflake_adapter/scripts/simulate_changes.py --dry-run

# 5. Run Snowflake adapter in offline mock verification mode
python services/snowflake_adapter/scripts/run_adapter.py --mock

# 6. Query the FastAPI Event-Inspection API (http://localhost:8000)
# Check API liveness & PostgreSQL database connectivity
curl -s "http://localhost:8000/healthz" | jq

# Fetch paginated raw events from the append-only ledger
curl -s "http://localhost:8000/events?limit=10" | jq

# Filter events by entity key and event action type
curl -s "http://localhost:8000/events?entity_key=101&type=insert" | jq

# Retrieve real-time stream aggregation metrics, processing lag, and watermarks
curl -s "http://localhost:8000/stats" | jq

# 7. (Optional) Run the FastAPI inspection service locally with auto-reload
python services/api/scripts/run_api.py --reload
# or: make api

# 8. Run automated pytest test suites
pytest services/snowflake_adapter/tests -v
pytest services/api/tests -v
# or run the full platform test suite:
make test
```

### Event-Inspection API Reference & Documentation

The FastAPI inspection service runs on port `8000` (configurable via `API_PORT`):

| Endpoint | Method | Description | Example |
| :--- | :---: | :--- | :--- |
| `/healthz` | `GET` | Health check probe & PostgreSQL connectivity status | `curl -s http://localhost:8000/healthz \| jq` |
| `/events` | `GET` | Paginated raw event ledger with filtering (`entity_key`, `type`/`event_type`, `start`/`start_time`, `end`/`end_time`, `limit`, `offset`) | `curl -s "http://localhost:8000/events?limit=10" \| jq` |
| `/stats` | `GET` | Aggregated stream metrics: event counts by type, pipeline latency lag (`avg_lag_seconds`, `max_lag_seconds`), DLQ quarantine count, unique entity count, and watermark | `curl -s http://localhost:8000/stats \| jq` |
| `/docs` | `GET` | Interactive Swagger UI API documentation & OpenAPI schema | [http://localhost:8000/docs](http://localhost:8000/docs) |
| `/redoc` | `GET` | Interactive ReDoc OpenAPI documentation | [http://localhost:8000/redoc](http://localhost:8000/redoc) |

---

## Independent Docker Service Monitoring & Garage Operations

All services—including **Garage**, the **Snowflake Adapter**, the **Consumer**, and the **FastAPI Inspection API**—run as separate container services in `docker-compose.yml`, enabling independent lifecycle control, resource allocation, and log monitoring:

```bash
# Follow live adapter ingestion logs (with JSON correlation IDs)
docker compose logs -f snowflake-adapter

# Follow live consumer streaming logs
docker compose logs -f consumer

# Follow live FastAPI inspection API logs
docker compose logs -f api

# Follow live Garage S3 logs
docker compose logs -f garage

# Follow live Kafka UI logs
docker compose logs -f kafka-ui

# Access Kafka UI in browser:
# http://localhost:8080

# Access FastAPI Swagger UI in browser:
# http://localhost:8000/docs

# Check Garage cluster status directly via entrypoint binary
docker exec daisy-garage /garage status

# List buckets and access keys in Garage
docker exec daisy-garage /garage bucket list
docker exec daisy-garage /garage key list

# Restart adapter independently (verifies AC-A04 crash recovery)
docker compose restart snowflake-adapter

# Restart API service independently
docker compose restart api
```

---

## Structured Logging & Correlation Traceability

Daisy Streams implements production-grade structured JSON logging and end-to-end distributed correlation tracking across all pipeline components, satisfying **Part D — Engineering Hygiene**:

```
+---------------------------------------------------------------------------------------------------------+
|                                    End-to-End Correlation Pipeline                                      |
+---------------------------------------------------------------------------------------------------------+
|                                                                                                         |
|  [Snowflake Adapter]                                                                                    |
|    - Generates batch_id (UUIDv4) per sync cycle                                                         |
|    - Emits structured JSON logs to stdout & buffers for S3                                               |
|    - Packages EventEnvelope with batch_id & event_id                                                    |
|                                     |                                                                   |
|                                     v Kafka Headers: [("batch_id", ...), ("event_id", ...)]             |
|                                       Kafka Payload: {"batch_id": "...", "event_id": "...", ...}        |
|                                                                                                         |
|  [Kafka Streaming Broker]                                                                               |
|    - Transports partition-keyed messages (O_CUSTKEY) with correlation headers                           |
|                                     |                                                                   |
|                                     v                                                                   |
|                                                                                                         |
|  [Consumer Service]                                                                                     |
|    - Extracts batch_id & event_id from payload / headers                                                |
|    - ContextVars (correlation_batch_id, correlation_event_id) inject IDs into all consumer logs         |
|    - Scoped try/finally lifecycle resets ContextVars to prevent leakage across messages                 |
|                                     |                                                                   |
|                  +------------------+------------------+                                                |
|                  |                                     |                                                |
|                  v                                     v                                                |
|       [PostgreSQL Sink]                      [Garage S3 Log Storage]                                    |
|       - event_log.batch_id                   - Bucket: `daisy-logs`                                     |
|       - event_log.event_id                   - JSON Lines (.jsonl) archives                             |
|       - current_state.latest_event_id        - Partitioned: `service/year=Y/month=M/day=D/batch_*.jsonl`|
|                                                                                                         |
+---------------------------------------------------------------------------------------------------------+
```

### How the Logging System Works

1. **Structured JSON Format**: Every Python service (`snowflake_adapter`, `consumer`, `api`) formats logs as single-line JSON objects containing `timestamp` (UTC ISO 8601), `level`, `logger`, `message`, `batch_id`, `event_id`, and stack traces.
2. **Dual-Destination Architecture**:
   - **Console (`sys.stdout`)**: Real-time streaming for developer visibility via `docker compose logs -f <service>` or local CLIs.
   - **Persistent Object Storage (`Garage S3`)**: Handled by the shared [`services.common.s3_log_handler.S3BufferedLogHandler`](file:///Users/andreamontes_p/Documents/goswitch/daisy_streams/services/common/s3_log_handler.py). Logs are buffered in memory and flushed as JSON Lines (`.jsonl`) files into the `daisy-logs` bucket in Garage S3.
3. **Flushing & Archival Strategy**:
   - **Adapter**: Flushes sync execution logs atomically per batch: `snowflake-adapter/year=YYYY/month=MM/day=DD/batch_{batch_id}_{timestamp}.jsonl`.
   - **Consumer & API**: Flushes on time interval (e.g., 30s), buffer threshold (e.g., 50 records), or graceful container shutdown (`SIGTERM`/`SIGINT`).
4. **Resilience & Zero-Crash Guarantee**: The S3 log handler is non-blocking. If Garage is temporarily restarting, upload warnings are routed to stderr without disrupting or crashing the message ingestion pipeline. Bucket creation is self-healing (`_ensure_bucket_exists`).

### Tracing a Batch Through the System

You can trace any synchronization cycle end-to-end across the entire stack:
1. **Find `batch_id` in Adapter Logs or S3**:
   ```bash
   docker compose logs snowflake-adapter | grep "batch_id"
   # or list archived batch logs in Garage:
   # s3://daisy-logs/snowflake-adapter/year=2026/month=10/day=14/batch_<batch_id>_*.jsonl
   ```
2. **Trace `batch_id` in Consumer Logs**:
   ```bash
   docker compose logs consumer | grep "<batch_id>"
   ```
3. **Audit in PostgreSQL**:
   ```sql
   SELECT event_id, entity_key, event_type, occurred_at, processed_at
   FROM event_log
   WHERE batch_id = '<your-batch-id>'
   ORDER BY id ASC;
   ```

---

## Spec-Driven Development (SDD) Compliance

This project strictly adheres to **Spec-Driven Development (SDD)**:

- [**`spec/spec-architecture-pipeline.md`**](./spec/spec-architecture-pipeline.md) — Master pipeline specification.
- [**`spec/spec-data-snowflake-adapter.md`**](./spec/spec-data-snowflake-adapter.md) — Part A Snowflake adapter, watermark sync, Garage S3 state, and Docker service isolation.
- [**`spec/spec-infrastructure-streaming-layer.md`**](./spec/spec-infrastructure-streaming-layer.md) — Part B Broker topology, partitioning, and Pydantic envelopes.
- [**`spec/spec-tool-consumer-and-api.md`**](./spec/spec-tool-consumer-and-api.md) — Part C Dual-table materialization, replay, and inspection API.
- [**`spec/spec-process-engineering-hygiene.md`**](./spec/spec-process-engineering-hygiene.md) — Part D Engineering hygiene, testing, and operations.

### Part A Acceptance Criteria Verification

| Criterion | Description | Status | Verification |
| :--- | :--- | :---: | :--- |
| **AC-A01** | Bounded initial ingestion (10k–50k rows) and records watermark | Passed | `services/snowflake_adapter/tests/test_snowflake_adapter.py::test_ac_a01_initial_bounded_sync` |
| **AC-A02** | Re-run on unmodified source emits 0 events | Passed | `services/snowflake_adapter/tests/test_snowflake_adapter.py::test_ac_a02_unmodified_source_emits_zero_events` |
| **AC-A03** | Change simulation updates captured as delta events | Passed | `services/snowflake_adapter/tests/test_snowflake_adapter.py::test_ac_a03_change_simulation_delta_capture` |
| **AC-A04** | Process/container restart resumes strictly from Garage watermark | Passed | `services/snowflake_adapter/tests/test_snowflake_adapter.py::test_ac_a04_crash_restart_resumes_from_persisted_watermark` |
| **AC-A05** | Garage and Adapter run as isolated Docker services | Passed | Verified in `docker-compose.yml` with healthchecks & port mappings |
| **AC-A06** | Zero-password authentication: `key_pair` (prod) or `externalbrowser` (dev) | Passed | `services/snowflake_adapter/tests/test_client_auth.py` (9 unit tests) |

### Part C Acceptance Criteria Verification

| Criterion | Description | Status | Verification |
| :--- | :--- | :---: | :--- |
| **AC-C01** | Append-only raw ledger (`event_log`) and materialized upsert (`current_state`) populated | Passed | `services/postgres/tests/test_postgres_storage.py::test_ac_c01_valid_event_populates_both_tables` |
| **AC-C02** | Full topic replay from offset 0 produces idempotent state in `current_state` | Passed | `services/postgres/tests/test_postgres_storage.py::test_ac_c02_offset_zero_replay_idempotency` |
| **AC-C03** | Data quality violations quarantined in DLQ table without pipeline crash | Passed | `services/postgres/tests/test_postgres_storage.py::test_ac_c03_negative_price_quarantined_in_dlq` |
| **AC-C04** | All FastAPI inspection endpoints (`/events`, `/stats`, `/healthz`) serve verified responses | Passed | `services/api/tests/test_events_endpoint.py`, `services/api/tests/test_stats_endpoint.py` |

---

## Authentication Security: Zero-Password Architecture

The Snowflake Adapter strictly forbids plain database passwords, adhering to enterprise zero-trust standards:

- **Local Development**: `SNOWFLAKE_AUTHENTICATOR=externalbrowser` redirects to your identity provider (Okta, Azure AD, Ping) via browser SSO. No keys or secrets are stored in `.env`.
- **Production / Docker**: `SNOWFLAKE_AUTHENTICATOR=key_pair` authenticates headless services using an encrypted **RSA 2048-bit PKCS#8** key pair. The service signs short-lived JWTs in-memory, while Snowflake holds only the public key.

```bash
# 1. Generate encrypted RSA 2048-bit key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -out rsa_key.p8
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# 2. Assign public key to dedicated service user in Snowflake
# ALTER USER DAISY_SERVICE_USER SET RSA_PUBLIC_KEY = '<clean-pubkey-base64>';
```

---

## Service Directory Structure

In adherence to the core platform constraint—**"Each service should live in its own folder"**—all pipeline components and shared utilities maintain modular boundaries within `services/`:

```
daisy_streams/
├── docker-compose.yml          # Multi-service topology (garage, kafka, kafka-ui, ...)
├── Makefile                    # High-level developer targets
├── .env.example                # Unified configuration template
└── services/
    ├── common/                 # Shared S3 buffered logging handler & cross-service utilities
    │   ├── __init__.py
    │   ├── s3_log_handler.py   # S3BufferedLogHandler (Garage S3 batch & interval flushing)
    │   └── tests/
    ├── garage/                 # Garage S3 object storage service config
    │   ├── garage.toml
    │   └── README.md
    ├── kafka_ui/               # Kafka UI web dashboard documentation
    │   └── README.md
    ├── snowflake_adapter/      # Dedicated Snowflake Adapter ingestion service (Part A)
    │   ├── Dockerfile          # Independent container image definition
    │   ├── requirements.txt    # Service dependencies
    │   ├── pyproject.toml      # Service build & test config
    │   ├── snowflake_adapter/  # Core service package (watermark tracking, delta extraction)
    │   ├── scripts/            # Service entrypoint CLI (--loop, --mock) & simulation
    │   └── tests/              # Service automated test suite
    ├── consumer/               # Kafka Consumer streaming ingestion service (Part C)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── consumer/           # Consumer loop, manual offset commit, DQ checks, offset-0 replay
    │   ├── scripts/            # Consumer runner CLI
    │   └── tests/
    ├── postgres/               # PostgreSQL schema, models, and dual-table repository (Part C)
    │   ├── init.sql            # DDL for event_log, current_state, quarantined_events
    │   ├── db.py               # Database connections (Postgres & SQLite in-memory test DB)
    │   ├── repository.py       # Atomic ingestion, monotonic upserts, query repository
    │   ├── models/             # Decoupled Pydantic domain models
    │   └── tests/
    └── api/                    # FastAPI event inspection service (Part C)
        ├── Dockerfile
        ├── requirements.txt
        ├── api/                # FastAPI app, routes (/events, /stats, /healthz), middleware
        ├── scripts/            # API runner CLI
        └── tests/
```

---

## Change Simulation (`scripts/simulate_changes.py`)

In standard Snowflake trial accounts, `SNOWFLAKE_SAMPLE_DATA` is a shared database and read-only. `simulate_changes.py` fulfills **REQ-A06** by providing:

1. **Mutable Working Table Setup**: With `--setup-mutable-table`, creates a working table (`daisy_orders` or custom configured `SNOWFLAKE_TABLE`) in the user's writable database/schema, seeded from `TPCH_SF1.ORDERS`.
2. **Verifiable `INSERT` Operations**: Generates new order records with unique keys and current timestamps.
3. **Verifiable `UPDATE` Operations**: Modifies prices, statuses, and advances timestamps on existing orders.
4. **Offline Dry-Run Mode**: With `--dry-run`, outputs execution receipts without requiring live credentials.

```bash
# Offline dry run simulation
python services/snowflake_adapter/scripts/simulate_changes.py --dry-run

# Live Snowflake execution
python services/snowflake_adapter/scripts/simulate_changes.py --num-inserts 10 --num-updates 5
```

---

## Testing

Run the full automated test suite covering models, Garage S3 state persistence, adapter sync cycles, change simulation, Kafka consumer materialization, and FastAPI inspection endpoints:

```bash
# Run specific service test suites:
pytest services/snowflake_adapter/tests -v
pytest services/api/tests -v
pytest services/consumer/tests -v
pytest services/postgres/tests -v
pytest services/common/tests -v

# Or run the entire test suite via Makefile:
make test
```
