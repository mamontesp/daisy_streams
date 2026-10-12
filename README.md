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

## Quickstart (< 5 Shell Commands)

```bash
# 1. Clone repository and initialize environment variables
cp .env.example .env

# 2. Start all services (Garage, Apache Kafka, PostgreSQL, and Snowflake Adapter)
docker compose up -d garage kafka postgres snowflake-adapter

# 3. Monitor services independently
docker compose logs -f snowflake-adapter
docker compose logs -f garage

# 4. Run change simulation to generate verifiable INSERTs and UPDATEs
python services/snowflake_adapter/scripts/simulate_changes.py --dry-run

# 5. Run Snowflake adapter in offline mock verification mode
python services/snowflake_adapter/scripts/run_adapter.py --mock

# 6. Run automated pytest test suite
pytest services/snowflake_adapter/tests -v
```

---

## Independent Docker Service Monitoring & Garage Operations

Both **Garage** and the **Snowflake Adapter** run as separate container services in `docker-compose.yml`, enabling independent lifecycle control, resource allocation, and log monitoring:

```bash
# Follow live adapter ingestion logs (with JSON correlation IDs)
docker compose logs -f snowflake-adapter

# Follow live Garage S3 logs
docker compose logs -f garage

# Check Garage cluster status directly via entrypoint binary
docker exec daisy-garage /garage status

# List buckets and access keys in Garage
docker exec daisy-garage /garage bucket list
docker exec daisy-garage /garage key list

# Restart adapter independently (verifies AC-A04 crash recovery)
docker compose restart snowflake-adapter
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

In adherence to the core platform constraint—**"Each service should live in its own folder"**—the Snowflake Adapter is completely self-contained within `services/snowflake_adapter/`:

```
daisy_streams/
├── docker-compose.yml          # Multi-service topology (minio, kafka, snowflake-adapter, ...)
├── Makefile                    # High-level developer targets
├── .env.example                # Unified configuration template
└── services/
    └── snowflake_adapter/      # Dedicated Snowflake Adapter service folder
        ├── Dockerfile          # Independent container image definition
        ├── requirements.txt    # Service dependencies
        ├── pyproject.toml      # Service build & test config
        ├── snowflake_adapter/  # Core service package
        │   ├── adapter.py      # Watermark tracking & delta extraction
        │   ├── client.py       # Snowflake connector & mock client
        │   ├── config.py       # Pydantic Settings
        │   ├── logging.py      # Structured JSON correlation logging
        │   ├── queries.py      # Monotonic SQL queries
        │   ├── models/         # EventEnvelope (REQ-B03), SyncState
        │   ├── state/          # MinIO (S3) & local file stores
        │   └── transport/      # Kafka producer with exponential retry
        ├── scripts/
        │   ├── run_adapter.py      # Service entrypoint CLI (--loop, --mock)
        │   └── simulate_changes.py # REQ-A06 change simulation CLI
        └── tests/                  # Service automated test suite
            ├── test_snowflake_adapter.py
            ├── test_models.py
            ├── test_state_store.py
            └── test_simulate_changes.py
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

Run the full automated test suite covering models, MinIO state persistence, adapter sync cycles, and change simulation:

```bash
# Run tests directly or via Makefile
pytest services/snowflake_adapter/tests -v
# or
make test
```
