---
title: Switch Intelligence Platform - End-to-End Streaming Pipeline Specification
version: 1.0
date_created: 2026-10-12
category: architecture
tags: [architecture, snowflake, kafka, redpanda, postgres, fastapi, streaming, docker]
---

# Switch Intelligence Platform — Data Streaming Pipeline

Miniature implementation of Switch's core platform pattern: connect to an external source (Snowflake) via an adapter, capture changes, stream events with correctness guarantees, and materialize downstream into PostgreSQL with an inspection API.

---

## Context & The Task

- **Objective**: Build a robust, runnable local pipeline spanning source capture, streaming transport, materialization, and API inspection.
- **Topology**: Snowflake (external cloud) $\rightarrow$ Python Adapter $\rightarrow$ Kafka/Redpanda $\rightarrow$ Python Consumer $\rightarrow$ PostgreSQL $\leftarrow$ FastAPI Inspection Service.
- **Runtime**: Entire stack (except Snowflake) runs locally via Docker Compose.
- **Guiding Principle**: Production instincts and correctness over feature count.

---

## Part A — Snowflake Adapter with Incremental Sync

1. **Source Data**:
   - Free Snowflake trial account using `SNOWFLAKE_SAMPLE_DATA` (e.g., `TPCH_SF1.ORDERS` joined with `CUSTOMER`) or self-loaded orders CSV.
   - Source table selection must be documented in README.
2. **Sync Mechanics**:
   - **Initial Sync**: Bounded slice of 10,000–50,000 rows.
   - **Incremental Sync**: High-watermark tracking via cursor column (`O_ORDERDATE` or maintained `updated_at` timestamp).
   - **Re-run Safety**: Re-running adapter must not re-emit previously synced records.
3. **State Persistence**:
   - Persist sync watermark and batch metadata across process restarts.
   - Storage backend (e.g., state table in Postgres, dedicated volume/file, or Kafka state topic) selected and defended in README.
4. **Change Simulation**:
   - Provide standalone script (`simulate_changes.py` or `.sql`) executing both `INSERT` and `UPDATE` statements against Snowflake to prove incremental capture.

---

## Part B — Streaming Layer

1. **Broker & Topic Topology**:
   - Redpanda (or Kafka) single-node running in Docker Compose.
   - Minimum 1 topic (e.g., `switch.orders.events`).
   - Partitioning key: Entity primary key (e.g., `order_id`) to ensure strict per-entity ordering.
2. **Explicit Event Envelope**:
   - Implemented via strict Pydantic model (`EventEnvelope`):
     - `event_id` (`UUIDv4`): Unique event identifier.
     - `event_type` (`str`): `insert` | `update`.
     - `source` (`str`): Source system identifier (e.g., `snowflake.orders`).
     - `entity_key` (`str`): Entity primary identifier.
     - `payload` (`dict`): Row payload data.
     - `occurred_at` (`datetime`): Source timestamp.
     - `captured_at` (`datetime`): Ingestion timestamp.
     - `schema_version` (`str`): Semantic version of the envelope (e.g., `1.0.0`).
3. **Resilience & Delivery Guarantees**:
   - Producer retries with exponential backoff on broker unavailability; no silent data loss.
   - Delivery guarantee: **At-least-once**. Downstream consumers must handle duplicates.

---

## Part C — Consumer and Event-Inspection API

1. **PostgreSQL Materialization**:
   - **Event Log Table (`event_log`)**: Append-only log storing every raw event received with ingest metadata.
   - **Current State Table (`current_state`)**: Materialized latest state per entity key.
   - **Idempotency**: Replaying topic from offset 0 must converge to the exact same current state.
2. **FastAPI Event-Inspection Service**:
   - `GET /events`: Paginated stream events. Query params: `entity_key`, `event_type`, `start_time`, `end_time`, `limit`, `offset`.
   - `GET /entities/{key}`: Returns entity's current materialized state and chronological change history.
   - `GET /stats`: Aggregated metrics: event counts by type, lag between `occurred_at` and `processed_at`, and last synced watermark.
3. **Data Quality (DQ) Checks (Minimum 3)**:
   - Check 1: Envelope schema conformance (missing fields, malformed types).
   - Check 2: Business rule integrity (null or negative order amounts/prices).
   - Check 3: Duplicate event ID detection.
   - **Failure Routing**: Non-conforming records routed to dead-letter storage (`dlq_events` table) or quarantined with error reason. Silent drops fail review.

---

## Part D — Engineering Hygiene

1. **Testing**:
   - Unit tests covering watermark progression logic, envelope schema validation, and idempotent upserts.
   - Framework: `pytest`.
2. **Structured Logging**:
   - JSON structured logging with correlation IDs (`event_id`, `batch_id`) across adapter, broker, and consumer.
3. **Configuration**:
   - 100% environment-variable driven (`pydantic-settings`).
   - `.env.example` provided. No secrets in code or git history.
4. **README Requirements**:
   - ASCII architecture diagram.
   - One-command setup ($\le 5$ shell commands total).
   - Defended design decisions & trade-offs.
   - What would be done differently with more time.
   - Multi-tenant architecture design: isolation of state, topics, and credentials (written analysis).

---

## Constraints

- **Language & Runtime**: Python 3.11+.
- **Core Stack**: FastAPI, Redpanda/Kafka, PostgreSQL, Docker Compose.
- **Orchestration / Tooling Restrictions**: No Airflow, Dagster, dbt, or low-code tools.
- **Code Ownership**: Code must be fully owned and modifiable live during interview.
- **Scope Discipline**: Timeboxed to 5–6 hours; cut features before cutting correctness.

---

## Stretch Goals (Optional — Attempt at Most One)

- **Option 1 (Schema Drift)**: Gracefully detect source column additions, update envelope version, and preserve consumer uptime.
- **Option 2 (Exactly-Once Effective)**: Consumer-side deduplication table or transactional outbox with documented edge-case failure windows.

---

## Deliverables & Evaluation

- **Deliverables**:
  1. Private Git repository with atomic, descriptive commits.
  2. 3–5 minute screen recording: startup $\rightarrow$ initial sync $\rightarrow$ change simulation $\rightarrow$ verification via `/events` & `/stats` $\rightarrow$ offset 0 replay convergence.
  3. README meeting Part D criteria.
- **Evaluation Weighting**:
  - Correctness of sync & streaming: 30%
  - Code quality & typing: 25%
  - Data modeling & event design: 15%
  - Operability & logging: 15%
  - Communication & judgment: 15%
