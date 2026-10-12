---
title: Part A - Snowflake Adapter with Incremental Sync
version: 1.0
date_created: 2026-10-12
category: data
tags: [data, snowflake, adapter, sync, watermark]
---

# Part A — Snowflake Adapter with Incremental Sync

## 1. Scope & Objective
Extract records from Snowflake, track high-watermark state, and publish incremental changes without re-emitting processed rows.

## 2. Requirements
- **REQ-A01 (Source Dataset)**: Connect to Snowflake trial (`SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS` joined with `CUSTOMER` or custom orders table).
- **REQ-A02 (Initial Full Sync)**: Extract bounded initial slice of 10k–50k rows.
- **REQ-A03 (Incremental Sync)**: Query delta rows using a monotonic watermark/cursor (`O_ORDERDATE` or `updated_at`).
- **REQ-A04 (No Re-emission)**: Subsequent sync runs must not emit records $\le$ current watermark.
- **REQ-A05 (State Persistence)**: Persist watermark and batch metadata across process restarts in Garage (S3-compatible distributed object storage, `dxflrs/garage`) or local fallback (state backend defended in README).
- **REQ-A06 (Change Simulation)**: Provide `simulate_changes.py` to execute verifiable `INSERT` and `UPDATE` operations in Snowflake.
- **REQ-A07 (Isolated Docker Services)**: Provide dedicated Docker services in `docker-compose.yml` for Garage (`garage` with S3 API, RPC, Admin API, and volume mounts) and the Snowflake Adapter (`snowflake-adapter`), enabling independent lifecycle management and log monitoring.
- **REQ-A08 (Zero-Password Authentication)**: Prohibit plain password authentication. The adapter must strictly support only `key_pair` (asymmetric RSA PKCS#8 key pair for headless production/containers) and `externalbrowser` (IdP SSO for local interactive development).

## 3. Interfaces & Contracts
- **State Store Contract**:
  ```json
  {
    "source_table": "orders",
    "last_watermark": "2026-10-12T00:00:00Z",
    "last_batch_id": "uuid-v4",
    "last_synced_at": "2026-10-12T11:00:00Z",
    "records_synced": 25000
  }
  ```

## 4. Acceptance Criteria
- **AC-A01**: Given initial state, when adapter runs, it ingests 10k–50k rows and records watermark.
- **AC-A02**: Given un-modified source, when adapter re-runs, zero new events are emitted.
- **AC-A03**: Given change simulator updates rows, when adapter runs, only updated/inserted rows are captured.
- **AC-A04**: Given an adapter crash/restart, it resumes strictly from persisted Garage watermark.
- **AC-A05**: Given `docker-compose.yml`, both `garage` and `snowflake-adapter` can be started, monitored, and inspected independently via Docker logs/metrics.
- **AC-A06**: Given Snowflake connection setup, adapter enforces `key_pair` (DER-encoded RSA private key) or `externalbrowser`, rejecting plain passwords and invalid authenticators.

