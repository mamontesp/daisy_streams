---
title: Part C - Consumer and Event-Inspection API
version: 1.0
date_created: 2026-10-12
category: tool
tags: [tool, postgres, consumer, fastapi, idempotency, data-quality]
---

# Part C — Consumer and Event-Inspection API

## 1. Scope & Objective
Consume events into PostgreSQL idempotently, validate data quality, and serve inspection endpoints via FastAPI.

## 2. Requirements
- **REQ-C01 (Storage Materialization)**:
  - `event_log`: Append-only table storing all raw events received.
  - `current_state`: Materialized table containing the latest version of each entity key.
- **REQ-C02 (Idempotency)**: Replaying the topic from offset 0 must produce the identical state in `current_state`.
- **REQ-C03 (Data Quality Checks - Minimum 3)**:
  - DQ-1: Envelope schema conformance (valid UUID, required fields).
  - DQ-2: Business logic validity (e.g., total price / amount > 0, non-null).
  - DQ-3: Duplicate event ID detection.
  - Failing records must route to `quarantined_events` / DLQ table with error details. No silent drops.
- **REQ-C04 (FastAPI Endpoints)**:
  - `GET /events`: Paginated events filtered by `entity_key`, `type`, and time range.
  - `GET /entities/{key}`: Returns current entity state plus full chronological change history.
  - `GET /stats`: Returns counts by event type, lag (`processed_at` - `occurred_at`), and last processed watermark.

## 3. Interfaces & Contracts
- **Endpoints**:
  - `GET /events?entity_key=&type=&start=&end=&limit=50&offset=0`
  - `GET /entities/{key}`
  - `GET /stats`

## 4. Acceptance Criteria
- **AC-C01**: Given valid events, both `event_log` and `current_state` are populated.
- **AC-C02**: Given full offset 0 replay, `current_state` row count and column values remain identical.
- **AC-C03**: Given a negative price event, record is quarantined in DLQ table and excluded from `current_state`.
- **AC-C04**: All 3 FastAPI endpoints return valid JSON responses with correct metrics.
