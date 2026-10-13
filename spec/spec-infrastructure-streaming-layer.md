---
title: Part B - Streaming Layer
version: 1.0
date_created: 2026-10-12
category: infrastructure
tags: [infrastructure, streaming, kafka]
---

# Part B — Streaming Layer

## 1. Scope & Objective
Transport captured change records as partitioned events through Kafka using a standardized event envelope with retry tolerance.

## 2. Requirements
- **REQ-B01 (Broker Topology)**: Single-node Kafka or  deployed via Docker Compose.
- **REQ-B02 (Topic & Partitioning)**: At least one topic (`switch.orders.events`). Partitioning key must be entity key (`order_id`) to preserve per-entity ordering.
- **REQ-B03 (Event Envelope)**: Explicit envelope enforced by Pydantic model.
- **REQ-B04 (Broker Fault Tolerance)**: Producer must handle transient broker downtime with exponential backoff retries. Zero silent data loss.
- **REQ-B05 (Delivery Guarantee)**: Explicitly at-least-once delivery; duplicates handled downstream.

## 3. Interfaces & Contracts
- **Pydantic Envelope Schema**:
  ```python
  class EventEnvelope(BaseModel):
      event_id: UUID
      event_type: Literal["insert", "update"]
      source: str  # e.g., "snowflake.tpch.orders"
      entity_key: str  # e.g., order_id
      payload: dict[str, Any]
      occurred_at: datetime
      captured_at: datetime
      schema_version: str = "1.0.0"
  ```

## 4. Acceptance Criteria
- **AC-B01**: Given captured records, all messages published to Kafka validate against `EventEnvelope`.
- **AC-B02**: Given events for the same `entity_key`, they land in the same topic partition.
- **AC-B03**: Given broker temporarily paused, producer retries until broker resumes without losing records.
