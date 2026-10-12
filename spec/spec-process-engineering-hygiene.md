---
title: Part D - Engineering Hygiene, Constraints & Deliverables
version: 1.0
date_created: 2026-10-12
category: process
tags: [process, testing, logging, docker, hygiene]
---

# Part D — Engineering Hygiene, Constraints & Deliverables

## 1. Testing & Code Quality
- **Unit Tests**: Watermark progression logic, envelope validation, and idempotent upserts using `pytest`.
- **Typing**: Fully typed Python 3.11+ code with clear domain separation (adapter, transport, consumer, API).

## 2. Observability & Operations
- **Structured Logging**: JSON logs with correlation IDs (`event_id`, `batch_id`) tracing flow end-to-end.
- **Config Management**: Environment variables with `.env.example`; zero credentials in code or repository.
- **Docker Compose**: Brings up Redpanda, Postgres, adapter, consumer, and API in $\le 5$ commands (Makefile included).

## 3. README Requirements
- ASCII architecture diagram.
- Setup guide ($\le 5$ commands).
- Key design decisions, trade-offs, and future improvements.
- Multi-tenant scaling strategy (isolation of state, topics, credentials).

## 4. Constraints & Prohibitions
- No orchestrators (Airflow/Dagster), no dbt, no low-code tools.
- Strict ownership of every line of code.

## 5. Deliverables
- Private Git repository with atomic commit history.
- 3–5 min video walkthrough demonstrating initial sync, change simulation, API query, and replay convergence.
- README fulfilling all documentation criteria.
