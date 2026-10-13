-- Daisy Streams — Part C PostgreSQL DDL Initialization
-- Tables:
--   1. event_log: Append-only raw event ledger (REQ-C01)
--   2. current_state: Materialized latest state per entity key (REQ-C01, REQ-C02)
--   3. quarantined_events: Dead-Letter Queue (DLQ) for DQ failures (REQ-C03)

-- 1. Append-Only Event Log Table
CREATE TABLE IF NOT EXISTS event_log (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    source VARCHAR(128) NOT NULL,
    entity_key VARCHAR(128) NOT NULL,
    partition_key VARCHAR(128),
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version VARCHAR(32) NOT NULL DEFAULT '1.0.0'
);

CREATE INDEX IF NOT EXISTS idx_event_log_event_id ON event_log (event_id);
CREATE INDEX IF NOT EXISTS idx_event_log_entity_key ON event_log (entity_key);
CREATE INDEX IF NOT EXISTS idx_event_log_event_type ON event_log (event_type);
CREATE INDEX IF NOT EXISTS idx_event_log_occurred_at ON event_log (occurred_at);
CREATE INDEX IF NOT EXISTS idx_event_log_processed_at ON event_log (processed_at);
CREATE INDEX IF NOT EXISTS idx_event_log_entity_history ON event_log (entity_key, occurred_at ASC, id ASC);

-- 2. Materialized Current State Table
CREATE TABLE IF NOT EXISTS current_state (
    entity_key VARCHAR(128) PRIMARY KEY,
    latest_event_id UUID NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    state JSONB NOT NULL,
    last_occurred_at TIMESTAMPTZ NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_current_state_last_occurred ON current_state (last_occurred_at);

-- 3. Dead-Letter Queue (DLQ) / Quarantined Events Table
CREATE TABLE IF NOT EXISTS quarantined_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID,
    entity_key VARCHAR(128),
    error_code VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantined_event_id ON quarantined_events (event_id);
CREATE INDEX IF NOT EXISTS idx_quarantined_entity_key ON quarantined_events (entity_key);
CREATE INDEX IF NOT EXISTS idx_quarantined_error_code ON quarantined_events (error_code);
CREATE INDEX IF NOT EXISTS idx_quarantined_quarantined_at ON quarantined_events (quarantined_at);
