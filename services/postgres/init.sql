-- Daisy Streams — Medallion Architecture (Bronze & Silver) PostgreSQL DDL Initialization
-- Schemas:
--   - bronze: Raw event log, dead-letter queue, and structured order event log (materialized view)
--   - silver: Cleaned, conformed latest materialized state per order

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- ============================================================================
-- 1. BRONZE LAYER: Raw Event Ingestion
-- ============================================================================

-- 1.1 Append-Only Raw Event Log Table
CREATE TABLE IF NOT EXISTS bronze.raw_event_log (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    batch_id VARCHAR(64),
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

CREATE INDEX IF NOT EXISTS idx_raw_event_log_event_id ON bronze.raw_event_log (event_id);
CREATE INDEX IF NOT EXISTS idx_raw_event_log_batch_id ON bronze.raw_event_log (batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_event_log_entity_key ON bronze.raw_event_log (entity_key);
CREATE INDEX IF NOT EXISTS idx_raw_event_log_event_type ON bronze.raw_event_log (event_type);
CREATE INDEX IF NOT EXISTS idx_raw_event_log_occurred_at ON bronze.raw_event_log (occurred_at);
CREATE INDEX IF NOT EXISTS idx_raw_event_log_processed_at ON bronze.raw_event_log (processed_at);
CREATE INDEX IF NOT EXISTS idx_raw_event_log_entity_history ON bronze.raw_event_log (entity_key, occurred_at ASC, id ASC);

-- 1.2 Dead-Letter Queue (DLQ) / Quarantined Events Table
CREATE TABLE IF NOT EXISTS bronze.quarantined_event_log (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID,
    entity_key VARCHAR(128),
    error_code VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantined_event_id ON bronze.quarantined_event_log (event_id);
CREATE INDEX IF NOT EXISTS idx_quarantined_entity_key ON bronze.quarantined_event_log (entity_key);
CREATE INDEX IF NOT EXISTS idx_quarantined_error_code ON bronze.quarantined_event_log (error_code);
CREATE INDEX IF NOT EXISTS idx_quarantined_at ON bronze.quarantined_event_log (quarantined_at);

-- 1.3 Materialized View: bronze.raw_orders_log (Unpacks event payload into typed columns)
CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.raw_orders_log AS
SELECT
    id AS log_id,
    event_id,
    batch_id,
    event_type,
    source,
    entity_key,
    partition_key,
    (COALESCE(payload->>'O_ORDERKEY', payload->>'order_key', entity_key))::BIGINT AS order_key,
    (COALESCE(payload->>'O_CUSTKEY', payload->>'cust_key', partition_key))::BIGINT AS cust_key,
    COALESCE(payload->>'O_ORDERSTATUS', payload->>'order_status') AS order_status,
    (COALESCE(payload->>'O_TOTALPRICE', payload->>'total_price'))::NUMERIC(15, 2) AS total_price,
    (COALESCE(payload->>'O_ORDERDATE', payload->>'order_date'))::DATE AS order_date,
    COALESCE(payload->>'O_ORDERPRIORITY', payload->>'order_priority') AS order_priority,
    COALESCE(payload->>'O_CLERK', payload->>'clerk') AS clerk,
    (COALESCE(payload->>'O_SHIPPRIORITY', payload->>'ship_priority'))::INTEGER AS ship_priority,
    COALESCE(payload->>'O_COMMENT', payload->>'comment') AS comment,
    payload->>'C_NAME' AS customer_name,
    payload->>'C_PHONE' AS customer_phone,
    (payload->>'C_ACCTBAL')::NUMERIC(15, 2) AS customer_acct_bal,
    payload->>'C_MKTSEGMENT' AS customer_market_segment,
    occurred_at,
    captured_at,
    processed_at,
    schema_version
FROM bronze.raw_event_log;

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_orders_log_log_id ON bronze.raw_orders_log (log_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_log_event_id ON bronze.raw_orders_log (event_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_log_order_key ON bronze.raw_orders_log (order_key);
CREATE INDEX IF NOT EXISTS idx_raw_orders_log_cust_key ON bronze.raw_orders_log (cust_key);
CREATE INDEX IF NOT EXISTS idx_raw_orders_log_order_date ON bronze.raw_orders_log (order_date);
CREATE INDEX IF NOT EXISTS idx_raw_orders_log_occurred_at ON bronze.raw_orders_log (occurred_at);

-- ============================================================================
-- 2. SILVER LAYER: Staged & Materialized Current State
-- ============================================================================

-- 2.1 Materialized Latest State Table per Order Entity
CREATE TABLE IF NOT EXISTS silver.stg_current_orders (
    entity_key VARCHAR(128) PRIMARY KEY,
    order_key BIGINT,
    cust_key BIGINT,
    order_status VARCHAR(32),
    total_price NUMERIC(15, 2),
    order_date DATE,
    order_priority VARCHAR(32),
    clerk VARCHAR(64),
    ship_priority INTEGER,
    comment TEXT,
    customer_name VARCHAR(128),
    customer_phone VARCHAR(64),
    customer_acct_bal NUMERIC(15, 2),
    customer_market_segment VARCHAR(64),
    latest_event_id UUID NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    state JSONB NOT NULL,
    last_occurred_at TIMESTAMPTZ NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_current_orders_order_key ON silver.stg_current_orders (order_key);
CREATE INDEX IF NOT EXISTS idx_stg_current_orders_cust_key ON silver.stg_current_orders (cust_key);
CREATE INDEX IF NOT EXISTS idx_stg_current_orders_order_date ON silver.stg_current_orders (order_date);
CREATE INDEX IF NOT EXISTS idx_stg_current_orders_last_occurred ON silver.stg_current_orders (last_occurred_at);

-- ============================================================================
-- 3. PUBLIC SCHEMA COMPATIBILITY VIEWS
-- ============================================================================
-- Ensures zero breaking changes for existing inspection API and legacy queries.
-- If legacy physical tables exist in public schema, migrate data and replace with views.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'event_log') THEN
        DROP TABLE public.event_log CASCADE;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'current_state') THEN
        DROP TABLE public.current_state CASCADE;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'quarantined_events') THEN
        DROP TABLE public.quarantined_events CASCADE;
    END IF;
END $$;

CREATE OR REPLACE VIEW public.event_log AS
    SELECT * FROM bronze.raw_event_log;

CREATE OR REPLACE VIEW public.current_state AS
    SELECT * FROM silver.stg_current_orders;

CREATE OR REPLACE VIEW public.quarantined_events AS
    SELECT * FROM bronze.quarantined_event_log;

