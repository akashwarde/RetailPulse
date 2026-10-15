-- RetailPulse Data Quality (DQ) Schema
-- Stores quarantine records and per-batch data quality summary metrics.

CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.quarantine (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    rule_failed TEXT NOT NULL,
    reason TEXT,
    batch_id VARCHAR(100) NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_record JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dq_quarantine_batch
    ON dq.quarantine(batch_id);

CREATE INDEX IF NOT EXISTS idx_dq_quarantine_source
    ON dq.quarantine(source);

CREATE INDEX IF NOT EXISTS idx_dq_quarantine_ingested
    ON dq.quarantine(ingested_at);

CREATE TABLE IF NOT EXISTS dq.summary (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    total BIGINT NOT NULL,
    accepted BIGINT NOT NULL,
    rejected BIGINT NOT NULL,
    reject_pct NUMERIC(5,2) NOT NULL,
    per_rule_counts JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_summary_batch
    ON dq.summary(batch_id);

CREATE INDEX IF NOT EXISTS idx_dq_summary_source
    ON dq.summary(source);

CREATE INDEX IF NOT EXISTS idx_dq_summary_created
    ON dq.summary(created_at);
