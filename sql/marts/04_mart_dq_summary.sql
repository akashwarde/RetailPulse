-- =============================================================================
-- Mart 4: Data Quality Scorecard
-- Schema: dw
-- View: mart_dq_summary
--
-- Grain: One row per source dataset across ingestion batches.
-- Source: dq.summary
-- Metrics: Total batches, total records, accepted rows, rejected rows, reject pct.
-- =============================================================================

CREATE OR REPLACE VIEW dw.mart_dq_summary AS
SELECT
    source,
    COUNT(DISTINCT batch_id) AS total_batches,
    SUM(total) AS total_records,
    SUM(accepted) AS total_accepted,
    SUM(rejected) AS total_rejected,
    ROUND(
        COALESCE(
            (SUM(rejected)::NUMERIC / NULLIF(SUM(total), 0)::NUMERIC) * 100.0,
            0.0
        ),
        2
    ) AS overall_reject_pct,
    MAX(created_at) AS latest_batch_time
FROM dq.summary
GROUP BY
    source;
