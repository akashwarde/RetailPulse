-- =============================================================================
-- Mart 5: Store & Channel Performance (Online vs In-Store)
-- Schema: dw
-- View: mart_channel_performance
--
-- Grain: One row per (date_key, channel, store_id).
-- Metrics: Revenue, order count, units sold, average order value, and channel share pct.
-- Reporting: Enables online vs in-store share comparison and physical store rankings.
-- =============================================================================

CREATE OR REPLACE VIEW dw.mart_channel_performance AS
SELECT
    f.date_key,
    d.full_date,
    f.channel,
    COALESCE(s.store_id, 0) AS store_id,
    COALESCE(s.store_name, 'Online Channel') AS store_name,
    COALESCE(s.city, 'Online') AS city,
    COALESCE(s.state, 'N/A') AS state,
    COALESCE(SUM(f.revenue), 0.00) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COALESCE(SUM(f.quantity), 0) AS total_units_sold,
    ROUND(
        COALESCE(SUM(f.revenue), 0.00) / NULLIF(COUNT(DISTINCT f.order_id), 0),
        2
    ) AS avg_order_value,
    ROUND(
        COALESCE(SUM(f.revenue), 0.00) * 100.0 / NULLIF(SUM(SUM(f.revenue)) OVER (PARTITION BY f.date_key), 0),
        2
    ) AS channel_share_pct
FROM dw.fact_sales f
JOIN dw.dim_date d ON f.date_key = d.date_key
LEFT JOIN dw.dim_store s ON f.store_key = s.store_key
GROUP BY
    f.date_key,
    d.full_date,
    f.channel,
    s.store_id,
    s.store_name,
    s.city,
    s.state;
