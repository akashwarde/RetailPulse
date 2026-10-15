-- =============================================================================
-- Mart 1: Daily Revenue Trends
-- Schema: dw
-- View: mart_daily_revenue
--
-- Grain: One row per calendar date with recorded sales transactions.
-- Metrics: Total revenue, distinct orders count, total units sold, avg order value.
-- =============================================================================

CREATE OR REPLACE VIEW dw.mart_daily_revenue AS
SELECT
    f.date_key,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    COALESCE(SUM(f.revenue), 0.00) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COALESCE(SUM(f.quantity), 0) AS total_units_sold,
    ROUND(
        COALESCE(SUM(f.revenue), 0.00) / NULLIF(COUNT(DISTINCT f.order_id), 0),
        2
    ) AS avg_order_value
FROM dw.fact_sales f
JOIN dw.dim_date d ON f.date_key = d.date_key
GROUP BY
    f.date_key,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend;
