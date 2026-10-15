-- =============================================================================
-- Mart 2: Product Performance & Top Products by Revenue
-- Schema: dw
-- View: mart_top_products
--
-- Grain: One row per dimensional product with historical sales.
-- Metrics: Total revenue, units sold, order count, and dense rank by revenue.
-- Slicing: Supports category rollup and top-N ranking.
-- =============================================================================

CREATE OR REPLACE VIEW dw.mart_top_products AS
SELECT
    p.product_key,
    p.product_id,
    p.name AS product_name,
    p.category,
    p.price,
    COALESCE(SUM(f.revenue), 0.00) AS total_revenue,
    COALESCE(SUM(f.quantity), 0) AS total_units_sold,
    COUNT(DISTINCT f.order_id) AS total_orders,
    DENSE_RANK() OVER (ORDER BY COALESCE(SUM(f.revenue), 0.00) DESC) AS revenue_rank
FROM dw.fact_sales f
JOIN dw.dim_product p ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_id,
    p.name,
    p.category,
    p.price;
