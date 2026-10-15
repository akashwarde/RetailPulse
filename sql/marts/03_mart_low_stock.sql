-- =============================================================================
-- Mart 3: Low Stock Alerts & Inventory Health
-- Schema: dw
-- View: mart_low_stock
--
-- Grain: One row per low-stock item snapshot where stock_qty < 10.
-- Categorization:
--   - 0: OUT_OF_STOCK
--   - 1-5: CRITICAL
--   - 6-9: LOW
-- =============================================================================

CREATE OR REPLACE VIEW dw.mart_low_stock AS
SELECT
    fi.inventory_key,
    fi.date_key,
    d.full_date AS snapshot_date,
    p.product_key,
    p.product_id,
    p.name AS product_name,
    p.category,
    s.store_key,
    s.store_id,
    s.store_name,
    s.city AS store_city,
    fi.stock_qty,
    10 AS low_stock_threshold,
    CASE
        WHEN fi.stock_qty = 0 THEN 'OUT_OF_STOCK'
        WHEN fi.stock_qty <= 5 THEN 'CRITICAL'
        ELSE 'LOW'
    END AS stock_status
FROM dw.fact_inventory fi
JOIN dw.dim_date d ON fi.date_key = d.date_key
JOIN dw.dim_product p ON fi.product_key = p.product_key
JOIN dw.dim_store s ON fi.store_key = s.store_key
WHERE fi.stock_qty < 10;
