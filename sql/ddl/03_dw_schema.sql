-- =============================================================================
-- RetailPulse Data Warehouse (DW) Star Schema DDL
-- Target: PostgreSQL 17 (warehouse_db)
-- Schema: dw
--
-- Design Overview & Architectural Choices:
-- 1. Star Schema Architecture:
--    - Dimension tables: dim_date, dim_product, dim_customer, dim_store.
--    - Fact tables: fact_sales, fact_inventory.
--    - Normalized dimensions connected to centralized fact tables for optimal
--      OLAP query performance and dimensional slicing.
-- 2. Key Architecture:
--    - Surrogate keys (SERIAL / BIGSERIAL) as primary keys decouple analytical
--      serving from mutable operational source IDs and support type-1/type-2 SCDs.
--    - Natural / Business keys (product_id, customer_id, store_id) enforced UNIQUE.
--    - Foreign keys on fact tables enforce referential integrity with CASCADE/RESTRICT
--      safety.
-- 3. Idempotency:
--    - DDL utilizes CREATE SCHEMA IF NOT EXISTS, CREATE TABLE IF NOT EXISTS,
--      CREATE INDEX IF NOT EXISTS, and CREATE OR REPLACE VIEW.
--    - Date dimension includes a 2-year pre-population generator using generate_series
--      with ON CONFLICT (date_key) DO NOTHING to safely allow re-execution.
-- 4. Analytical Performance:
--    - B-Tree indexes created on all fact table foreign key columns to eliminate
--      full table scans during dimensional joins.
--    - Analytical Mart views (mart_daily_revenue, mart_top_products, mart_low_stock,
--      mart_dq_summary) encapsulate common aggregations for BI dashboards (Metabase).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- =============================================================================
-- 1. DIMENSION TABLES
-- =============================================================================

-- Date Dimension (2 years: 2024-01-01 through 2025-12-31)
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key        INT PRIMARY KEY,              -- Format: YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    day             INT NOT NULL,                 -- Day of month (1-31)
    month           INT NOT NULL,                 -- Month number (1-12)
    month_name      VARCHAR(20) NOT NULL,         -- e.g., 'January'
    quarter         INT NOT NULL,                 -- Quarter (1-4)
    year            INT NOT NULL,                 -- e.g., 2024, 2025
    day_name        VARCHAR(20) NOT NULL,         -- e.g., 'Monday'
    is_weekend      BOOLEAN NOT NULL              -- TRUE for Saturday/Sunday
);

-- Pre-populate dim_date with 2 full years (2024-01-01 to 2025-12-31)
INSERT INTO dw.dim_date (
    date_key,
    full_date,
    day,
    month,
    month_name,
    quarter,
    year,
    day_name,
    is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d::DATE AS full_date,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(MONTH FROM d)::INT AS month,
    TRIM(TO_CHAR(d, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(YEAR FROM d)::INT AS year,
    TRIM(TO_CHAR(d, 'Day')) AS day_name,
    (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
FROM generate_series('2024-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

-- Product Dimension
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key     SERIAL PRIMARY KEY,           -- Surrogate key
    product_id      BIGINT NOT NULL UNIQUE,       -- Natural / source ID
    name            VARCHAR(255) NOT NULL,        -- Product name
    category        VARCHAR(100) NOT NULL,
    price           NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key    SERIAL PRIMARY KEY,           -- Surrogate key
    customer_id     BIGINT NOT NULL UNIQUE,       -- Natural / source ID
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(100),
    segment         VARCHAR(50) NOT NULL DEFAULT 'Standard',
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Store Dimension
CREATE TABLE IF NOT EXISTS dw.dim_store (
    store_key       SERIAL PRIMARY KEY,           -- Surrogate key
    store_id        BIGINT NOT NULL UNIQUE,       -- Natural / source ID
    store_name      VARCHAR(150),
    city            VARCHAR(100) NOT NULL,
    state           VARCHAR(100),
    store_type      VARCHAR(50) NOT NULL DEFAULT 'Physical', -- e.g., 'Physical', 'Online'
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 2. FACT TABLES
-- =============================================================================

-- Sales Fact Table
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sales_key       BIGSERIAL PRIMARY KEY,        -- Surrogate key
    order_id        BIGINT NOT NULL,              -- Natural order identifier
    order_item_id   BIGINT,                       -- Natural line item identifier
    date_key        INT NOT NULL REFERENCES dw.dim_date(date_key),
    product_key     INT NOT NULL REFERENCES dw.dim_product(product_key),
    customer_key    INT NOT NULL REFERENCES dw.dim_customer(customer_key),
    store_key       INT REFERENCES dw.dim_store(store_key),  -- Nullable for online orders
    quantity        INT NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    revenue         NUMERIC(12, 2) NOT NULL CHECK (revenue >= 0),
    channel         VARCHAR(20) NOT NULL DEFAULT 'in_store' CHECK (channel IN ('in_store', 'online')),
    order_timestamp TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Inventory Snapshot Fact Table
CREATE TABLE IF NOT EXISTS dw.fact_inventory (
    inventory_key   BIGSERIAL PRIMARY KEY,        -- Surrogate key
    date_key        INT NOT NULL REFERENCES dw.dim_date(date_key),
    product_key     INT NOT NULL REFERENCES dw.dim_product(product_key),
    store_key       INT NOT NULL REFERENCES dw.dim_store(store_key),
    stock_qty       INT NOT NULL CHECK (stock_qty >= 0),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_inventory UNIQUE (date_key, product_key, store_key)
);

-- =============================================================================
-- 3. INDEXES ON FACT FOREIGN KEYS & FREQUENT FILTER COLUMNS
-- =============================================================================

-- Fact Sales foreign key & lookup indexes
CREATE INDEX IF NOT EXISTS idx_fact_sales_date
    ON dw.fact_sales(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product
    ON dw.fact_sales(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer
    ON dw.fact_sales(customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_store
    ON dw.fact_sales(store_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_channel
    ON dw.fact_sales(channel);

CREATE INDEX IF NOT EXISTS idx_fact_sales_order
    ON dw.fact_sales(order_id);

-- Fact Inventory foreign key indexes
CREATE INDEX IF NOT EXISTS idx_fact_inventory_date
    ON dw.fact_inventory(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_inventory_product
    ON dw.fact_inventory(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_inventory_store
    ON dw.fact_inventory(store_key);

-- =============================================================================
-- 4. ANALYTICAL MARTS (VIEWS)
-- =============================================================================

-- Mart 1: Daily Revenue Trends
CREATE OR REPLACE VIEW dw.mart_daily_revenue AS
SELECT
    f.date_key,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value
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

-- Mart 2: Top Products by Revenue
CREATE OR REPLACE VIEW dw.mart_top_products AS
SELECT
    p.product_key,
    p.product_id,
    p.name AS product_name,
    p.category,
    p.price,
    SUM(f.revenue) AS total_revenue,
    SUM(f.quantity) AS total_units_sold,
    COUNT(DISTINCT f.order_id) AS total_orders,
    DENSE_RANK() OVER (ORDER BY SUM(f.revenue) DESC) AS revenue_rank
FROM dw.fact_sales f
JOIN dw.dim_product p ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_id,
    p.name,
    p.category,
    p.price;

-- Mart 3: Low Stock Alerts (Stock below 10 units)
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

-- Mart 4: Data Quality Scorecard (Aggregated from dq.summary)
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
