-- Raw landing deliberately permits defects rejected by operational constraints.
CREATE TABLE IF NOT EXISTS raw_sales (
    raw_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT, order_item_id BIGINT, customer_id BIGINT,
    store_id BIGINT, product_id BIGINT, quantity INTEGER,
    unit_price NUMERIC(12,2), order_timestamp TEXT,
    order_status TEXT, payment_method TEXT, source_date DATE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_sales_date ON raw_sales(source_date);
