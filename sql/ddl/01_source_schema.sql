CREATE TABLE IF NOT EXISTS customers (
    customer_id      BIGINT PRIMARY KEY,
    first_name       VARCHAR(100) NOT NULL,
    last_name        VARCHAR(100) NOT NULL,
    email            VARCHAR(255) UNIQUE,
    city             VARCHAR(100),
    state            VARCHAR(100),
    signup_date      DATE NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id       BIGINT PRIMARY KEY,
    product_name     VARCHAR(255) NOT NULL,
    category         VARCHAR(100) NOT NULL,
    unit_price       NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stores (
    store_id         BIGINT PRIMARY KEY,
    store_name       VARCHAR(150) NOT NULL,
    city             VARCHAR(100) NOT NULL,
    state            VARCHAR(100),
    opened_date      DATE
);

CREATE TABLE IF NOT EXISTS orders (
    order_id         BIGINT PRIMARY KEY,
    customer_id      BIGINT NOT NULL REFERENCES customers(customer_id),
    store_id         BIGINT REFERENCES stores(store_id),
    order_timestamp  TIMESTAMP NOT NULL,
    order_status     VARCHAR(30) NOT NULL,
    payment_method   VARCHAR(50),
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id    BIGINT PRIMARY KEY,
    order_id         BIGINT NOT NULL REFERENCES orders(order_id),
    product_id       BIGINT NOT NULL REFERENCES products(product_id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price       NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0)
);

CREATE INDEX IF NOT EXISTS idx_orders_timestamp
    ON orders(order_timestamp);

CREATE INDEX IF NOT EXISTS idx_orders_customer
    ON orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_order_items_order
    ON order_items(order_id);
