
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT NOT NULL,
    ordered_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ,
    sales_channel TEXT NOT NULL,
    store_id TEXT,
    shipping_revenue NUMERIC(14, 2) NOT NULL,
    source_record_number BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(14, 2) NOT NULL,
    item_discount_amount NUMERIC(14, 2) NOT NULL,
    source_record_number BIGINT NOT NULL,
    source_record_id TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);
