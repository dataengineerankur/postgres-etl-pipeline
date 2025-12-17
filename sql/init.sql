-- Grocery analytics schema (created inside the container's default DB; compose uses POSTGRES_DB=airflow)
CREATE SCHEMA IF NOT EXISTS grocery;

CREATE TABLE IF NOT EXISTS grocery.dim_store (
  store_id TEXT PRIMARY KEY,
  store_name TEXT NOT NULL,
  region TEXT NOT NULL,
  opened_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS grocery.dim_product (
  sku TEXT PRIMARY KEY,
  product_name TEXT NOT NULL,
  category TEXT NOT NULL,
  is_perishable BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS grocery.stg_transactions (
  run_id TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  txn_id TEXT NOT NULL,
  store_id TEXT NOT NULL,
  sku TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  unit_price_cents INTEGER NOT NULL,
  tender_type TEXT NOT NULL,
  customer_id TEXT NULL,
  raw_payload JSONB NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (run_id, txn_id)
);

CREATE TABLE IF NOT EXISTS grocery.fct_sales (
  event_time TIMESTAMPTZ NOT NULL,
  txn_id TEXT PRIMARY KEY,
  store_id TEXT NOT NULL,
  sku TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  revenue_cents BIGINT NOT NULL,
  tender_type TEXT NOT NULL,
  region TEXT NOT NULL,
  category TEXT NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed dims
INSERT INTO grocery.dim_store (store_id, store_name, region, opened_date)
VALUES
  ('SFO-001', 'Market St Grocery', 'west', '2017-03-01'),
  ('NYC-014', 'Union Sq Grocery', 'east', '2019-09-12'),
  ('AUS-002', 'Congress Ave Grocery', 'south', '2020-01-20')
ON CONFLICT (store_id) DO NOTHING;

INSERT INTO grocery.dim_product (sku, product_name, category, is_perishable)
VALUES
  ('SKU-APPLE', 'Apple', 'produce', TRUE),
  ('SKU-MILK', 'Milk', 'dairy', TRUE),
  ('SKU-BREAD', 'Bread', 'bakery', TRUE),
  ('SKU-COFFEE', 'Coffee', 'beverages', FALSE),
  ('SKU-RICE', 'Rice', 'pantry', FALSE)
ON CONFLICT (sku) DO NOTHING;


