CREATE TABLE customers (
  customer_id TEXT PRIMARY KEY,
  segment     TEXT,
  updated_at  TIMESTAMP(3) NOT NULL
);
-- full before-images for the CDC changelog
ALTER TABLE customers REPLICA IDENTITY FULL;

-- JDBC upsert sink target
CREATE TABLE customer_ltv (
  customer_id    TEXT PRIMARY KEY,
  lifetime_value NUMERIC(38, 2),
  order_count    BIGINT
);

INSERT INTO customers VALUES
  ('c1', 'smb',        '2026-01-01 09:00:00'),
  ('c2', 'enterprise', '2026-01-01 09:00:00');

-- soak --slot-loss resnapshot: customers per segment, compared against this table's truth
CREATE TABLE segment_counts (
  segment   TEXT PRIMARY KEY,
  customers BIGINT
);
