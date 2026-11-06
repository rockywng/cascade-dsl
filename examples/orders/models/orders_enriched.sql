-- Event-time temporal join: each order sees the customer row as of order_ts.
-- Append-only output; state is bounded by watermarks.
SELECT
  o.order_id,
  o.customer_id,
  c.name AS customer_name,
  COALESCE(c.segment, 'unknown') AS segment,
  o.amount,
  o.currency,
  o.order_ts
FROM clean_orders AS o
LEFT JOIN customers FOR SYSTEM_TIME AS OF o.order_ts AS c
  ON o.customer_id = c.customer_id
