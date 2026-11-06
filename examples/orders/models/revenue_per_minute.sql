SELECT
  window_start,
  window_end,
  segment,
  currency,
  SUM(amount) AS revenue,
  COUNT(*) AS orders
FROM TABLE(
  TUMBLE(TABLE orders_enriched, DESCRIPTOR(order_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, segment, currency
