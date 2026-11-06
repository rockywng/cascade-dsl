"""The orders example from examples/orders/pipeline.yaml, written with the Python API.

Compiles to the same job and manifest as the YAML version (tests/test_python_api.py checks this).
"""
from cascade_dsl.api import Pipeline, expect, iceberg, kafka, postgres_cdc, secret

pipeline = Pipeline(
    "orders_analytics",
    description="Enriched orders, per-minute revenue and customer LTV",
    owner="data-eng",
    state_ttl="30d",
    scaling={"max_parallelism": 720, "parallelism": 2, "min": 1, "max": 48,
             "target_utilization": 0.7, "max_lag": "1m"},
    flink_config={
        "table.exec.mini-batch.enabled": "true",
        "table.exec.mini-batch.allow-latency": "2s",
        "table.exec.mini-batch.size": "5000",
    },
    deploy={"image": "ghcr.io/acme/cascade-orders:${var:image_tag}", "namespace": "data-pipelines",
            "jobmanager": {"memory": "2g", "cpu": 1}, "taskmanager": {"memory": "4g", "cpu": 2, "slots": 2},
            "checkpoints": {"interval": "30s", "dir": "${var:state_bucket}/checkpoints"},
            "savepoints": "${var:state_bucket}/savepoints", "upgrade_mode": "savepoint"},
    vars={"kafka_bootstrap": "localhost:9092", "warehouse": "s3://dev-lake/warehouse",
          "image_tag": "dev", "state_bucket": "s3://dev-flink-state/orders"},
    environments={"prod": {"kafka_bootstrap": secret("kafka/prod/bootstrap"),
                           "warehouse": "s3://prod-lake/warehouse",
                           "image_tag": "1.4.0", "state_bucket": "s3://prod-flink-state/orders"}},
)
p = pipeline

events = p.connection("events", kafka(bootstrap_servers=p.var("kafka_bootstrap")))
crm_db = p.connection("crm_db", postgres_cdc(hostname="crm.internal", port=5432, database="crm",
                                             username="flink_cdc", password=secret("crm/password")))
lake = p.connection("lake", iceberg(catalog_type="rest", uri="https://catalog.internal",
                                    warehouse=p.var("warehouse")))

orders = p.source(
    "orders", events, topic="orders.v1", format="json", startup="earliest",
    columns={"order_id": "STRING NOT NULL", "customer_id": "STRING", "amount": "DECIMAL(12, 2)",
             "currency": "STRING", "order_ts": "TIMESTAMP(3)"},
    event_time=("order_ts", "5s"),
)
customers = p.source(
    "customers", crm_db, table="public.customers",
    columns={"customer_id": "STRING NOT NULL", "name": "STRING", "segment": "STRING",
             "updated_at": "TIMESTAMP(3)"},
    primary_key=["customer_id"],
    event_time=("updated_at", "1s"),
)


@p.model(
    description="Orders that pass basic quality checks",
    columns={"order_id": "STRING NOT NULL", "customer_id": "STRING", "amount": "DECIMAL(12, 2)",
             "currency": "STRING", "order_ts": "TIMESTAMP(3)"},
    expectations=[
        expect.fail("has_order_id", "order_id IS NOT NULL"),
        expect.drop("positive_amount", "amount > 0"),
        expect.quarantine("known_currency", "currency IN ('USD', 'EUR', 'GBP')"),
    ],
)
def clean_orders(orders):
    return f"""
      SELECT order_id, customer_id, amount, UPPER(currency) AS currency, order_ts
      FROM {orders}
    """


@p.model
def orders_enriched(clean_orders, customers):
    # Event-time temporal join: each order sees the customer row as of order_ts.
    return f"""
      SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,
        COALESCE(c.segment, 'unknown') AS segment,
        o.amount,
        o.currency,
        o.order_ts
      FROM {clean_orders} AS o
      LEFT JOIN {customers} FOR SYSTEM_TIME AS OF o.order_ts AS c
        ON o.customer_id = c.customer_id
    """


@p.model
def revenue_per_minute(orders_enriched):
    return f"""
      SELECT
        window_start,
        window_end,
        segment,
        currency,
        SUM(amount) AS revenue,
        COUNT(*) AS orders
      FROM TABLE(
        TUMBLE(TABLE {orders_enriched}, DESCRIPTOR(order_ts), INTERVAL '1' MINUTE)
      )
      GROUP BY window_start, window_end, segment, currency
    """


@p.model(description="Running lifetime value per customer (updating result)", scaling={"min": 2})
def customer_ltv(clean_orders):
    return f"""
      SELECT customer_id,
             SUM(amount) AS lifetime_value,
             COUNT(*) AS order_count,
             MAX(order_ts) AS last_order_ts
      FROM {clean_orders}
      GROUP BY customer_id
    """


p.sink("revenue_to_lake", revenue_per_minute, lake, table="analytics.revenue_per_minute", mode="append")
p.sink("ltv_to_kafka", customer_ltv, events, topic="customer_ltv.v1", format="json", mode="upsert",
       primary_key=["customer_id"])

p.quarantine(events, topic="{pipeline}.{model}.quarantine", format="json")
