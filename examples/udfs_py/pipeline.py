"""The UDF example (examples/udfs/pipeline.yaml) with the Python API. Python functions are passed
as function objects from a project module; calling the returned handle renders a SQL call."""
from cascade_dsl.api import Pipeline, kafka

from udfs.scoring import churn_risk as churn_risk_py

pipeline = p = Pipeline("customer_scoring", owner="data-science", state_ttl="7d",
                        scaling={"max_parallelism": 360, "parallelism": 1, "max": 12})

mask_email = p.java_function("mask_email", "com.acme.udf.MaskEmail", jar="../udfs/udfs/acme-udfs.jar",
                             returns="STRING",
                             description="Keeps the first character and the domain of an email address")
churn_risk = p.python_function(churn_risk_py, returns="DOUBLE")

events = p.connection("events", kafka(bootstrap_servers="localhost:9092"))
activity = p.source(
    "activity", events, topic="customer_activity", format="json",
    columns={"customer_id": "STRING NOT NULL", "email": "STRING", "days_inactive": "INT", "orders_90d": "INT",
             "ts": "TIMESTAMP(3)"},
    event_time=("ts", "5s"),
)


@p.model(description="Churn risk per activity event, with the email masked before it leaves the job")
def scored_customers(activity):
    return activity.select("customer_id", "ts", email_masked=mask_email("email"),
                           churn=churn_risk("days_inactive", "orders_90d"))


p.sink("scores", scored_customers, events, topic="customer_scores", format="json")
