"""Clickstream attribution, written with the Python builder API.

page views are deduplicated, counted in sliding windows per country, and joined to purchases that
happen within 30 minutes of a view to attribute revenue to URLs.
"""
from cascade_dsl.api import Pipeline, count, count_distinct, expect, iceberg, kafka, sum_

pipeline = p = Pipeline("clickstream", owner="growth", state_ttl="1d")

events = p.connection("events", kafka(bootstrap_servers="localhost:9092"))
lake = p.connection("lake", iceberg(catalog_type="rest", uri="https://catalog.internal"))

page_views = p.source(
    "page_views", events, topic="page_views", format="json",
    columns={"view_id": "STRING NOT NULL", "user_id": "STRING", "url": "STRING", "country": "STRING",
             "view_ts": "TIMESTAMP(3)"},
    event_time=("view_ts", "10s"),
)
purchases = p.source(
    "purchases", events, topic="purchases", format="json",
    columns={"purchase_id": "STRING NOT NULL", "user_id": "STRING", "amount": "DECIMAL(10, 2)",
             "purchase_ts": "TIMESTAMP(3)"},
    event_time=("purchase_ts", "10s"),
)


@p.model(expectations=[expect.drop("has_user", "user_id IS NOT NULL")])
def unique_views(page_views):
    """Producers retry, so the same view can arrive more than once; keep the first."""
    return page_views.dedup("view_id", order_by="view_ts", keep="first")


@p.model
def views_per_country(page_views):
    """Views and distinct users per country over a 5-minute window sliding every minute."""
    return (page_views.hop("view_ts", slide="1m", size="5m")
            .group_by("country")
            .agg(views=count(), users=count_distinct("user_id")))


@p.model
def attributed_purchases(unique_views, purchases):
    """Purchases made within 30 minutes after a view of a URL by the same user."""
    return (unique_views
            .join(purchases, on=["user_id"], within=("view_ts", "purchase_ts", "30m"))
            .select("unique_views.url", "purchases.purchase_id", "purchases.amount", "purchases.purchase_ts"))


@p.model
def revenue_by_url(attributed_purchases):
    return attributed_purchases.group_by("url").agg(revenue=sum_("amount"), purchases=count())


p.sink("views_to_lake", views_per_country, lake, table="growth.views_per_country")
p.sink("url_revenue", revenue_by_url, events, topic="url_revenue", mode="upsert", primary_key=["url"])
