"""Kinds of upgrade, as edits to a copy of examples/orders (v1 -> v2).

Each scenario says what should happen. test_upgrade_matrix checks that the three independent
answers agree with each other and with the expectation:
  - `check-upgrade` (logical, from manifests)
  - the `plan` pin report (physical, from the compiled plans)
  - a real savepoint restore of v2 from v1 (strict, and with state loss allowed)
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "examples", "orders")
YAML = "pipeline.yaml"

LTV = """    sql: |
      SELECT customer_id,
             SUM(amount) AS lifetime_value,
             COUNT(*) AS order_count,
             MAX(order_ts) AS last_order_ts
      FROM clean_orders
      GROUP BY customer_id
"""


@dataclass
class Scenario:
    name: str
    edits: list  # (file, old, new)
    verdict: str  # expected check-upgrade verdict: ok | warning | state_loss | breaking
    restores: bool  # does a strict restore succeed?
    restores_with_loss: bool | None = None  # with ignore-unclaimed-state (state_loss cases)
    codes: list = field(default_factory=list)  # change codes check-upgrade must report

    def apply(self, root: str):
        for rel, old, new in self.edits:
            path = os.path.join(root, rel)
            with open(path) as f:
                text = f.read()
            if old not in text:
                raise AssertionError(f"{self.name}: '{old[:40]}...' not in {rel}")
            with open(path, "w") as f:
                f.write(text.replace(old, new, 1))


def copy_example(dst: str) -> str:
    shutil.copytree(EXAMPLE, dst)
    return dst


SCENARIOS = [
    Scenario("identical", [], "ok", True),
    Scenario("comment_and_whitespace", [
        (YAML, "             COUNT(*) AS order_count,", "             count(*)   AS order_count,  -- orders"),
    ], "ok", True),
    Scenario("add_model_and_sink", [
        (YAML, "\nsinks:\n", "\n  orders_per_currency:\n"
               "    sql: SELECT currency, COUNT(*) AS n FROM clean_orders GROUP BY currency\n"
               "\nsinks:\n  currency_counts:\n    from: orders_per_currency\n    connection: print\n"
               "    mode: upsert\n    primary_key: [currency]\n"),
    ], "warning", True, codes=["STATE_NEW", "SINK_ADDED"]),
    Scenario("upstream_filter_change", [
        (YAML, "      FROM orders\n", "      FROM orders\n      WHERE customer_id IS NOT NULL\n"),
    ], "warning", True, codes=["SEMANTIC_DRIFT"]),
    Scenario("change_aggregation", [
        (YAML, "             MAX(order_ts) AS last_order_ts\n",
               "             MAX(order_ts) AS last_order_ts,\n             MIN(order_ts) AS first_order_ts\n"),
    ], "state_loss", False, restores_with_loss=True, codes=["STATE_INCOMPATIBLE", "SINK_SCHEMA_CHANGED"]),
    Scenario("change_group_key", [
        (YAML, "      GROUP BY customer_id\n", "      GROUP BY customer_id, currency\n"),
        (YAML, "      SELECT customer_id,\n             SUM(amount)",
               "      SELECT customer_id, currency,\n             SUM(amount)"),
        (YAML, "    primary_key: [customer_id]\n\n# Where", "    primary_key: [customer_id, currency]\n\n# Where"),
    ], "breaking", False, restores_with_loss=True, codes=["STATE_INCOMPATIBLE", "SINK_SEMANTICS_CHANGED"]),
    Scenario("remove_model_and_sink", [
        (YAML, "  revenue_per_minute:\n    sql_file: models/revenue_per_minute.sql\n\n", ""),
        (YAML, "  revenue_to_lake:\n    from: revenue_per_minute\n    connection: lake\n"
               "    table: analytics.revenue_per_minute\n    mode: append\n\n", ""),
    ], "state_loss", False, restores_with_loss=True, codes=["STATE_DROPPED", "SINK_REMOVED"]),
    Scenario("rename_model", [
        (YAML, "  customer_ltv:\n", "  customer_value:\n"),
        (YAML, "    from: customer_ltv\n", "    from: customer_value\n"),
    ], "ok", True, codes=["RENAMED"]),
    Scenario("rename_upstream_model", [
        (YAML, "  clean_orders:\n", "  valid_orders:\n"),
        (YAML, "      FROM clean_orders\n      GROUP BY", "      FROM valid_orders\n      GROUP BY"),
        ("models/orders_enriched.sql", "FROM clean_orders AS o", "FROM valid_orders AS o"),
    ], "warning", True, codes=["SINK_REMOVED", "SINK_ADDED"]),  # its quarantine topic is named after it
    Scenario("rescale", [
        (YAML, "  parallelism: 2\n", "  parallelism: 3\n"),
    ], "ok", True),
    Scenario("change_max_parallelism", [
        (YAML, "  max_parallelism: 720\n", "  max_parallelism: 360\n"),
    ], "breaking", False, restores_with_loss=False, codes=["MAX_PARALLELISM_CHANGED"]),
    Scenario("change_state_ttl", [
        (YAML, "  state_ttl: 30d\n", "  state_ttl: 7d\n"),
    ], "ok", True),
    Scenario("disable_minibatch", [
        (YAML, '    table.exec.mini-batch.enabled: "true"\n', '    table.exec.mini-batch.enabled: "false"\n'),
    ], "state_loss", False, restores_with_loss=True, codes=["PLANNER_CONFIG_CHANGED"]),
]
