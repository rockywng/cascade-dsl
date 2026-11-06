"""Cross-checks static analysis against the real Flink planner. Requires FLINK_HOME."""
import os
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.verify import find_flink_home, run_verify

from helpers import compile_text, one_model

FLINK = find_flink_home()
EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "examples", "orders")

# (name, sql, expected static changelog); updating cases whose key the compiler cannot prove
# to equal the sink key are written with `on_conflict: error` (see CD410)
CASES = [
    ("projection", "SELECT order_id AS k, amount FROM orders", "append"),
    ("running_agg", "SELECT customer_id AS k, SUM(amount) AS s FROM orders GROUP BY customer_id", "updating"),
    ("global_agg", "SELECT 'all' AS k, COUNT(*) AS n FROM orders", "updating"),
    ("window_agg", """SELECT window_start AS k, window_end, COUNT(*) AS n
        FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
        GROUP BY window_start, window_end""", "append"),
    ("hop_window", """SELECT window_end AS k, SUM(amount) AS s
        FROM TABLE(HOP(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
        GROUP BY window_start, window_end""", "append"),
    ("inner_join", """SELECT o.order_id AS k, p.paid FROM orders o
        JOIN payments p ON o.order_id = p.order_id""", "append"),
    ("left_join", """SELECT o.order_id AS k, p.paid FROM orders o
        LEFT JOIN payments p ON o.order_id = p.order_id""", "updating"),
    ("interval_join", """SELECT o.order_id AS k, p.paid FROM orders o
        JOIN payments p ON o.order_id = p.order_id AND p.pay_ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR""",
     "append"),
    ("temporal_join", """SELECT o.order_id AS k, c.tier FROM orders o
        JOIN customers FOR SYSTEM_TIME AS OF o.ts AS c ON o.customer_id = c.customer_id""", "append"),
    ("cdc_passthrough", "SELECT customer_id AS k, tier FROM customers", "updating"),
    ("dedup_first", """SELECT order_id AS k, amount FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ts ASC) AS rn FROM orders) WHERE rn = 1""",
     "append"),
    ("dedup_last", """SELECT order_id AS k, amount FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ts DESC) AS rn FROM orders) WHERE rn = 1""",
     "updating"),
    ("over_agg", """SELECT order_id AS k, SUM(amount) OVER (PARTITION BY customer_id ORDER BY ts
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS s FROM orders""", "append"),
]


@unittest.skipIf(FLINK is None, "set FLINK_HOME to run planner cross-checks")
class FlinkVerifyTest(unittest.TestCase):
    def test_example(self):
        r = compile_pipeline(EXAMPLE)
        v = run_verify(r.checked, r.lowered.sql, FLINK)
        self.assertTrue(v.ok, v.error or v.findings)
        self.assertEqual(v.sink_modes["ltv_to_kafka"], "I,UA")
        self.assertEqual(v.sink_modes["revenue_to_lake"], "I")
        self.assertEqual(v.findings, [])

    def test_static_changelog_matches_planner(self):
        mismatches = []
        for name, sql, expected in CASES:
            with self.subTest(case=name):
                r = one_model(sql, sink_mode="upsert")
                if "CD410" in r.diagnostics.codes():
                    r = one_model(sql, sink_mode="upsert", on_conflict="error")
                self.assertTrue(r.lowered is not None, [d.format() for d in r.diagnostics.items])
                static = r.checked.pipeline.relations["m"].changelog
                self.assertEqual(static, expected, f"{name}: static analysis")
                v = run_verify(r.checked, r.lowered.sql, FLINK)
                self.assertFalse(v.error, f"{name}: planner rejected SQL: {v.error}")
                mode = v.sink_modes.get("out", "")
                planner = "updating" if any(x in mode for x in ("UA", "UB", "D")) else "append"
                if planner != static:
                    mismatches.append(f"{name}: static={static} planner={planner} [{mode}]")
        self.assertEqual(mismatches, [])

    def test_on_conflict_required_by_planner(self):
        sql = "SELECT o.order_id AS k, p.paid FROM orders o LEFT JOIN payments p ON o.order_id = p.order_id"
        r = one_model(sql)
        self.assertIn("CD410", r.diagnostics.codes())
        v = run_verify(r.checked, r.lowered.sql, FLINK)
        self.assertIn("ON CONFLICT", v.error)
        r = one_model(sql, on_conflict="error")
        self.assertIn("ON CONFLICT DO ERROR", r.lowered.sql)
        self.assertTrue(run_verify(r.checked, r.lowered.sql, FLINK).ok)

    def test_planner_rejects_invalid_sql(self):
        r = one_model("SELECT order_id AS k, amount FROM orders WHERE no_such_col > 1", sink_mode="append")
        self.assertTrue(r.ok)  # statically fine: the analyzer does not resolve WHERE columns
        v = run_verify(r.checked, r.lowered.sql, FLINK)
        self.assertFalse(v.ok)
        self.assertIn("no_such_col", v.error)

    def test_append_sink_with_updates_is_caught_statically(self):
        r = compile_text("""
            defaults: {state_ttl: 1d}
            models:
              m: {sql: "SELECT o.order_id, p.paid FROM orders o LEFT JOIN payments p ON o.order_id = p.order_id"}
            sinks:
              out: {from: m, connection: print}
            """)
        self.assertIn("CD407", r.diagnostics.codes())


if __name__ == "__main__":
    unittest.main()
