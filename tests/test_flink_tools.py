"""Integration tests for the JVM tools: test runner, exact schemas, compiled plans and restore tests.
Require FLINK_HOME and the jars from runtime/build.sh; skipped otherwise."""
import json
import os
import shutil
import tempfile
import unittest

from cascade_dsl import testing
from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jvm import TARGET, find_flink_home
from cascade_dsl.plans import UID_KEY, compile_plan, operator_map_for, restore_test
from cascade_dsl.schemas import infer
from cascade_dsl.verify import run_verify

from helpers import project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))
EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples")


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class RunnerTest(unittest.TestCase):
    def test_examples_pass(self):
        for name in ("orders", "orders_py", "clickstream_py", "functions", "functions_py"):
            with self.subTest(example=name):
                r, results = testing.run(os.path.join(EXAMPLES, name), FLINK)
                self.assertIsNotNone(results, [d.format() for d in r.diagnostics.errors])
                self.assertTrue(results)
                for t in results:
                    self.assertTrue(t.passed, f"{t.case.name}: {t.message}")

    def test_failures_are_reported(self):
        d = project("""
            defaults: {state_ttl: 1d}
            models:
              capped:
                sql: SELECT order_id, amount FROM orders
                expectations:
                  - {name: below_limit, check: amount < 1000, on_violation: fail}
            sinks:
              out: {from: capped, connection: print}
            """, files={"tests/t.yaml": """
            tests:
              - name: big_order_fails_the_job
                target: capped
                given:
                  orders: [{order_id: o1, amount: 5000, ts: "2026-01-01 00:00:00"}]
                expect: {error: below_limit}
              - name: wrong_expectation_shows_a_diff
                target: capped
                given:
                  orders: [{order_id: o1, amount: 10, ts: "2026-01-01 00:00:00"}]
                expect:
                  rows: [{order_id: o1, amount: 11}]
              - name: bad_fixture_column
                target: capped
                given:
                  orders: [{order_id: o1, amont: 10}]
                expect: {empty: true}
            """})
        _, results = testing.run(d, FLINK)
        by_name = {t.case.name: t for t in results}
        self.assertTrue(by_name["big_order_fails_the_job"].passed, by_name["big_order_fails_the_job"].message)
        diff = by_name["wrong_expectation_shows_a_diff"]
        self.assertFalse(diff.passed)
        self.assertIn("{order_id: 'o1', amount: 11}", diff.message)
        self.assertIn("{order_id: 'o1', amount: 10}", diff.message)
        self.assertIn("unknown column(s) ['amont']", by_name["bad_fixture_column"].message)
        shutil.rmtree(d)


LOOKUP = """
pipeline: lookups
defaults: {state_ttl: 1d}
scaling: {max_parallelism: 360}
connections:
  bus: {type: kafka, properties: {bootstrap_servers: "b:9092"}}
  crm: {type: jdbc, properties: {url: "jdbc:postgresql://crm/app", password: "${secret:crm/pw}"}}
sources:
  orders:
    connection: bus
    topic: orders
    columns: {order_id: STRING NOT NULL, customer_id: STRING, amount: "DECIMAL(10, 2)"}
    processing_time: proc_time
  customers:
    connection: crm
    table: customers
    columns: {customer_id: STRING NOT NULL, tier: STRING}
    primary_key: [customer_id]
models:
  enriched:
    sql: |
      SELECT o.order_id, o.customer_id, COALESCE(c.tier, 'none') AS tier, o.amount
      FROM orders AS o
      LEFT JOIN customers FOR SYSTEM_TIME AS OF o.proc_time AS c
        ON o.customer_id = c.customer_id
  spend:
    sql: SELECT customer_id, SUM(amount) AS total FROM enriched GROUP BY customer_id
sinks:
  enriched_out: {from: enriched, connection: print}
  spend_by_tier:
    from: spend
    connection: print
    mode: upsert
    primary_key: [customer_id]
"""


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class FixtureConnectorTest(unittest.TestCase):
    """Lookup joins served from fixture rows, and sinks as test targets."""

    def test_lookup_join_and_sink_targets(self):
        d = project(LOOKUP, base=False, files={"tests/t.yaml": """
            tests:
              - name: lookup_uses_the_table_rows
                target: enriched
                given:
                  customers:
                    - {customer_id: c1, tier: gold}
                    - {customer_id: c2, tier: silver}
                  orders:
                    - {order_id: o1, customer_id: c1, amount: 10}
                    - {order_id: o2, customer_id: c3, amount: 5}
                expect:
                  rows:
                    - {order_id: o1, tier: gold}
                    - {order_id: o2, tier: none}
              - name: upsert_sink_final_table
                target: spend_by_tier
                given:
                  orders:
                    - {order_id: o1, customer_id: c1, amount: 10}
                    - {order_id: o2, customer_id: c1, amount: 5}
                    - {order_id: o3, customer_id: c2, amount: 1}
                expect:
                  rows:
                    - {customer_id: c1, total: 15}
                    - {customer_id: c2, total: 1}
              - name: append_sink_target
                target: enriched_out
                given:
                  orders: [{order_id: o1, customer_id: c1, amount: 10}]
                expect:
                  rows: [{order_id: o1, tier: none}]
            """})
        r, results = testing.run(d, FLINK)
        self.assertIsNotNone(results, [x.format() for x in r.diagnostics.errors])
        for t in results:
            self.assertTrue(t.passed, f"{t.case.name}: {t.message}")
        shutil.rmtree(d)

    def test_on_conflict_policies(self):
        # upsert key (order_id, customer_id) differs from the sink key (customer_id): two orders of one
        # customer conflict. ERROR fails the job; NOTHING keeps the first row.
        def proj(policy):
            return project(f"""
                defaults: {{state_ttl: 1d}}
                models:
                  m:
                    sql: |
                      SELECT order_id, customer_id, SUM(amount) AS total
                      FROM orders GROUP BY order_id, customer_id
                sinks:
                  by_customer:
                    from: m
                    connection: print
                    mode: upsert
                    primary_key: [customer_id]
                    on_conflict: {policy}
                """, files={"tests/t.yaml": """
                tests:
                  - name: conflict
                    target: by_customer
                    given:
                      orders:
                        - {order_id: o1, customer_id: c1, amount: 10, ts: "2026-01-01 10:00:00"}
                        - {order_id: o2, customer_id: c1, amount: 5, ts: "2026-01-01 10:01:00"}
                    expect:
                      """ + ("{error: Primary key constraint violation}" if policy == "error" else
                             "{rows: [{customer_id: c1, total: 10}]}")})
        for policy in ("error", "nothing"):
            with self.subTest(policy=policy):
                d = proj(policy)
                r, results = testing.run(d, FLINK)
                self.assertIsNotNone(results, [x.format() for x in r.diagnostics.errors])
                self.assertTrue(results[0].passed, results[0].message)
                shutil.rmtree(d)


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class InferTest(unittest.TestCase):
    def test_infer_resolves_untyped_expressions(self):
        d = project("""
            models:
              m:
                sql: SELECT order_id, amount * 2 AS doubled, CONCAT(order_id, '-x') AS tag FROM orders
            sinks:
              out: {from: m, connection: print}
            """)
        self.assertIn("CD408", compile_pipeline(d).diagnostics.codes())
        doc, path = infer(d, FLINK)
        types = {c["name"]: c["type"] for c in doc["models"]["m"]["columns"]}
        self.assertTrue(types["doubled"].startswith("DECIMAL("), types)
        r = compile_pipeline(d)
        self.assertTrue(r.ok, [x.format() for x in r.diagnostics.items])
        self.assertIn(f"`doubled` {types['doubled'].replace(' NOT NULL', '')}", r.lowered.sql)
        self.assertTrue(run_verify(r.checked, r.lowered, FLINK).ok)
        shutil.rmtree(d)


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class PlanAndRestoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="cascade-restore-test-")
        cls.new = os.path.join(cls.dir, "new")
        shutil.copytree(os.path.join(EXAMPLES, "orders"), cls.new)
        path = os.path.join(cls.new, "pipeline.yaml")
        with open(path) as f:
            text = f.read()
        # v2 adds a new aggregate + sink, which renumbers the plan's nodes
        text = text.replace("sinks:\n  revenue_to_lake:", "  orders_per_currency:\n"
                            "    sql: SELECT currency, COUNT(*) AS n FROM clean_orders GROUP BY currency\n\n"
                            "sinks:\n  currency_counts:\n    from: orders_per_currency\n    connection: print\n"
                            "    mode: upsert\n    primary_key: [currency]\n  revenue_to_lake:")
        with open(path, "w") as f:
            f.write(text)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_plan_pinning_keeps_uids(self):
        old = os.path.join(self.dir, "old.json")
        new = os.path.join(self.dir, "new.json")
        compile_plan(os.path.join(EXAMPLES, "orders"), FLINK, old, local=True, workdir=os.path.join(self.dir, "w1"))
        _, report, setup = compile_plan(self.new, FLINK, new, local=True, previous=old,
                                        workdir=os.path.join(self.dir, "w2"))
        self.assertEqual(report.dropped_stateful, [])
        self.assertEqual(len(report.new_stateful), 2)  # new aggregate and its sink
        with open(new) as f:
            pinned = json.load(f)
        self.assertTrue(all(UID_KEY in n["configuration"] for n in pinned["nodes"]))
        self.assertEqual({s.kind for s in setup}, {"set", "function", "source", "sink", "quarantine"})

    def test_restore_needs_pinning(self):
        unpinned = restore_test(os.path.join(EXAMPLES, "orders"), self.new, FLINK, pin=False, seconds=4)
        self.assertFalse(unpinned["restored"])
        self.assertIn("Cannot map checkpoint/savepoint state", unpinned.get("error", ""))
        pinned = restore_test(os.path.join(EXAMPLES, "orders"), self.new, FLINK, pin=True, seconds=4)
        self.assertTrue(pinned["restored"], pinned.get("error"))


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class OperatorMapTest(unittest.TestCase):
    def ops(self, example):
        d = tempfile.mkdtemp(prefix="cascade-ops-")
        plan = os.path.join(d, "plan.json")
        r, _, setup = compile_plan(os.path.join(EXAMPLES, example), FLINK, plan, local=True, workdir=d)
        m = operator_map_for(r, setup, plan, FLINK)
        shutil.rmtree(d)
        return m

    def by_operator(self, m):
        return {op["name"].split("[")[0]: op for v in m["vertices"] for op in v["operators"]
                if not op["name"].startswith(("Calc", "Exchange", "MiniBatch", "Watermark", "Source"))}

    def test_orders(self):
        m = self.ops("orders")
        ops = self.by_operator(m)
        self.assertEqual(ops["TemporalJoin"]["owner"], {"kind": "model", "name": "orders_enriched"})
        self.assertEqual(ops["GlobalWindowAggregate"]["owner"]["name"], "revenue_per_minute")
        self.assertEqual(ops["LocalWindowAggregate"]["owner"]["name"], "revenue_per_minute")
        self.assertEqual(ops["GlobalGroupAggregate"]["owner"]["name"], "customer_ltv")
        # clean_orders is stateless: its filter/projection is fused into Calcs feeding the join and the LTV
        calcs = [op for v in m["vertices"] for op in v["operators"] if op["name"].startswith("Calc")]
        self.assertTrue(any(op["models"] == ["clean_orders"] for op in calcs))
        for name in ("clean_orders", "orders_enriched", "revenue_per_minute", "customer_ltv"):
            self.assertTrue(m["models"][name]["vertices"], name)
        self.assertEqual(m["fused_away"], [])
        self.assertEqual(m["model_scaling"], {"customer_ltv": {"min": 2, "max": None}})
        self.assertTrue(all(v["max_parallelism"] == 720 for v in m["vertices"]))

    def test_clickstream(self):
        ops = self.by_operator(self.ops("clickstream_py"))
        self.assertEqual(ops["Deduplicate"]["owner"]["name"], "unique_views")
        self.assertEqual(ops["IntervalJoin"]["owner"]["name"], "attributed_purchases")
        self.assertEqual(ops["GroupAggregate"]["owner"]["name"], "revenue_by_url")


if __name__ == "__main__":
    unittest.main()
