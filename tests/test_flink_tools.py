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
from cascade_dsl.plans import UID_KEY, compile_plan, restore_test
from cascade_dsl.schemas import infer
from cascade_dsl.verify import run_verify

from helpers import project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))
EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples")


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class RunnerTest(unittest.TestCase):
    def test_examples_pass(self):
        for name in ("orders", "orders_py", "clickstream_py"):
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


if __name__ == "__main__":
    unittest.main()
