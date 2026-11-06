"""backfill: true -- a model compiled over its own copies of its sources, read from the beginning."""
import os
import shutil
import unittest

from cascade_dsl import testing
from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jvm import TARGET, find_flink_home

from helpers import project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))

PIPE = """
defaults: {state_ttl: 1d}
models:
  clean:
    sql: SELECT order_id, customer_id, amount, ts FROM orders
    expectations:
      - {name: positive, check: amount > 0, on_violation: drop}
  enriched:
    sql: |
      SELECT o.order_id, o.amount, o.ts, COALESCE(c.tier, 'none') AS tier
      FROM clean AS o
      LEFT JOIN customers FOR SYSTEM_TIME AS OF o.ts AS c ON o.customer_id = c.customer_id
  spend_by_tier:
    backfill: true
    sql: SELECT tier, SUM(amount) AS total FROM enriched GROUP BY tier
  big:
    sql: SELECT order_id FROM clean WHERE amount > 100
sinks:
  by_tier: {from: spend_by_tier, connection: print, mode: upsert, primary_key: [tier]}
  big_out: {from: big, connection: print}
"""


class BackfillCompileTest(unittest.TestCase):
    def setUp(self):
        self.root = project(PIPE)
        self.r = compile_pipeline(self.root)
        self.assertTrue(self.r.ok, [d.format() for d in self.r.diagnostics.items])
        self.p = self.r.checked.pipeline

    def tearDown(self):
        shutil.rmtree(self.root)

    def test_copies_the_upstream_closure(self):
        self.assertEqual({n for n in self.p.sources if "__bf_" in n},
                         {"orders__bf_spend_by_tier", "customers__bf_spend_by_tier"})
        self.assertEqual({n for n in self.p.models if "__bf_" in n},
                         {"clean__bf_spend_by_tier", "enriched__bf_spend_by_tier"})
        self.assertEqual(self.p.relations["spend_by_tier"].inputs, ["enriched__bf_spend_by_tier"])
        # untouched models keep reading the shared sources
        self.assertEqual(self.p.relations["big"].inputs, ["clean"])
        self.assertEqual(self.p.relations["clean"].inputs, ["orders"])

    def test_copies_start_from_the_beginning_with_their_own_positions(self):
        opts = self.r.checked.options
        self.assertEqual(opts["orders__bf_spend_by_tier"]["scan.startup.mode"], "earliest-offset")
        self.assertNotEqual(opts["orders__bf_spend_by_tier"]["properties.group.id"],
                            opts["orders"]["properties.group.id"])
        self.assertEqual(opts["customers__bf_spend_by_tier"]["scan.startup.mode"], "initial")
        self.assertEqual(opts["customers__bf_spend_by_tier"]["slot.name"], "test_pipe_customers__bf_spend_by_tier")
        self.assertNotEqual(opts["customers__bf_spend_by_tier"]["slot.name"], opts["customers"]["slot.name"])

    def test_expectations_are_kept_and_reported(self):
        self.assertEqual([(e.name, e.on_violation) for e in self.p.models["clean__bf_spend_by_tier"].expectations],
                         [("positive", "drop")])
        info = next(d for d in self.r.diagnostics.items if d.code == "CD370")
        self.assertIn("orders, customers", info.message)
        self.assertIn("recomputes clean, enriched", info.message)
        m = self.r.lowered.manifest["nodes"]
        self.assertEqual(m["orders__bf_spend_by_tier"]["backfill_copy_of"], "orders")
        self.assertIsNone(m["orders"]["backfill_copy_of"])

    def test_cdc_temporal_join_with_idle_timeout_warns(self):
        self.assertNotIn("CD372", self.r.diagnostics.codes())  # no idle timeout configured
        root = project(PIPE + "flink:\n  config:\n    table.exec.source.idle-timeout: 2s\n")
        r = compile_pipeline(root)
        d = next(x for x in r.diagnostics.items if x.code == "CD372")
        self.assertIn("customers", d.message)
        shutil.rmtree(root)

    def test_explicit_group_id_gets_a_backfill_suffix(self):
        root = project(PIPE.replace("defaults:", "defaults:", 1), files=None)
        path = os.path.join(root, "pipeline.yaml")
        with open(path) as f:
            text = f.read()
        with open(path, "w") as f:
            f.write(text.replace("    topic: orders\n", "    topic: orders\n    group_id: shop\n", 1))
        r = compile_pipeline(root)
        self.assertEqual(r.checked.options["orders__bf_spend_by_tier"]["properties.group.id"],
                         "shop.backfill.spend_by_tier")
        shutil.rmtree(root)


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class BackfillFixtureTest(unittest.TestCase):
    def test_backfilled_model_sees_the_same_fixture_input(self):
        d = project(PIPE, files={"tests/t.yaml": """
            tests:
              - name: spend_per_tier
                target: by_tier
                given:
                  customers:
                    - {customer_id: c1, tier: gold, updated_at: "2026-01-01 09:00:00"}
                  orders:
                    - {order_id: o1, customer_id: c1, amount: 10, ts: "2026-01-01 10:00:00"}
                    - {order_id: o2, customer_id: c2, amount: 5, ts: "2026-01-01 10:00:01"}
                    - {order_id: o3, customer_id: c1, amount: -1, ts: "2026-01-01 10:00:02"}
                expect:
                  rows:
                    - {tier: gold, total: 10}
                    - {tier: none, total: 5}
            """})
        r, results = testing.run(d, FLINK)
        self.assertIsNotNone(results, [x.format() for x in r.diagnostics.errors])
        self.assertTrue(results[0].passed, results[0].message)
        shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
