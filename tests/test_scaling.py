"""Scaling block: validation, lowering, manifest and upgrade checks."""
import os
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.upgrade import diff_manifests

from helpers import BASE, compile_text, project

AGG = """
defaults: {state_ttl: 1d}
models:
  m:
    sql: SELECT customer_id AS k, COUNT(*) AS n FROM orders GROUP BY customer_id
sinks:
  out: {from: m, connection: print, mode: upsert, primary_key: [k]}
"""


def codes(r):
    return [d.code for d in r.diagnostics.items]


class ScalingTest(unittest.TestCase):
    def test_keyed_state_without_max_parallelism_warns(self):
        r = compile_text(AGG)
        self.assertIn("CD520", codes(r))

    def test_stateless_pipeline_does_not_warn(self):
        r = compile_text("models:\n  m: {sql: SELECT order_id FROM orders}\n"
                         "sinks:\n  out: {from: m, connection: print}\n")
        self.assertNotIn("CD520", codes(r))

    def test_lowering_and_manifest(self):
        r = compile_text(AGG + """
scaling:
  max_parallelism: 360
  parallelism: 4
  min: 2
  max: 60
  target_utilization: 0.6
  max_lag: 30s
""")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("SET 'pipeline.max-parallelism' = '360';", r.lowered.sql)
        self.assertIn("SET 'parallelism.default' = '4';", r.lowered.sql)
        sc = r.lowered.manifest["scaling"]
        self.assertEqual(sc["max_lag_ms"], 30_000)
        self.assertEqual(sc["aligned_parallelisms"], [2, 3, 4, 5, 6, 8, 9, 10, 12, 15, 18, 20, 24, 30, 36, 40, 45, 60])

    def test_bounds_are_checked(self):
        for block, needle in (("{max_parallelism: 100, max: 200}", "exceeds"),
                              ("{min: 8, max: 4}", "greater than"),
                              ("{max_parallelism: 128, parallelism: 300}", "outside"),
                              ("{max_parallelism: 0}", "between"),
                              ("{target_utilization: 1.5}", "(0, 1]")):
            with self.subTest(block=block):
                r = compile_text(AGG + f"scaling: {block}\n")
                d = [x for x in r.diagnostics.items if x.code == "CD521"]
                self.assertTrue(d, codes(r))
                self.assertIn(needle, d[0].message)

    def test_unknown_key(self):
        r = compile_text(AGG + "scaling: {max_paralelism: 720}\n")
        d = next(x for x in r.diagnostics.items if x.code == "CD001")
        self.assertIn("max_parallelism", d.hint)

    def test_prime_max_parallelism_warns(self):
        r = compile_text(AGG + "scaling: {max_parallelism: 127, max: 64}\n")
        self.assertIn("CD522", codes(r))
        r = compile_text(AGG + "scaling: {max_parallelism: 720, max: 64}\n")
        self.assertNotIn("CD522", codes(r))

    def test_flink_config_cannot_bypass(self):
        r = compile_text(AGG + "scaling: {max_parallelism: 720}\n"
                         "flink:\n  config:\n    pipeline.max-parallelism: '128'\n")
        self.assertIn("CD521", codes(r))

    def test_per_model_bounds(self):
        text = AGG.replace("    sql: SELECT", "    scaling: {min: 2, max: 100}\n    sql: SELECT")
        r = compile_text(text + "scaling: {max_parallelism: 720, max: 64}\n")
        d = next(x for x in r.diagnostics.items if x.code == "CD521")
        self.assertIn("model 'm'", d.message)
        r = compile_text(text.replace("max: 100", "max: 32") + "scaling: {max_parallelism: 720, max: 64}\n")
        self.assertTrue(r.ok)
        self.assertEqual(r.lowered.manifest["scaling"]["models"], {"m": {"min": 2, "max": 32}})


class ScalingUpgradeTest(unittest.TestCase):
    def manifest(self, scaling):
        r = compile_text(AGG + (f"scaling: {scaling}\n" if scaling else ""))
        return r.lowered.manifest

    def test_changing_max_parallelism_is_breaking(self):
        verdict, changes = diff_manifests(self.manifest("{max_parallelism: 720}"),
                                          self.manifest("{max_parallelism: 360}"))
        self.assertEqual(verdict, "breaking")
        self.assertEqual(changes[0].code, "MAX_PARALLELISM_CHANGED")

    def test_pinning_later_warns_with_derived_value(self):
        verdict, changes = diff_manifests(self.manifest(None), self.manifest("{max_parallelism: 720}"))
        self.assertEqual(verdict, "warning")
        self.assertIn("128", changes[0].message)

    def test_parallelism_change_is_fine(self):
        verdict, _ = diff_manifests(self.manifest("{max_parallelism: 720, parallelism: 2}"),
                                    self.manifest("{max_parallelism: 720, parallelism: 8}"))
        self.assertEqual(verdict, "ok")


class OnConflictWatermarkTest(unittest.TestCase):
    """ON CONFLICT DO ERROR / NOTHING resolve conflicts on watermarks: every scanned source needs one."""

    RAW = ("  payments_raw:\n    connection: bus\n    topic: p\n"
           "    columns: {order_id: STRING, paid: \"DECIMAL(10, 2)\"}\n")

    def compile(self, payments: str, policy: str):
        body = BASE.replace("  customers:\n", self.RAW + "  customers:\n", 1) + f"""
defaults: {{state_ttl: 1d}}
models:
  m:
    sql: SELECT o.order_id, o.customer_id, SUM(p.paid) AS paid FROM orders AS o JOIN {payments} AS p
         ON o.order_id = p.order_id GROUP BY o.order_id, o.customer_id
sinks:
  out: {{from: m, connection: print, mode: upsert, primary_key: [customer_id], on_conflict: {policy}}}
"""
        return codes(compile_pipeline(project(body, base=False)))

    def test_sources_need_event_time(self):
        self.assertNotIn("CD412", self.compile("payments", "error"))
        self.assertIn("CD412", self.compile("payments_raw", "error"))
        self.assertIn("CD412", self.compile("payments_raw", "nothing"))
        self.assertNotIn("CD412", self.compile("payments_raw", "deduplicate"))


class ScalingExamplesTest(unittest.TestCase):
    def test_python_example_scaling(self):
        r = compile_pipeline(os.path.join(os.path.dirname(__file__), "..", "examples", "orders_py"))
        self.assertEqual(r.lowered.manifest["scaling"]["models"], {"customer_ltv": {"min": 2, "max": None}})


if __name__ == "__main__":
    unittest.main()
