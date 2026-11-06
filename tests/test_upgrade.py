import os
import re
import shutil
import tempfile
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.upgrade import diff_manifests

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "examples", "orders")


class UpgradeTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="cascade-upgrade-")
        shutil.copytree(EXAMPLE, self.dir, dirs_exist_ok=True)
        self.old = self._manifest()

    def _manifest(self):
        r = compile_pipeline(self.dir)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        return r.lowered.manifest

    def _edit(self, rel, old, new):
        path = os.path.join(self.dir, rel)
        with open(path) as f:
            text = f.read()
        self.assertIn(old, text)
        with open(path, "w") as f:
            f.write(text.replace(old, new))

    def test_identical_is_ok(self):
        self.assertEqual(diff_manifests(self.old, self._manifest())[0], "ok")

    def test_whitespace_and_comments_do_not_matter(self):
        self._edit("models/revenue_per_minute.sql", "SUM(amount) AS revenue,",
                   "sum(amount)   AS revenue,  -- total revenue")
        self.assertEqual(diff_manifests(self.old, self._manifest())[0], "ok")

    def test_changing_aggregation_loses_its_state(self):
        self._edit("pipeline.yaml", "COUNT(*) AS order_count", "COUNT(DISTINCT order_id) AS order_count")
        verdict, changes = diff_manifests(self.old, self._manifest())
        self.assertEqual(verdict, "state_loss")
        incompatible = {c.node for c in changes if c.code == "STATE_INCOMPATIBLE"}
        self.assertEqual(incompatible, {"customer_ltv"})  # other stateful nodes are untouched

    def test_upstream_filter_change_is_semantic_drift(self):
        self._edit("pipeline.yaml", "check: amount > 0", "check: amount > 1")
        verdict, changes = diff_manifests(self.old, self._manifest())
        drift = {c.node for c in changes if c.code == "SEMANTIC_DRIFT"}
        self.assertEqual(drift, {"customer_ltv", "orders_enriched", "revenue_per_minute"})
        self.assertEqual(verdict, "warning")

    def test_topic_change_resets_source_offsets(self):
        self._edit("pipeline.yaml", "topic: orders.v1", "topic: orders.v2")
        verdict, changes = diff_manifests(self.old, self._manifest())
        self.assertEqual(verdict, "state_loss")
        self.assertIn(("STATE_INCOMPATIBLE", "orders"), {(c.code, c.node) for c in changes})

    def test_removing_sink_column_is_breaking(self):
        self._edit("models/revenue_per_minute.sql", "  COUNT(*) AS orders\n", "")
        self._edit("models/revenue_per_minute.sql", "SUM(amount) AS revenue,", "SUM(amount) AS revenue")
        verdict, changes = diff_manifests(self.old, self._manifest())
        self.assertEqual(verdict, "breaking")
        self.assertTrue(any(c.code == "SINK_SCHEMA_CHANGED" and "removed ['orders']" in c.message for c in changes))

    def test_new_stateful_model_is_reported(self):
        with open(os.path.join(self.dir, "pipeline.yaml")) as f:
            text = f.read()
        text = re.sub(r"\nsinks:\n", "\n  big_spenders:\n    sql: SELECT customer_id, lifetime_value FROM customer_ltv "
                      "WHERE lifetime_value > 1000\n\nsinks:\n", text, count=1)
        text += ("\n  big_spenders_out:\n    from: big_spenders\n    connection: events\n    topic: big\n"
                 "    mode: upsert\n    primary_key: [customer_id]\n")
        text = text.replace("\n# Where", "\n# Where", 1)
        # move the appended sink above `quarantine:` (YAML key order does not matter, nesting does)
        sink_block = text[text.index("\n  big_spenders_out:"):]
        text = text[:text.index("\n  big_spenders_out:")]
        text = text.replace("\nquarantine:", sink_block.rstrip("\n") + "\n\nquarantine:", 1)
        with open(os.path.join(self.dir, "pipeline.yaml"), "w") as f:
            f.write(text)
        verdict, changes = diff_manifests(self.old, self._manifest())
        self.assertIn(("SINK_ADDED", "big_spenders_out"), {(c.code, c.node) for c in changes})
        self.assertEqual(verdict, "warning")


if __name__ == "__main__":
    unittest.main()
