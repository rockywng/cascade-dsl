"""Column-level lineage and OpenLineage events, from the Flink planner (needs FLINK_HOME and the built tools)."""
import os
import shutil
import tempfile
import unittest

from cascade_dsl.jvm import TARGET, find_flink_home
from cascade_dsl.lineage import column_lineage, openlineage_event

FLINK = find_flink_home()
READY = (FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))
         and bool([f for f in os.listdir(os.path.join(FLINK, "opt")) if f.startswith("flink-table-planner_2.12")]))

PIPELINE = """
pipeline: lp
defaults: {state_ttl: 1d}
connections:
  gen: {type: datagen}
  lake: {type: print}
sources:
  orders:
    connection: gen
    columns: {id: STRING NOT NULL, customer: STRING, amount: BIGINT, qty: BIGINT}
  customers:
    connection: gen
    columns: {customer: STRING NOT NULL, segment: STRING}
models:
  enriched:
    sql: SELECT o.id, o.customer, CAST(o.amount * o.qty AS BIGINT) AS worth, c.segment
         FROM orders o JOIN customers c ON o.customer = c.customer
  by_segment:
    sql: SELECT segment, CAST(SUM(worth) AS BIGINT) AS total, COUNT(*) AS n FROM enriched GROUP BY segment
sinks:
  out:
    from: by_segment
    connection: lake
    mode: upsert
    primary_key: [segment]
"""


@unittest.skipUnless(READY, "needs FLINK_HOME with opt/flink-table-planner and runtime/build.sh")
class ColumnLineageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="cascade-collineage-")
        with open(os.path.join(cls.dir, "pipeline.yaml"), "w") as f:
            f.write(PIPELINE)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir)

    def test_columns_trace_through_the_join_and_aggregate(self):
        out = column_lineage(self.dir, FLINK)["out"]
        self.assertEqual(out["segment"], {"derived": False, "unknown": False, "from": [("customers", "segment")]})
        self.assertTrue(out["total"]["derived"])
        self.assertEqual(sorted(out["total"]["from"]), [("orders", "amount"), ("orders", "qty")])
        self.assertEqual(out["n"]["from"], [])  # COUNT(*) reads no column

    def test_openlineage_event(self):
        ev = openlineage_event(self.dir, FLINK, run_id="r1", event_time="2026-01-01T00:00:00+00:00")
        self.assertEqual((ev["eventType"], ev["job"]["name"], ev["run"]["runId"]), ("COMPLETE", "lp", "r1"))
        self.assertEqual({i["name"] for i in ev["inputs"]}, {"orders", "customers"})
        (o,) = ev["outputs"]
        self.assertEqual(o["name"], "out")
        fields = o["facets"]["columnLineage"]["fields"]
        self.assertEqual(fields["segment"]["inputFields"][0]["field"], "segment")
        self.assertEqual(fields["segment"]["inputFields"][0]["transformations"][0]["subtype"], "IDENTITY")
        self.assertEqual({f["field"] for f in fields["total"]["inputFields"]}, {"amount", "qty"})
        self.assertEqual([f["name"] for f in o["facets"]["schema"]["fields"]], ["segment", "total", "n"])


if __name__ == "__main__":
    unittest.main()
