"""from_pipeline: a source that reads another pipeline's sink."""
import os
import shutil
import tempfile
import unittest

from cascade_dsl.compiler import compile_pipeline

UP = """
pipeline: shop
defaults: {state_ttl: 1d}
connections:
  bus: {type: kafka, properties: {bootstrap_servers: "k:9092", sasl_jaas_config: "${secret:kafka/jaas}"}}
sources:
  orders:
    connection: bus
    topic: orders
    columns: {order_id: STRING, customer_id: STRING, amount: "DECIMAL(10, 2)", ts: TIMESTAMP(3)}
    event_time: {column: ts, max_out_of_orderness: 5s}
models:
  ltv:
    sql: SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
  big:
    sql: SELECT order_id, amount, ts FROM orders WHERE amount > 100
sinks:
  ltv_out: {from: ltv, connection: bus, topic: ltv, mode: upsert, primary_key: [customer_id]}
  big_out: {from: big, connection: bus, topic: big, delivery: exactly-once}
  dev: {from: big, connection: print}
"""

DOWN = """
pipeline: marketing
defaults: {state_ttl: 1d}
connections:
  bus: {type: kafka, properties: {bootstrap_servers: "k:9092", sasl_jaas_config: "${secret:kafka/jaas}"}}
sources:
  SRC
models:
  vip:
    sql: SELECT customer_id FROM ltv WHERE total > 1000
sinks:
  out: {from: vip, connection: print, mode: upsert, primary_key: [customer_id]}
"""


class ImportTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="cascade-imports-")
        self.write("shop", UP)

    def tearDown(self):
        shutil.rmtree(self.root)

    def write(self, name, text):
        os.makedirs(os.path.join(self.root, name), exist_ok=True)
        with open(os.path.join(self.root, name, "pipeline.yaml"), "w") as f:
            f.write(text)
        return os.path.join(self.root, name)

    def down(self, src: str):
        return compile_pipeline(self.write("marketing", DOWN.replace("SRC", src)))

    def test_reads_an_upsert_sink_as_a_keyed_changelog(self):
        r = self.down("ltv: {from_pipeline: ../shop, sink: ltv_out}")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        o = r.checked.options["ltv"]
        self.assertEqual((o["connector"], o["topic"], o["key.format"]), ("upsert-kafka", "ltv", "json"))
        s = r.checked.pipeline.sources["ltv"]
        self.assertEqual(s.primary_key, ["customer_id"])
        self.assertEqual([c.name for c in s.columns], ["customer_id", "total"])
        self.assertEqual(r.checked.pipeline.relations["ltv"].changelog, "updating")
        self.assertEqual(s.connection, "bus")  # identical connection reused
        self.assertEqual(r.lowered.manifest["imports"],
                         [{"source": "ltv", "pipeline": "shop", "sink": "ltv_out", "path": "../shop"}])

    def test_exactly_once_sink_is_read_committed_and_overrides_apply(self):
        r = self.down("big:\n    from_pipeline: ../shop\n    sink: big_out\n    startup: earliest\n"
                      "    event_time: {column: ts, max_out_of_orderness: 1s}")
        r2 = compile_pipeline(self.write("m2", DOWN.replace("SRC", "big:\n    from_pipeline: ../shop\n    sink: big_out\n"
                                                                 "    startup: earliest\n    event_time: ts")
                                         .replace("SELECT customer_id FROM ltv WHERE total > 1000", "SELECT order_id AS customer_id FROM big")))
        self.assertTrue(r2.ok, [d.format() for d in r2.diagnostics.items])
        o = r2.checked.options["big"]
        self.assertEqual((o["properties.isolation.level"], o["scan.startup.mode"]), ("read_committed", "earliest-offset"))

    def test_a_new_connection_is_brought_in(self):
        r = compile_pipeline(self.write("m3", DOWN.replace("k:9092", "other:9092").replace(
            "SRC", "ltv: {from_pipeline: ../shop, sink: ltv_out}")))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertEqual(r.checked.pipeline.sources["ltv"].connection, "bus__shop")
        self.assertIn("kafka/jaas", r.checked.pipeline.secrets)

    def test_errors(self):
        self.assertIn("CD132", self.down("ltv: {from_pipeline: ../shop, sink: nope}").diagnostics.codes())
        self.assertIn("CD133", self.down("ltv: {from_pipeline: ../shop, sink: dev}").diagnostics.codes())
        self.assertIn("CD131", self.down("ltv: {from_pipeline: ../nowhere, sink: x}").diagnostics.codes())
        self.assertIn("CD005", self.down("ltv: {from_pipeline: ../shop, sink: ltv_out, connection: bus}")
                      .diagnostics.codes())

    def test_cycle(self):
        self.write("shop", UP.replace("  orders:\n", "  loop: {from_pipeline: ../marketing, sink: out}\n  orders:\n"))
        r = self.down("ltv: {from_pipeline: ../shop, sink: ltv_out}")
        self.assertTrue(any(c in r.diagnostics.codes() for c in ("CD130", "CD131")))


if __name__ == "__main__":
    unittest.main()
