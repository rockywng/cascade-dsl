import os
import textwrap
import unittest

from cascade_dsl.api import Pipeline, count, expect, kafka, sum_
from cascade_dsl.builder import BuilderError
from cascade_dsl.compiler import compile_pipeline

from helpers import project

ROOT = os.path.join(os.path.dirname(__file__), "..", "examples")


def small_pipeline():
    p = Pipeline("shop", state_ttl="1d")
    bus = p.connection("bus", kafka(bootstrap_servers="localhost:9092"))
    orders = p.source("orders", bus, topic="orders", format="json",
                      columns={"order_id": "STRING NOT NULL", "customer_id": "STRING",
                               "amount": "DECIMAL(10, 2)", "ts": "TIMESTAMP(3)"},
                      event_time=("ts", "5s"))
    return p, bus, orders


class EquivalenceTest(unittest.TestCase):
    def test_python_and_yaml_compile_to_the_same_job(self):
        a = compile_pipeline(os.path.join(ROOT, "orders"))
        b = compile_pipeline(os.path.join(ROOT, "orders_py"))
        self.assertTrue(a.ok and b.ok)
        ma, mb = dict(a.lowered.manifest), dict(b.lowered.manifest)
        ma.pop("sql_sha256")
        mb.pop("sql_sha256")
        self.assertEqual(ma, mb)  # same graph, schemas, state ids, options, secrets

    def test_env_vars_and_secrets_work_in_python(self):
        r = compile_pipeline(os.path.join(ROOT, "orders_py"), env="prod")
        self.assertTrue(r.ok)
        self.assertIn("kafka/prod/bootstrap", r.lowered.manifest["secrets"])
        self.assertIn("s3://prod-lake/warehouse", r.lowered.sql)


class DiagnosticsTest(unittest.TestCase):
    def test_errors_point_at_python_lines(self):
        d = project("", base=False)
        with open(os.path.join(d, "pipeline.py"), "w") as f:
            f.write(textwrap.dedent('''
                from cascade_dsl.api import Pipeline, kafka
                p = Pipeline("x")
                bus = p.connection("bus", kafka(bootstrap_servers="b:9092"))
                orders = p.source("orders", bus, topic="o", columns={"id": "STRING", "n": "INT"})

                @p.model
                def counts(orders):
                    return f"SELECT id, COUNT(*) AS c FROM {orders} GROUP BY id"

                p.sink("out", counts, bus, topic="t")
            '''))
        os.remove(os.path.join(d, "pipeline.yaml"))
        r = compile_pipeline(d)
        by_code = {x.code: x for x in r.diagnostics.items}
        self.assertIn("CD350", by_code)  # unbounded state without TTL
        self.assertIn("CD407", by_code)  # append sink, updating input
        self.assertTrue(by_code["CD407"].location.file.endswith("pipeline.py"))
        self.assertEqual(by_code["CD407"].location.line, 11)
        self.assertEqual(by_code["CD350"].location.line, 7)

    def test_unknown_parameter(self):
        p, bus, orders = small_pipeline()

        @p.model
        def m(ordrs):
            return f"SELECT * FROM {ordrs}"
        p.sink("out", m, "print")
        r = p.compile()
        self.assertIn("CD302", r.diagnostics.codes())

    def test_builder_error_is_a_diagnostic(self):
        p, bus, orders = small_pipeline()

        @p.model
        def m(orders):
            return orders.where("amount > 0").tumble("ts", "1m").group_by().agg(n=count())
        p.sink("out", m, "print")
        r = p.compile()
        d = next(x for x in r.diagnostics.items if x.code == "CD304")
        self.assertIn("windows must be applied directly", d.message)

    def test_exception_in_pipeline_py(self):
        d = project("", base=False)
        os.remove(os.path.join(d, "pipeline.yaml"))
        with open(os.path.join(d, "pipeline.py"), "w") as f:
            f.write("from cascade_dsl.api import Pipeline\np = Pipeline('x')\nraise ValueError('boom')\n")
        r = compile_pipeline(d)
        self.assertEqual(r.diagnostics.items[0].location.line, 3)


class BuilderTest(unittest.TestCase):
    def sql(self, build):
        p, bus, orders = small_pipeline()
        p.source("customers", bus, topic="c", format="debezium-json",
                 columns={"customer_id": "STRING NOT NULL", "tier": "STRING", "updated_at": "TIMESTAMP(3)"},
                 primary_key=["customer_id"], event_time="updated_at")
        p.model(build, name="m")
        p.sink("out", "m", "print", mode="upsert", primary_key=["k"], on_conflict="error")
        r = p.compile()
        return r, r.checked.pipeline.models["m"].sql if r.checked else None

    def test_window_aggregate(self):
        r, sql = self.sql(lambda orders: orders.tumble("ts", "1m").group_by("customer_id")
                          .agg(total=sum_("amount"), n=count()))
        self.assertIn("TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE))", sql)
        self.assertIn("GROUP BY window_start, window_end, customer_id", sql)
        self.assertIn("SUM(amount) AS `total`", sql)
        rel = r.checked.pipeline.relations["m"]
        self.assertEqual(rel.changelog, "append")
        self.assertEqual({c.name for c in rel.columns},
                         {"window_start", "window_end", "customer_id", "total", "n"})

    def test_filter_then_select_flattens(self):
        _, sql = self.sql(lambda orders: orders.where("amount > 0").select("order_id", k="customer_id"))
        self.assertEqual(sql, "SELECT order_id, customer_id AS `k`\nFROM orders\nWHERE (amount > 0)")

    def test_dedup_uses_source_columns(self):
        r, sql = self.sql(lambda orders: orders.dedup("order_id", order_by="ts"))
        self.assertIn("SELECT order_id, customer_id, amount, ts\nFROM (", sql)
        self.assertIn("ORDER BY ts ASC) AS _rn", sql)
        self.assertEqual(r.checked.pipeline.relations["m"].changelog, "append")
        _, sql = self.sql(lambda orders: orders.dedup("order_id", order_by="ts", keep="last"))
        self.assertIn("ORDER BY ts DESC", sql)

    def test_temporal_join(self):
        r, sql = self.sql(lambda orders, customers: orders.temporal_join(
            customers, on=["customer_id"], as_of="ts").select("orders.order_id", "customers.tier"))
        self.assertIn("LEFT JOIN customers FOR SYSTEM_TIME AS OF orders.ts AS customers", sql)
        self.assertIn("temporal_join", {o.kind for o in r.checked.pipeline.relations["m"].stateful_ops})

    def test_interval_join(self):
        r, sql = self.sql(lambda orders, customers: orders.join(
            customers, on=["customer_id"], within=("ts", "updated_at", "1h", "10m"), how="inner")
            .select("orders.order_id"))
        self.assertIn("customers.updated_at BETWEEN orders.ts - INTERVAL '10' MINUTE AND "
                      "orders.ts + INTERVAL '1' HOUR", sql)

    def test_wrapping_is_deterministic(self):
        build = lambda orders: orders.group_by("customer_id").agg(n=count()).where("n > 1")  # noqa: E731
        _, a = self.sql(build)
        _, b = self.sql(build)
        self.assertEqual(a, b)
        self.assertIn(") AS q1\nWHERE (n > 1)", a)

    def test_bad_join_kind(self):
        p, bus, orders = small_pipeline()
        with self.assertRaises(BuilderError):
            orders.join(orders, on=["order_id"], how="sideways")


class ExpectationsInPythonTest(unittest.TestCase):
    def test_expectations_lower_to_metric_udf(self):
        p, bus, orders = small_pipeline()
        p.model(lambda orders: f"SELECT order_id, amount FROM {orders}", name="m",
                expectations=[expect.drop("pos", "amount > 0")])
        p.sink("out", "m", "print")
        r = p.compile()
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("CASCADE_EXPECT((amount > 0) IS TRUE, 'm', 'pos', 'drop')", r.lowered.sql)
        self.assertEqual(r.lowered.manifest["runtime_functions"], ["dev.cascade.runtime.ExpectFunction"])


if __name__ == "__main__":
    unittest.main()


with open(os.path.join(ROOT, "functions_py", "udfs", "reviews.py")) as _f:
    REVIEWS = _f.read()


class NewDeclarationsTest(unittest.TestCase):
    def test_function_kinds_and_ml_models(self):
        d = project("", base=False, files={
            "udfs/reviews.py": REVIEWS,
            "pipeline.py": '''
                from cascade_dsl.api import Pipeline, kafka
                p = Pipeline("shop_py", state_ttl="1d")
                bus = p.connection("bus", kafka(bootstrap_servers="localhost:9092"))
                p.source("reviews", bus, topic="reviews", key_fields=["product_id"],
                         columns={"product_id": "STRING NOT NULL", "rating": "DOUBLE", "votes": "DOUBLE",
                                  "tags": "STRING", "kts": {"type": "TIMESTAMP_LTZ(3)", "metadata": "timestamp"}})
                avg = p.python_aggregate("udfs/reviews.py:WeightedAvg", returns="DOUBLE",
                                         accumulator="ROW<total DOUBLE, weight DOUBLE>", name="wavg")
                p.java_function("split_tags", "com.acme.fn.SplitTags", kind="table", returns="ROW<tag STRING, pos INT>")
                p.ml_model("triage", provider="openai", input={"tags": "STRING"}, output={"queue": "STRING"},
                           properties={"endpoint": "https://e/v1/chat/completions", "model": "m",
                                       "api_key": "${secret:openai/key}"})
                p.sql_model("rated", "SELECT product_id, wavg(rating, votes) AS r FROM reviews GROUP BY product_id")
                p.sql_model("tagged", "SELECT r.product_id, t.tag FROM reviews AS r, LATERAL TABLE(split_tags(r.tags)) AS t")
                p.sql_model("routed", "SELECT product_id, queue FROM ML_PREDICT(TABLE reviews, MODEL triage, DESCRIPTOR(tags))")
                p.sink("o1", "rated", "print", mode="upsert", primary_key=["product_id"])
                p.sink("o2", "tagged", "print")
                p.sink("o3", "routed", "print")
                pipeline = p
            '''})
        r = compile_pipeline(os.path.join(d, "pipeline.py"))
        self.assertTrue(r.ok, [x.format() for x in r.diagnostics.items])
        sql = r.lowered.sql
        self.assertIn("CREATE TEMPORARY MODEL `triage`", sql)
        self.assertIn("`kts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'", sql)
        self.assertIn("'key.fields' = 'product_id'", sql)
        fns = r.checked.pipeline.functions
        self.assertEqual((fns["wavg"].kind, fns["wavg"].class_attr), ("aggregate", "WeightedAvg"))
        self.assertEqual(fns["split_tags"].kind, "table")
