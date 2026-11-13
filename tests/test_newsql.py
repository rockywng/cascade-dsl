"""Flink 2.x SQL: VARIANT and ML_PREDICT models."""
import os
import shutil
import unittest

from cascade_dsl import testing
from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jvm import TARGET, find_flink_home
from cascade_dsl.plans import PlanError, compile_plan, restore_test, uses_variant

from helpers import compile_text, project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))
EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples")

VARIANT = """
pipeline: v
connections:
  k: {type: kafka, properties: {bootstrap_servers: "b:9092"}}
sources:
  ev: {connection: k, topic: a, columns: {id: STRING, doc: VARIANT, payload: STRING}}
models:
  m:
    sql: |
      SELECT id, doc, JSON_STRING(doc['kind']) AS kind, TRY_PARSE_JSON(payload) AS parsed FROM ev
sinks:
  out: {from: m, connection: k, topic: b}
"""


def errors(r):
    return sorted(d.code for d in r.diagnostics.items if d.severity == "error")


class VariantTest(unittest.TestCase):
    def test_variant_columns_and_functions_are_typed(self):
        r = compile_text(VARIANT, base=False)
        self.assertEqual(errors(r), [], [d.format() for d in r.diagnostics.items])
        cols = {c.name: c.type.text for c in r.checked.pipeline.relations["m"].columns}
        self.assertEqual(cols, {"id": "STRING", "doc": "VARIANT", "kind": "STRING", "parsed": "VARIANT"})
        self.assertTrue(uses_variant(r.checked.pipeline))


class MLModelTest(unittest.TestCase):
    def test_example_compiles_to_a_temporary_model(self):
        r = compile_pipeline(os.path.join(EXAMPLES, "ml_predict"))
        self.assertEqual(errors(r), [], [d.format() for d in r.diagnostics.items])
        self.assertIn("CREATE TEMPORARY MODEL `triage`\nINPUT (`body` STRING)\nOUTPUT (`queue` STRING)",
                      r.lowered.sql)
        self.assertIn("'api-key' = '${secret:openai/api_key}'", r.lowered.sql)
        self.assertIn("org.apache.flink:flink-model-openai", r.lowered.manifest["artifacts"])
        cols = [c.name for c in r.checked.pipeline.relations["routed"].columns]
        self.assertEqual(cols, ["ticket_id", "queue", "ts"])
        self.assertEqual(r.checked.pipeline.relations["routed"].inputs, ["tickets"])

    def test_model_errors(self):
        cases = [
            ("{provider: openai, input: {t: STRING}, output: {o: STRING}, properties: {endpoint: e, model: x}}",
             "CD209"),  # api-key missing
            ("{provider: openai, input: {t: STRING}, output: {o: STRING}, properties: {endpoint: e, model: x, "
             "api_key: plain}}", "CD201"),
            ("{provider: acme, input: {t: STRING}, output: {o: STRING}}", "CD200"),
        ]
        for decl, code in cases:
            with self.subTest(decl=decl):
                r = compile_text(f"ml_models:\n  mdl: {decl}\nsinks:\n  out: {{from: orders, connection: print}}\n")
                self.assertIn(code, errors(r))
        r = compile_text("""
            models:
              m: {sql: "SELECT * FROM ML_PREDICT(TABLE orders, MODEL nope, DESCRIPTOR(order_id))"}
            sinks:
              out: {from: m, connection: print}
            """)
        self.assertIn("CD302", errors(r))


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class NewSqlFlinkTest(unittest.TestCase):
    def test_variant_fixtures_and_restore_without_a_plan(self):
        d = project(VARIANT, base=False, files={"tests/t.yaml": """
            tests:
              - name: variant_from_fixture
                target: m
                given:
                  ev: [{id: a, doc: {kind: x, n: [1, 2]}, payload: '{"amount": 2.5}'}]
                expect:
                  rows: [{id: a, kind: '"x"', doc: {kind: x, n: [1, 2]}, parsed: {amount: 2.5}}]
            """})
        try:
            r, results = testing.run(d, FLINK)
            self.assertIsNotNone(results, [x.format() for x in r.diagnostics.errors])
            for t in results:
                self.assertTrue(t.passed, f"{t.case.name}: {t.message}")
            with self.assertRaises(PlanError):  # Flink 2.3 can't serialize VARIANT into a plan
                compile_plan(d, FLINK, os.path.join(d, "plan.json"))
            res = restore_test(d, d, FLINK)
            self.assertTrue(res.get("restored"), res)
            self.assertEqual(res["mode"], "statements")
        finally:
            shutil.rmtree(d)

    def test_ml_predict_restores_with_a_plan(self):
        res = restore_test(os.path.join(EXAMPLES, "ml_predict"), os.path.join(EXAMPLES, "ml_predict"), FLINK)
        self.assertTrue(res.get("restored"), res)
        self.assertTrue(res["pinned"])

    def test_model_fixture_missing_input_fails_the_test(self):
        with open(os.path.join(EXAMPLES, "ml_predict", "pipeline.yaml")) as f:
            text = f.read()
        d = project(text, base=False, files={"tests/t.yaml": """
            tests:
              - name: unpinned_input
                target: routed
                given:
                  tickets: [{ticket_id: t1, body: "hello", ts: "2026-01-01 10:00:00"}]
                expect: {error: no model fixture for input}
            """})
        try:
            r, results = testing.run(d, FLINK)
            self.assertTrue(results[0].passed, results[0].message)
        finally:
            shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
