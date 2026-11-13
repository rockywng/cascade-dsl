"""Aggregate, table and process table functions (Java and plain Python)."""
import os
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.udfs import shim

from helpers import compile_text

EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples")
AGG = """
class Avg:
    def create_accumulator(self):
        return [0.0, 0]

    def accumulate(self, acc, v):
        acc[0] += v
        acc[1] += 1

    def get_value(self, acc):
        return acc[0] / acc[1]


def explode(s):
    for x in s.split(","):
        yield x, len(x)
"""


def codes(r):
    return sorted(d.code for d in r.diagnostics.items if d.severity == "error")


class FunctionKindsTest(unittest.TestCase):
    def test_examples_compile_with_typed_outputs(self):
        for name in ("functions", "functions_py"):
            with self.subTest(example=name):
                r = compile_pipeline(os.path.join(EXAMPLES, name))
                self.assertEqual(codes(r), [], [d.format() for d in r.diagnostics.items])
        r = compile_pipeline(os.path.join(EXAMPLES, "functions"))
        p = r.checked.pipeline
        self.assertEqual([(c.name, c.type.text) for c in p.relations["courier_streaks"].columns],
                         [("courier", "STRING NOT NULL"), ("streak", "INT"), ("ok", "BOOLEAN")])
        self.assertEqual([(c.name, c.type.text) for c in p.relations["review_tags"].columns],
                         [("product_id", "STRING NOT NULL"), ("tag", "STRING"), ("pos", "INT")])
        self.assertEqual(p.relations["courier_streaks"].inputs, ["deliveries"])
        kinds = {op.kind for op in p.relations["courier_streaks"].stateful_ops}
        self.assertIn("process_table", kinds)

    def test_ptf_state_needs_a_ttl(self):
        r = compile_text("""
            functions:
              f: {language: java, kind: process_table, class: a.F, returns: "ROW<n INT>"}
            models:
              m: {sql: "SELECT customer_id, n FROM f(input => TABLE orders PARTITION BY customer_id)"}
            sinks:
              out: {from: m, connection: print}
            """)
        self.assertIn("CD350", codes(r))

    def test_python_shim(self):
        r = compile_text("""
            defaults: {state_ttl: 1d}
            functions:
              avg: {language: python, kind: aggregate, file: f.py, class: Avg, accumulator: "ROW<s DOUBLE, n INT>",
                    returns: DOUBLE}
              explode: {language: python, kind: table, file: f.py, returns: "ROW<part STRING, len INT>"}
            models:
              m:
                sql: |
                  SELECT o.customer_id, t.part, t.len FROM orders AS o, LATERAL TABLE(explode(o.order_id)) AS t
              a: {sql: "SELECT customer_id, avg(CAST(amount AS DOUBLE)) AS v FROM orders GROUP BY customer_id"}
            sinks:
              out: {from: m, connection: print}
              out2: {from: a, connection: print, mode: upsert, primary_key: [customer_id]}
            """, files={"f.py": AGG})
        self.assertEqual(codes(r), [], [d.format() for d in r.diagnostics.items])
        text = shim(r.checked.pipeline)
        self.assertIn("avg = udaf(_Aggregate(_avg, True), result_type='DOUBLE', "
                      "accumulator_type='ROW<s DOUBLE, n INT>'", text)
        self.assertIn("explode = udtf(_explode, result_types='ROW<part STRING, len INT>'", text)
        compile(text, "cascade_udfs.py", "exec")  # the generated module is valid Python

    def test_declaration_errors(self):
        cases = [
            ("{language: java, kind: window, class: a.F}", "CD014"),
            ("{language: python, kind: process_table, file: f.py, returns: 'ROW<a INT>'}", "CD014"),
            ("{language: java, kind: table, class: a.F, returns: INT}", "CD010"),
            ("{language: python, kind: aggregate, file: f.py, class: Avg, returns: DOUBLE}", "CD005"),
            ("{language: python, kind: aggregate, file: f.py, class: Nope, accumulator: INT, returns: DOUBLE}",
             "CD161"),
        ]
        for decl, code in cases:
            with self.subTest(decl=decl):
                r = compile_text(f"""
                    functions:
                      f: {decl}
                    sinks:
                      out: {{from: orders, connection: print}}
                    """, files={"f.py": AGG})
                self.assertIn(code, codes(r))

    def test_scalar_function_used_as_table(self):
        r = compile_text("""
            functions:
              f: {language: java, class: a.F, returns: STRING}
            models:
              m: {sql: "SELECT o.order_id, t.x FROM orders AS o, LATERAL TABLE(f(o.order_id)) AS t(x)"}
            sinks:
              out: {from: m, connection: print}
            """)
        self.assertIn("CD165", codes(r))


if __name__ == "__main__":
    unittest.main()
