"""cascade_dsl.evaluate: run pipeline.py elsewhere, compile only its document here."""
import json
import os
import subprocess
import sys
import tempfile
import unittest

import yaml

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.evaluate import MARKER, evaluate

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.join(HERE, "..", "examples", "orders_py")


def _project(code: str) -> str:
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "pipeline.py"), "w") as f:
        f.write(code)
    return d


class EvaluateTest(unittest.TestCase):
    def test_the_document_compiles_like_the_python_itself(self):
        r = evaluate(EXAMPLE)
        self.assertTrue(r["ok"], r)
        json.dumps(r["document"])  # plain data
        d = tempfile.mkdtemp()
        with open(os.path.join(d, "pipeline.yaml"), "w") as f:
            yaml.safe_dump(r["document"], f, sort_keys=False)
        body = lambda res: [l for l in res.lowered.sql.splitlines() if not l.startswith("-- Generated")]  # noqa: E731
        self.assertEqual(body(compile_pipeline(d, None)), body(compile_pipeline(EXAMPLE, None)))

    def test_errors_name_the_line(self):
        r = evaluate(_project("from cascade_dsl.api import Pipeline\np = Pipeline('x')\nraise ValueError('nope')\n"))
        self.assertFalse(r["ok"])
        self.assertIn("pipeline.py:3: error[CD000]: pipeline.py raised ValueError: nope", r["errors"][0])
        r = evaluate(_project("x = 1\n"))
        self.assertIn("exactly one Pipeline", r["errors"][0])

    def test_the_command_prints_one_result_line(self):
        p = subprocess.run([sys.executable, "-m", "cascade_dsl.evaluate", EXAMPLE], capture_output=True, text=True,
                           cwd=os.path.join(HERE, ".."))
        self.assertEqual(p.returncode, 0, p.stderr)
        line = [l for l in p.stdout.splitlines() if l.startswith(MARKER)][0]
        self.assertEqual(json.loads(line[len(MARKER):])["document"]["pipeline"], "orders_analytics")
        bad = subprocess.run([sys.executable, "-m", "cascade_dsl.evaluate", _project("raise SystemExit(3)\n")],
                             capture_output=True, text=True, cwd=os.path.join(HERE, ".."))
        self.assertEqual(bad.returncode, 1)
        self.assertIn('"ok": false', bad.stdout)  # even sys.exit in the author's code is reported


if __name__ == "__main__":
    unittest.main()
