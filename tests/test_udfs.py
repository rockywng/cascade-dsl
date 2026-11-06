"""User-defined functions: validation, static typing, lowering, deployment, and real execution."""
import os
import shutil
import subprocess
import tempfile
import unittest

import yaml

from cascade_dsl import testing
from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.deploy import build
from cascade_dsl.jvm import TARGET, find_flink_home, python_executable

from helpers import project

EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples")
FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))

SCORING = '''
def churn_risk(days, orders):
    return 0.5
'''


def udf_project(functions: str, sql: str = "SELECT order_id, churn_risk(1, 2) AS risk FROM orders",
                files=None):
    return project(f"""
        functions:
{functions}
        models:
          m:
            sql: {sql}
        sinks:
          out: {{from: m, connection: print}}
        """, files={"udfs/scoring.py": SCORING, **(files or {})})


PY_FN = """          churn_risk:
            language: python
            file: udfs/scoring.py
            returns: DOUBLE
"""


def codes(r):
    return [d.code for d in r.diagnostics.items]


class UdfCompileTest(unittest.TestCase):
    def test_python_function_lowering_and_typing(self):
        r = compile_pipeline(udf_project(PY_FN))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("`risk` DOUBLE", r.lowered.sql)  # typed from `returns`, no CD408
        self.assertIn("AS 'cascade_udfs.churn_risk' LANGUAGE PYTHON;", r.lowered.sql)
        self.assertIn("SET 'python.files' = ", r.lowered.sql)
        shim_dir = os.path.join(r.checked.pipeline.root_dir, ".cascade", "python")
        with open(os.path.join(shim_dir, "cascade_udfs.py")) as f:
            shim = f.read()
        self.assertIn("from scoring import churn_risk as _churn_risk", shim)
        self.assertIn("churn_risk = udf(_churn_risk, result_type='DOUBLE', deterministic=True)", shim)
        self.assertTrue(os.path.exists(os.path.join(shim_dir, "scoring.py")))
        self.assertEqual(r.lowered.manifest["functions"]["churn_risk"]["returns"], "DOUBLE")

    def test_java_function(self):
        r = compile_pipeline(udf_project("""          mask:
            language: java
            class: com.acme.Mask
            jar: udfs/x.jar
""", sql="SELECT mask(order_id) AS k FROM orders"))
        self.assertIn("CD164", codes(r))  # jar not built yet: a warning
        self.assertIn("CD408", codes(r))  # no `returns`: unknown output type
        lowered = compile_pipeline(r.checked.pipeline.root_dir, emit=True)
        self.assertIsNone(lowered.lowered)  # CD408 blocks output until `returns` or `cascade infer`

    def test_java_function_with_returns(self):
        r = compile_pipeline(udf_project("""          mask:
            language: java
            class: com.acme.Mask
            returns: STRING
""", sql="SELECT mask(order_id) AS k FROM orders"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS `mask` AS 'com.acme.Mask' LANGUAGE JAVA;",
                      r.lowered.sql)

    def test_validation(self):
        cases = [
            ("          f:\n            language: python\n            file: udfs/scoring.py\n", "CD005"),  # returns
            ("          f:\n            language: python\n            file: udfs/nope.py\n"
             "            returns: INT\n", "CD160"),
            ("          f:\n            language: python\n            file: udfs/scoring.py\n"
             "            function: missing\n            returns: INT\n", "CD161"),
            ("          upper:\n            language: java\n            class: x.Upper\n", "CD162"),
            ("          f:\n            language: rust\n", "CD014"),
            ("          f:\n            language: java\n            clas: x.F\n", "CD001"),
        ]
        for fdef, code in cases:
            with self.subTest(code=code):
                r = compile_pipeline(udf_project(fdef, sql="SELECT order_id FROM orders"))
                self.assertIn(code, codes(r), [d.format() for d in r.diagnostics.items])

    def test_module_name_collision(self):
        r = compile_pipeline(udf_project(PY_FN + """          other:
            language: python
            file: lib/scoring.py
            function: churn_risk
            returns: DOUBLE
""", files={"lib/scoring.py": SCORING}))
        self.assertIn("CD163", codes(r))

    def test_python_api_matches_yaml(self):
        a = compile_pipeline(os.path.join(EXAMPLES, "udfs"))
        b = compile_pipeline(os.path.join(EXAMPLES, "udfs_py"))
        self.assertTrue(a.checked and b.checked)
        fa, fb = a.lowered.manifest["functions"], b.lowered.manifest["functions"]
        self.assertEqual(fa["churn_risk"], fb["churn_risk"])
        self.assertEqual(fa["mask_email"]["class"], fb["mask_email"]["class"])
        self.assertEqual(a.lowered.manifest["nodes"]["scored_customers"]["columns"],
                         [{"name": "customer_id", "type": "STRING NOT NULL"},
                          {"name": "email_masked", "type": "STRING"},
                          {"name": "churn", "type": "DOUBLE"}, {"name": "ts", "type": "TIMESTAMP(3)"}])

    def test_deploy_ships_functions(self):
        out = tempfile.mkdtemp()
        build(compile_pipeline(os.path.join(EXAMPLES, "udfs")), out, image="img:1")
        with open(os.path.join(out, "configmap.yaml")) as f:
            setup = yaml.safe_load(f)["data"]["setup.sql"]
        self.assertIn("USING JAR '/opt/flink/usrlib/acme-udfs.jar'", setup)
        self.assertIn("SET 'python.files' = '/opt/cascade/python';", setup)
        with open(os.path.join(out, "python-configmap.yaml")) as f:
            self.assertEqual(set(yaml.safe_load(f)["data"]), {"cascade_udfs.py", "scoring.py"})
        with open(os.path.join(out, "Dockerfile")) as f:
            dockerfile = f.read()
        self.assertIn("apache-flink==2.3.0", dockerfile)
        self.assertIn("udfs/acme-udfs.jar /opt/flink/usrlib/acme-udfs.jar", dockerfile)
        with open(os.path.join(out, "flinkdeployment.yaml")) as f:
            fd = yaml.safe_load(f)
        self.assertEqual(fd["spec"]["flinkConfiguration"]["python.executable"], "/opt/pyflink/bin/python")
        shutil.rmtree(out)


@unittest.skipUnless(READY and python_executable(), "needs FLINK_HOME, runtime/build.sh and PyFlink "
                                                    "(pip install apache-flink, or set CASCADE_PYTHON)")
class UdfExecutionTest(unittest.TestCase):
    def test_examples_run_java_and_python_udfs(self):
        for name in ("udfs", "udfs_py"):
            with self.subTest(example=name):
                d = tempfile.mkdtemp(prefix="cascade-udf-")
                shutil.copytree(os.path.join(EXAMPLES, "udfs"), os.path.join(d, "udfs"))
                if name == "udfs_py":
                    shutil.copytree(os.path.join(EXAMPLES, "udfs_py"), os.path.join(d, "udfs_py"))
                subprocess.run([os.path.join(d, "udfs", "udfs", "build.sh")], check=True, capture_output=True,
                               env={**os.environ, "FLINK_HOME": FLINK})
                r, results = testing.run(os.path.join(d, name), FLINK)
                self.assertIsNotNone(results, [x.format() for x in r.diagnostics.errors])
                self.assertTrue(results)
                for t in results:
                    self.assertTrue(t.passed, f"{t.case.name}: {t.message}")
                shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
