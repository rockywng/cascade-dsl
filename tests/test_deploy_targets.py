"""deploy.target: session (FlinkSessionJob) and flink-run (run.sh around `flink run`)."""
import base64
import glob
import os
import shutil
import subprocess
import tempfile
import unittest

import yaml

from cascade_dsl import deploy
from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jvm import TARGET, find_flink_home, runtime_jar

from helpers import project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-runtime.jar"))

PIPE = """
pipeline: files
mode: batch
deploy: DEPLOY
connections:
  fs: {type: filesystem, properties: {base_path: "file://DATA"}}
sources:
  orders:
    connection: fs
    path: in
    format: csv
    columns: {customer_id: STRING, amount: BIGINT}
models:
  totals:
    sql: SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
sinks:
  out: {from: totals, connection: fs, path: out, format: csv}
"""


def build(deploy_block: str, data: str = "/tmp/x"):
    root = project(PIPE.replace("DEPLOY", deploy_block).replace("DATA", data), base=False)
    out = tempfile.mkdtemp()
    r = compile_pipeline(root)
    assert r.ok, [d.format() for d in r.diagnostics.items]
    return root, out, r


class SessionTargetTest(unittest.TestCase):
    def test_session_job_carries_its_scripts(self):
        root, out, r = build("{target: session, session_cluster: shared-flink, jar_uri: 's3://b/cascade-runtime.jar'}")
        try:
            info = deploy.build(r, out)
            self.assertEqual(info["target"], "session")
            with open(os.path.join(out, "flinksessionjob.yaml")) as f:
                job = yaml.safe_load(f)
            self.assertEqual((job["kind"], job["spec"]["deploymentName"], job["spec"]["job"]["jarURI"]),
                             ("FlinkSessionJob", "shared-flink", "s3://b/cascade-runtime.jar"))
            args = job["spec"]["job"]["args"]
            self.assertEqual(args[0], "--setup-b64")
            self.assertIn("CREATE TEMPORARY TABLE `orders`", base64.b64decode(args[1]).decode())
            self.assertEqual(job["spec"]["job"]["upgradeMode"], "stateless")  # batch
            self.assertTrue(os.path.exists(os.path.join(out, "REQUIREMENTS")))
        finally:
            shutil.rmtree(root)
            shutil.rmtree(out)

    def test_session_needs_cluster_and_jar(self):
        root, out, r = build("{target: session}")
        try:
            with self.assertRaises(deploy.DeployError):
                deploy.build(r, out)
        finally:
            shutil.rmtree(root)
            shutil.rmtree(out)


class FlinkRunTargetTest(unittest.TestCase):
    def test_run_script(self):
        for block, want in (("{target: flink-run, run: {jobmanager: 'flink:8081'}}", "run -m flink:8081 -d"),
                            ("{target: flink-run, run: {mode: yarn-application}}",
                             "run -t yarn-application -Dyarn.ship-files=job")):
            with self.subTest(block=block):
                root, out, r = build(block)
                try:
                    deploy.build(r, out)
                    with open(os.path.join(out, "run.sh")) as f:
                        sh = f.read()
                    self.assertIn(want, sh)
                    self.assertIn("--setup job/setup.sql --statements job/statements.sql", sh)
                    self.assertTrue(os.path.exists(os.path.join(out, "job", "setup.sql")))
                    self.assertTrue(os.access(os.path.join(out, "run.sh"), os.X_OK))
                finally:
                    shutil.rmtree(root)
                    shutil.rmtree(out)


@unittest.skipUnless(READY, "needs FLINK_HOME and runtime/build.sh")
class InlineScriptsRunTest(unittest.TestCase):
    def test_runner_runs_base64_scripts(self):
        data = tempfile.mkdtemp(prefix="cascade-b64-")
        os.makedirs(os.path.join(data, "in"))
        with open(os.path.join(data, "in", "p.csv"), "w") as f:
            f.write("c1,10\nc2,5\nc1,7\n")
        root, out, r = build("{target: session, session_cluster: s, jar_uri: 'https://x/r.jar'}", data)
        try:
            deploy.build(r, out)
            with open(os.path.join(out, "flinksessionjob.yaml")) as f:
                args = yaml.safe_load(f)["spec"]["job"]["args"]
            cp = os.pathsep.join([runtime_jar(), os.path.join(FLINK, "lib", "*")])
            p = subprocess.run(["java", "-cp", cp, "dev.cascade.runner.SqlRunner", *args, "--await"],
                               capture_output=True, text=True, timeout=300)
            self.assertEqual(p.returncode, 0, p.stderr[-3000:])
            rows = set()
            for part in glob.glob(os.path.join(data, "out", "**", "*"), recursive=True):
                if os.path.isfile(part) and not os.path.basename(part).startswith("."):
                    with open(part) as f:
                        rows |= {x.strip() for x in f if x.strip()}
            self.assertEqual(rows, {"c1,17", "c2,5"})
        finally:
            shutil.rmtree(root)
            shutil.rmtree(out)
            shutil.rmtree(data)


if __name__ == "__main__":
    unittest.main()
