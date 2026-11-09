"""mode: batch -- one run over bounded inputs."""
import glob
import os
import shutil
import subprocess
import tempfile
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jvm import TARGET, find_flink_home, runtime_jar, script

from helpers import project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-runtime.jar"))

KAFKA = """
mode: batch
pipeline: daily
connections:
  bus: {type: kafka, properties: {bootstrap_servers: k:9092}}
  db: {type: postgres-cdc, properties: {hostname: h, database: d, username: u, password: "${secret:db/pw}"}}
sources:
  orders:
    connection: bus
    topic: orders
    columns: {order_id: STRING, customer_id: STRING, amount: "DECIMAL(10, 2)"}
  customers:
    connection: db
    table: public.customers
    columns: {customer_id: STRING NOT NULL, tier: STRING}
    primary_key: [customer_id]
models:
  spend:
    sql: |
      SELECT c.tier, SUM(o.amount) AS total
      FROM orders AS o JOIN customers AS c ON o.customer_id = c.customer_id
      GROUP BY c.tier
sinks:
  out: {from: spend, connection: print, mode: upsert, primary_key: [tier]}
"""


def compiled(text):
    root = project(text, base=False)
    try:
        return compile_pipeline(root)
    finally:
        shutil.rmtree(root)


class BatchCompileTest(unittest.TestCase):
    def test_bounded_reads_and_runtime_mode(self):
        r = compiled(KAFKA)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("SET 'execution.runtime-mode' = 'batch';", r.lowered.sql)
        self.assertEqual(r.checked.options["orders"]["scan.bounded.mode"], "latest-offset")
        self.assertEqual(r.checked.options["customers"]["scan.startup.mode"], "snapshot")
        self.assertNotIn("CD350", r.diagnostics.codes())  # a regular join without TTL is fine for one run

    def test_unbounded_source(self):
        r = compiled(KAFKA.replace("bus: {type: kafka, properties: {bootstrap_servers: k:9092}}",
                                   "bus: {type: kinesis, properties: {region: us-east-1}}")
                     .replace("    topic: orders\n", "    stream_arn: arn:aws:kinesis:us-east-1:1:stream/o\n"))
        self.assertIn("CD120", r.diagnostics.codes())

    def test_temporal_join_and_backfill_are_streaming_only(self):
        text = KAFKA.replace("JOIN customers AS c", "JOIN customers FOR SYSTEM_TIME AS OF o.ts AS c") \
            .replace('amount: "DECIMAL(10, 2)"}', 'amount: "DECIMAL(10, 2)", ts: TIMESTAMP(3)}\n    event_time: ts') \
            .replace("    sql: |\n      SELECT c.tier", "    backfill: true\n    sql: |\n      SELECT c.tier")
        codes = compiled(text).diagnostics.codes()
        self.assertIn("CD121", codes)
        self.assertIn("CD122", codes)

    def test_deploys_stateless_without_a_plan(self):
        from cascade_dsl import deploy
        from cascade_dsl.plans import PlanError, compile_plan
        root = project(KAFKA, base=False)
        try:
            out = tempfile.mkdtemp()
            r = compile_pipeline(root)
            info = deploy.build(r, out, image="img")
            import yaml
            with open(os.path.join(out, "flinkdeployment.yaml")) as f:
                fd = yaml.safe_load(f)
            with self.assertRaises(PlanError):
                compile_plan(root, FLINK or "/nonexistent", os.path.join(out, "plan.json"))
            if fd is not None:
                self.assertEqual(fd["spec"]["job"]["upgradeMode"], "stateless")
        finally:
            shutil.rmtree(root)


CSV = """
mode: batch
pipeline: files
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
    sql: SELECT customer_id, SUM(amount) AS total, COUNT(*) AS n FROM orders GROUP BY customer_id
sinks:
  out: {from: totals, connection: fs, path: out, format: csv}
"""


@unittest.skipUnless(READY, "needs FLINK_HOME and runtime/build.sh")
class BatchRunTest(unittest.TestCase):
    def test_runs_to_completion_with_final_results(self):
        data = tempfile.mkdtemp(prefix="cascade-batch-")
        os.makedirs(os.path.join(data, "in"))
        with open(os.path.join(data, "in", "part-0.csv"), "w") as f:
            f.write("c1,10\nc2,5\nc1,7\nc3,1\nc2,5\n")
        root = project(CSV.replace("DATA", data), base=False)
        try:
            r = compile_pipeline(root)
            self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
            stmts = r.lowered.statements
            with open(os.path.join(root, "setup.sql"), "w") as f:
                f.write(script([s for s in stmts if s.kind != "insert"]))
            with open(os.path.join(root, "statements.sql"), "w") as f:
                f.write(script([s for s in stmts if s.kind == "insert"]))
            cp = os.pathsep.join([runtime_jar(), os.path.join(FLINK, "lib", "*")])
            p = subprocess.run(["java", "-cp", cp, "dev.cascade.runner.SqlRunner", "--setup",
                                os.path.join(root, "setup.sql"), "--statements", os.path.join(root, "statements.sql"),
                                "--await"], capture_output=True, text=True, timeout=300)
            self.assertEqual(p.returncode, 0, p.stderr[-3000:])
            rows = set()
            for part in glob.glob(os.path.join(data, "out", "**", "*"), recursive=True):
                if os.path.isfile(part) and not os.path.basename(part).startswith("."):
                    with open(part) as f:
                        rows |= {line.strip() for line in f if line.strip()}
            # batch: final values only, one row per key (a streaming run would append every update)
            self.assertEqual(rows, {"c1,17,2", "c2,10,2", "c3,1,1"})
        finally:
            shutil.rmtree(root)
            shutil.rmtree(data)


if __name__ == "__main__":
    unittest.main()
