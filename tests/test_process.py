"""Process models: DataStream code between SQL models."""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

from cascade_dsl import testing
from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jvm import TARGET, find_flink_home

from helpers import project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-runtime.jar"))
EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "examples", "sessions")

PIPE = """
defaults: {state_ttl: 1d}
models:
  dedup:
    process:
      class: com.acme.Dedup
      inputs: [orders]
      key_by: [order_id]
      PROC_EXTRA
    columns: {order_id: STRING, amount: "DECIMAL(10, 2)", ts: TIMESTAMP(3)}
  totals:
    sql: SELECT COUNT(*) AS n FROM dedup
sinks:
  out: {from: totals, connection: print, mode: upsert, primary_key: [n]}
"""


def compiled(extra="", text=PIPE):
    root = project(text.replace("PROC_EXTRA", extra))
    try:
        return compile_pipeline(root)
    finally:
        shutil.rmtree(root)


class ProcessCompileTest(unittest.TestCase):
    def test_relation_and_directive(self):
        r = compiled("event_time: ts")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        rel = r.checked.pipeline.relations["dedup"]
        self.assertEqual((rel.inputs, rel.changelog, rel.rowtime), (["orders"], "append", {"ts"}))
        st = next(s for s in r.lowered.statements if s.kind == "process")
        spec = json.loads(st.sql.split(" ", 2)[2])
        self.assertEqual((spec["class"], spec["inputs"], spec["key_by"], spec["view"]),
                         ("com.acme.Dedup", ["orders"], ["order_id"], "dedup"))
        self.assertIn("CD140", r.diagnostics.codes())  # state is outside upgrade checks
        # the process runs before the SQL that reads it
        kinds = [(s.kind, s.name) for s in r.lowered.statements]
        self.assertLess(kinds.index(("process", "dedup")), kinds.index(("view", "totals")))

    def test_upsert_output_needs_and_carries_a_key(self):
        r = compiled("changelog: upsert\n      primary_key: [order_id]")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        rel = r.checked.pipeline.relations["dedup"]
        self.assertEqual((rel.changelog, rel.upsert_key), ("updating", ["order_id"]))
        self.assertIn("CD005", compiled("changelog: upsert").diagnostics.codes())

    def test_errors(self):
        self.assertIn("CD141", compiled(text=PIPE.replace("key_by: [order_id]", "key_by: [nope]")).diagnostics.codes())
        self.assertIn("CD142", compiled("event_time: nope").diagnostics.codes())
        self.assertIn("CD302", compiled(text=PIPE.replace("inputs: [orders]", "inputs: [ordrs]")).diagnostics.codes())
        no_cols = PIPE.replace('    columns: {order_id: STRING, amount: "DECIMAL(10, 2)", ts: TIMESTAMP(3)}\n', "")
        self.assertIn("CD005", compiled(text=no_cols).diagnostics.codes())

    def test_side_outputs_are_relations(self):
        r = compiled("side_outputs:\n        dupes: {columns: {order_id: STRING}}")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertEqual(r.checked.pipeline.relations["dedup_dupes"].inputs, ["dedup"])

    def test_backfill_copies_a_process_model(self):
        r = compiled(text=PIPE.replace("  totals:\n", "  totals:\n    backfill: true\n"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        copy = r.checked.pipeline.models["dedup__bf_totals"]
        self.assertEqual(copy.process["inputs"], ["orders__bf_totals"])

    def test_no_compiled_plan_and_deploy_ships_the_jar(self):
        from cascade_dsl import deploy
        from cascade_dsl.plans import PlanError, compile_plan
        root = project(PIPE.replace("PROC_EXTRA", "jar: jobs/p.jar"), files={"jobs/p.jar": ""})
        out = tempfile.mkdtemp()
        try:
            with self.assertRaises(PlanError):
                compile_plan(root, FLINK or "/nonexistent", os.path.join(out, "plan.json"))
            r = compile_pipeline(root)
            deploy.build(r, out, image="img")
            with open(os.path.join(out, "Dockerfile")) as f:
                self.assertIn("/opt/flink/usrlib/p.jar", f.read())
            import yaml
            with open(os.path.join(out, "configmap.yaml")) as f:
                setup = yaml.safe_load(f)["data"]["setup.sql"]
            self.assertIn('"jar": "/opt/flink/usrlib/p.jar"', setup)
        finally:
            shutil.rmtree(root)
            shutil.rmtree(out)


@unittest.skipUnless(READY, "needs FLINK_HOME and runtime/build.sh")
class ProcessExecutionTest(unittest.TestCase):
    def test_sessions_example(self):
        d = tempfile.mkdtemp(prefix="cascade-process-")
        try:
            shutil.copytree(EXAMPLE, os.path.join(d, "sessions"))
            subprocess.run([os.path.join(d, "sessions", "jobs", "build.sh")], check=True, capture_output=True,
                           env={**os.environ, "FLINK_HOME": FLINK, "CASCADE_RUNTIME_JAR": os.path.join(TARGET, "cascade-runtime.jar")})
            r, results = testing.run(os.path.join(d, "sessions"), FLINK)
            self.assertIsNotNone(results, [x.format() for x in r.diagnostics.errors])
            self.assertEqual(len(results), 3)
            for t in results:
                self.assertTrue(t.passed, f"{t.case.name}: {t.message}")
        finally:
            shutil.rmtree(d)


FILES = """
pipeline: sessions_files
defaults: {state_ttl: 1d}
connections:
  fs: {type: filesystem, properties: {base_path: "file://DATA"}}
sources:
  clicks:
    connection: fs
    path: in
    format: csv
    columns: {user_id: STRING, url: STRING, ts: TIMESTAMP(3)}
    event_time: ts
models:
  sessions:
    process:
      class: com.acme.process.Sessionize
      jar: JAR
      inputs: [clicks]
      key_by: [user_id]
      config: {gap: PT30M}
      event_time: session_closed
      side_outputs:
        late: {columns: {user_id: STRING, url: STRING, ts: TIMESTAMP(3)}}
    columns: {user_id: STRING, session_start: TIMESTAMP(3), session_end: TIMESTAMP(3), clicks: BIGINT,
              session_closed: TIMESTAMP(3)}
sinks:
  out: {from: sessions, connection: fs, path: out, format: csv}
"""


@unittest.skipUnless(READY, "needs FLINK_HOME and runtime/build.sh")
class ProcessRunnerTest(unittest.TestCase):
    def test_sql_runner_runs_sql_and_datastream_as_one_job(self):
        import glob
        from cascade_dsl.jvm import runtime_jar, script
        d = tempfile.mkdtemp(prefix="cascade-process-run-")
        try:
            shutil.copytree(EXAMPLE, os.path.join(d, "sessions"))
            subprocess.run([os.path.join(d, "sessions", "jobs", "build.sh")], check=True, capture_output=True,
                           env={**os.environ, "FLINK_HOME": FLINK, "CASCADE_RUNTIME_JAR": os.path.join(TARGET, "cascade-runtime.jar")})
            os.makedirs(os.path.join(d, "data", "in"))
            with open(os.path.join(d, "data", "in", "clicks.csv"), "w") as f:
                f.write("u1,/a,2026-01-01 10:00:00\nu2,/a,2026-01-01 10:05:00\nu1,/b,2026-01-01 10:10:00\n"
                        "u1,/c,2026-01-01 11:00:00\n")
            root = project(FILES.replace("DATA", os.path.join(d, "data"))
                           .replace("JAR", os.path.join(d, "sessions", "jobs", "acme-process.jar")), base=False)
            r = compile_pipeline(root)
            self.assertTrue(r.ok, [x.format() for x in r.diagnostics.items])
            with open(os.path.join(root, "setup.sql"), "w") as f:
                f.write(script([s for s in r.lowered.statements if s.kind != "insert"]))
            with open(os.path.join(root, "statements.sql"), "w") as f:
                f.write(script([s for s in r.lowered.statements if s.kind == "insert"]))
            cp = os.pathsep.join([runtime_jar(), os.path.join(FLINK, "lib", "*")])
            p = subprocess.run(["java", "-cp", cp, "dev.cascade.runner.SqlRunner", "--setup",
                                os.path.join(root, "setup.sql"), "--statements",
                                os.path.join(root, "statements.sql"), "--await"],
                               capture_output=True, text=True, timeout=300)
            self.assertEqual(p.returncode, 0, p.stderr[-3000:])
            rows = set()
            for part in glob.glob(os.path.join(d, "data", "out", "**", "*"), recursive=True):
                if os.path.isfile(part) and not os.path.basename(part).startswith("."):
                    with open(part) as f:
                        rows |= {line.strip() for line in f if line.strip()}
            self.assertEqual(rows, {
                'u1,"2026-01-01 10:00:00","2026-01-01 10:10:00",2,"2026-01-01 10:40:00"',
                'u1,"2026-01-01 11:00:00","2026-01-01 11:00:00",1,"2026-01-01 11:30:00"',
                'u2,"2026-01-01 10:05:00","2026-01-01 10:05:00",1,"2026-01-01 10:35:00"'})
            shutil.rmtree(root)
        finally:
            shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
