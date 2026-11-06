"""cascade deploy: generated Kubernetes resources, and the runner that executes them."""
import os
import shutil
import subprocess
import tempfile
import unittest

import yaml

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.deploy import DeployError, build, env_name, secret_key
from cascade_dsl.jvm import TARGET, Tools, find_flink_home

from helpers import project

EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples")
FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))


def load_all(d):
    out = {}
    for name in os.listdir(d):
        if name.endswith(".yaml"):
            with open(os.path.join(d, name)) as f:
                out[name] = yaml.safe_load(f)
    return out


class DeployTest(unittest.TestCase):
    def setUp(self):
        self.out = tempfile.mkdtemp(prefix="cascade-deploy-")

    def tearDown(self):
        shutil.rmtree(self.out, ignore_errors=True)

    def test_orders_prod(self):
        r = compile_pipeline(os.path.join(EXAMPLES, "orders"), env="prod")
        info = build(r, self.out)
        docs = load_all(self.out)
        fd = docs["flinkdeployment.yaml"]
        self.assertEqual(fd["kind"], "FlinkDeployment")
        self.assertEqual(fd["metadata"]["name"], "orders-analytics-prod")
        self.assertEqual(fd["spec"]["image"], "ghcr.io/acme/cascade-orders:1.4.0")  # vars per environment
        conf = fd["spec"]["flinkConfiguration"]
        self.assertEqual(conf["pipeline.max-parallelism"], "720")
        self.assertEqual(conf["job.autoscaler.vertex.max-parallelism"], "48")
        self.assertEqual(conf["execution.checkpointing.dir"], "s3://prod-flink-state/orders/checkpoints")
        self.assertEqual(fd["spec"]["job"]["parallelism"], 2)
        self.assertEqual(fd["spec"]["job"]["entryClass"], "dev.cascade.runner.SqlRunner")
        env = {e["name"]: e["valueFrom"]["secretKeyRef"] for e in
               fd["spec"]["podTemplate"]["spec"]["containers"][0]["env"]}
        self.assertEqual(env["CASCADE_SECRET_KAFKA_PROD_BOOTSTRAP"],
                         {"name": "orders-analytics-prod-secrets", "key": "kafka.prod.bootstrap"})
        cm = docs["configmap.yaml"]["data"]
        self.assertIn("${secret:kafka/prod/bootstrap}", cm["setup.sql"])  # placeholders only
        self.assertIn("INSERT INTO `ltv_to_kafka`", cm["statements.sql"])
        self.assertEqual(docs["secret.template.yaml"]["stringData"], {"crm.password": "", "kafka.prod.bootstrap": ""})
        self.assertEqual(info["mode"], "statements")
        with open(os.path.join(self.out, "Dockerfile")) as f:
            dockerfile = f.read()
        self.assertIn("ARG FLINK_SQL_CONNECTOR_KAFKA_VERSION\n", dockerfile)
        self.assertIn("COPY runtime/target/cascade-runtime.jar", dockerfile)

    def test_python_example_matches(self):
        a = tempfile.mkdtemp()
        build(compile_pipeline(os.path.join(EXAMPLES, "orders"), env="prod"), a)
        build(compile_pipeline(os.path.join(EXAMPLES, "orders_py"), env="prod"), self.out)
        for name in ("flinkdeployment.yaml", "configmap.yaml"):
            x, y = load_all(a)[name], load_all(self.out)[name]
            if name == "flinkdeployment.yaml":  # the SQL hash covers the header, which names the file
                x["metadata"]["annotations"].pop("cascade.dev/sql-sha256")
                y["metadata"]["annotations"].pop("cascade.dev/sql-sha256")
            if name == "configmap.yaml":
                for k in x["data"]:  # headers name the source file
                    x["data"][k] = x["data"][k].split("\n", 1)[1]
                    y["data"][k] = y["data"][k].split("\n", 1)[1]
            self.assertEqual(x, y, name)
        shutil.rmtree(a)

    def test_needs_an_image(self):
        r = compile_pipeline(os.path.join(EXAMPLES, "clickstream_py"))
        with self.assertRaises(DeployError):
            build(r, self.out)
        build(r, self.out, image="img:1")

    def test_secret_names(self):
        self.assertEqual(env_name("kafka/prod/bootstrap"), "CASCADE_SECRET_KAFKA_PROD_BOOTSTRAP")
        self.assertEqual(secret_key("db/pw#1"), "db.pw-1")

    def test_secrets_not_allowed_in_deploy_settings(self):
        r = compile_pipeline(project("""
            deploy: {image: "x", savepoints: "${secret:bucket}"}
            models:
              m: {sql: SELECT order_id FROM orders}
            sinks:
              out: {from: m, connection: print}
            """))
        self.assertIn("CD530", r.diagnostics.codes())


RUNNABLE = """
pipeline: runner_check
defaults: {state_ttl: 1h}
scaling: {max_parallelism: 128, parallelism: 1}
deploy: {image: "local/cascade:test"}
sources:
  ticks:
    connection: datagen
    number_of_rows: 20
    columns: {k: INT, v: BIGINT, ts: "TIMESTAMP(3)"}
    event_time: ts
models:
  totals:
    sql: SELECT k, SUM(v) AS total FROM ticks GROUP BY k
sinks:
  out:
    from: totals
    connection: print
    mode: upsert
    primary_key: [k]
    options: {print-identifier: "${secret:print/prefix}"}
"""


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class RunnerTest(unittest.TestCase):
    """The deployed entry point, run locally in --validate mode against the generated ConfigMap."""

    def run_runner(self, files: dict, args: list[str], env: dict):
        d = tempfile.mkdtemp(prefix="cascade-runner-")
        for name, text in files.items():
            with open(os.path.join(d, name), "w") as f:
                f.write(text)
        tools = Tools(FLINK)
        cp = os.pathsep.join([os.path.join(TARGET, "cascade-runtime.jar"), os.path.join(FLINK, "lib", "*")])
        cmd = [tools._java(), *tools._java_opts(), "-cp", cp, "dev.cascade.runner.SqlRunner",
               *[a.replace("/opt/cascade/job", d) for a in args], "--validate"]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300,
                              env={**os.environ, **env})
        shutil.rmtree(d)
        return proc

    def test_plan_and_statement_modes(self):
        from cascade_dsl.plans import compile_plan
        root = project(RUNNABLE, base=False)
        work = tempfile.mkdtemp()
        r, _, _ = compile_plan(root, FLINK, os.path.join(work, "plan.json"))
        for plan_dir in (work, None):
            out = os.path.join(work, "deploy-" + ("plan" if plan_dir else "stmts"))
            build(r if plan_dir else compile_pipeline(root), out, plan_dir=plan_dir)
            docs = load_all(out)
            args = docs["flinkdeployment.yaml"]["spec"]["job"]["args"]
            files = docs["configmap.yaml"]["data"]
            with self.subTest(mode="plan" if plan_dir else "statements"):
                ok = self.run_runner(files, args, {"CASCADE_SECRET_PRINT_PREFIX": "it's"})
                self.assertEqual(ok.returncode, 0, ok.stderr[-3000:])
                self.assertIn("OK", ok.stdout)
                missing = self.run_runner(files, args, {})
                self.assertNotEqual(missing.returncode, 0)
                self.assertIn("missing secrets: print/prefix (CASCADE_SECRET_PRINT_PREFIX)", missing.stderr)
        shutil.rmtree(work)
        shutil.rmtree(root)


if __name__ == "__main__":
    unittest.main()
