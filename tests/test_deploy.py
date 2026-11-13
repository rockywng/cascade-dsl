"""cascade deploy: generated Kubernetes resources, and the runner that executes them."""
import os
import re
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


def sql_text(script: str) -> str:
    """SQL without comments and layout (the YAML example's .sql files have comments and other indentation)."""
    return " ".join(re.sub(r"--[^\n]*", "", script).split())


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
        # tested versions are the defaults; deploy.artifacts overrides them
        self.assertIn("ARG FLINK_SQL_CONNECTOR_KAFKA_VERSION=5.0.0-2.2\n", dockerfile)
        # Iceberg ships one runtime jar per Flink minor version
        self.assertIn("/org/apache/iceberg/iceberg-flink-runtime-2.3/", dockerfile)
        self.assertIn("COPY runtime/target/cascade-runtime.jar", dockerfile)

    def test_python_example_matches(self):
        a = tempfile.mkdtemp()
        build(compile_pipeline(os.path.join(EXAMPLES, "orders"), env="prod"), a)
        build(compile_pipeline(os.path.join(EXAMPLES, "orders_py"), env="prod"), self.out)
        for name in ("flinkdeployment.yaml", "configmap.yaml"):
            x, y = load_all(a)[name], load_all(self.out)[name]
            if name == "flinkdeployment.yaml":  # the hashes cover SQL comments and headers, which differ
                x["metadata"]["annotations"].pop("cascade.dev/sql-sha256")
                y["metadata"]["annotations"].pop("cascade.dev/sql-sha256")
                x, y = (yaml.safe_load(re.sub(r"-job-[0-9a-f]{10}", "-job-X", yaml.safe_dump(d))) for d in (x, y))
            if name == "configmap.yaml":
                x["metadata"]["name"] = y["metadata"]["name"] = "X"
            if name == "configmap.yaml":
                for k in x["data"]:  # headers name the source file
                    x["data"][k] = sql_text(x["data"][k])
                    y["data"][k] = sql_text(y["data"][k])
            self.assertEqual(x, y, name)
        shutil.rmtree(a)

    def test_new_version_changes_the_pod_template(self):
        """The operator upgrades (with a savepoint) only when the spec changes, and mounted ConfigMaps
        change in place: so each version gets its own ConfigMap, referenced from the pod template."""
        root = project("""
            deploy: {image: "img:1"}
            models:
              m: {sql: SELECT order_id FROM orders}
            sinks:
              out: {from: m, connection: print}
            """)
        build(compile_pipeline(root), self.out)
        v1 = load_all(self.out)
        path = os.path.join(root, "pipeline.yaml")
        with open(path) as f:
            text = f.read()
        with open(path, "w") as f:
            f.write(text.replace("FROM orders}", "FROM orders WHERE amount > 0}"))  # no comma: flow YAML
        v2_out = os.path.join(self.out, "v2")
        build(compile_pipeline(root), v2_out)
        v2 = load_all(v2_out)
        names = [d["configmap.yaml"]["metadata"]["name"] for d in (v1, v2)]
        self.assertNotEqual(names[0], names[1])
        for d, cm in zip((v1, v2), names):
            spec = d["flinkdeployment.yaml"]["spec"]["podTemplate"]
            self.assertEqual(spec["spec"]["volumes"][0]["configMap"]["name"], cm)
            self.assertEqual(spec["metadata"]["annotations"]["cascade.dev/job-config"], cm)
        shutil.rmtree(root)

    def test_pod_template_is_merged(self):
        from cascade_dsl.deploy import merge_pod_template
        base = {"spec": {"containers": [{"name": "flink-main-container", "volumeMounts": [{"name": "a"}]}],
                         "volumes": [{"name": "a"}]}}
        extra = {"spec": {"containers": [{"name": "flink-main-container", "volumeMounts": [{"name": "b"}]},
                                         {"name": "sidecar", "image": "x"}],
                          "volumes": [{"name": "b"}], "nodeSelector": {"pool": "flink"}}}
        m = merge_pod_template(base, extra)["spec"]
        self.assertEqual([c["name"] for c in m["containers"]], ["flink-main-container", "sidecar"])
        self.assertEqual(m["containers"][0]["volumeMounts"], [{"name": "a"}, {"name": "b"}])
        self.assertEqual(m["volumes"], [{"name": "a"}, {"name": "b"}])
        self.assertEqual(m["nodeSelector"], {"pool": "flink"})

    def test_needs_an_image(self):
        r = compile_pipeline(os.path.join(EXAMPLES, "clickstream_py"))
        with self.assertRaises(DeployError):
            build(r, self.out)
        build(r, self.out, image="img:1")

    def _ha_conf(self, deploy: str):
        r = compile_pipeline(project("defaults: {state_ttl: 1d}\nsinks:\n  out: {from: orders, connection: print}\n"
                                     "deploy: " + deploy + "\n"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        out = tempfile.mkdtemp(dir=self.out)
        build(r, out)
        return load_all(out)["flinkdeployment.yaml"]["spec"]

    def test_high_availability_by_default_for_stateful_jobs(self):
        spec = self._ha_conf('{image: x, checkpoints: {dir: "s3://st/cp"}}')
        self.assertEqual(spec["flinkConfiguration"]["high-availability.type"], "kubernetes")
        self.assertEqual(spec["flinkConfiguration"]["high-availability.storageDir"], "s3://st/cp/ha")
        spec = self._ha_conf('{image: x, checkpoints: {dir: "s3://st/cp"}, high_availability: {storage_dir: '
                             '"s3://st/ha"}, jobmanager: {replicas: 2}}')
        self.assertEqual(spec["flinkConfiguration"]["high-availability.storageDir"], "s3://st/ha")
        self.assertEqual(spec["jobManager"]["replicas"], 2)
        for off in ('{image: x, checkpoints: {dir: "s3://st/cp"}, high_availability: false}',
                    '{image: x, checkpoints: {dir: "s3://st/cp"}, upgrade_mode: stateless}'):
            self.assertNotIn("high-availability.type", self._ha_conf(off)["flinkConfiguration"])

    def test_high_availability_requirements(self):
        for bad in ('{image: x, checkpoints: {dir: "s3://st/cp"}, high_availability: false, upgrade_mode: last-state}',
                    '{image: x, checkpoints: {dir: "s3://st/cp"}, high_availability: false, jobmanager: {replicas: 2}}',
                    '{image: x, high_availability: true}'):
            with self.subTest(deploy=bad), self.assertRaises(DeployError):
                self._ha_conf(bad)

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
