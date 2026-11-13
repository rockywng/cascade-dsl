"""The integration scenario on Kubernetes: kind + the Flink Kubernetes Operator, applying exactly what
`cascade deploy` writes (kustomization, content-addressed ConfigMap, FlinkDeployment, Secret).

    python3 integration/k8s.py [--keep] [--skip-build]

Needs Docker, kind, kubectl and helm (on PATH or in ~/.local/bin). Reuses the runtime image and
helpers from run.py. Unlike run.py, nothing is submitted by hand: the operator runs the job in
application mode, and the v2 upgrade is just `kubectl apply -k` of the new deploy directory, which
the operator turns into stop-with-savepoint + restore.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time

import yaml

import run as it
from run import Failed, ok, step

CLUSTER = "cascade-it"
NODE = f"{CLUSTER}-control-plane"
OPERATOR = "1.16.1"
os.environ["PATH"] = os.path.expanduser("~/.local/bin") + os.pathsep + os.environ["PATH"]


def kubectl(*args, input=None, check=True, timeout=600) -> str:
    return it.run(["kubectl", "--context", f"kind-{CLUSTER}", *args], input=input, check=check, timeout=timeout)


def exec_pod(service: str, *args, input=None, check=True) -> str:
    return kubectl("exec", "-i", f"deploy/{service}", "--", *args, input=input, check=check)


def cluster():
    step("kind cluster, images, Kafka, Postgres, Flink operator")
    if CLUSTER not in it.run(["kind", "get", "clusters"], check=False):
        it.run(["kind", "create", "cluster", "--name", CLUSTER, "--wait", "120s"], timeout=600)
    it.run(["docker", "exec", NODE, "sh", "-c", "rm -rf /flink-data && mkdir -m 777 /flink-data"])
    # Public images are pulled by the node. The local runtime image is exported for one platform:
    # `kind load docker-image` fails on Docker Desktop's containerd store ("content digest not found")
    archive = os.path.join(it.WORK, "cascade-it.tar")
    it.run(["docker", "save", "--platform", "linux/amd64", "-o", archive, "cascade-it:it"], timeout=900)
    it.run(["kind", "load", "image-archive", "--name", CLUSTER, archive], timeout=900)
    os.remove(archive)
    ok(f"cluster {CLUSTER}; runtime image cascade-it:it loaded")

    with open(os.path.join(it.HERE, "k8s", "infra.yaml")) as f:
        docs = list(yaml.safe_load_all(f))
    with open(os.path.join(it.HERE, "init.sql")) as f:
        next(d for d in docs if d["kind"] == "ConfigMap")["data"]["init.sql"] = f.read()
    kubectl("apply", "-f", "-", input=yaml.safe_dump_all(docs))
    kubectl("rollout", "status", "deploy/kafka", "deploy/postgres", "--timeout=300s")
    it.wait_for("Kafka", lambda: "orders" in it.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                                                       "kafka:9092", "--create", "--if-not-exists", "--topic",
                                                       "orders") + it.kafka(
        "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list"), timeout=120, every=3)
    for topic in it.TOPICS[1:]:
        it.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--create", "--if-not-exists",
                 "--topic", topic, "--partitions", "1", "--replication-factor", "1")
    it.wait_for("Postgres seed", lambda: it.psql("SELECT count(*) FROM customers") == [["2"]], timeout=120)
    ok("Kafka and Postgres running in the cluster")

    repo = f"https://downloads.apache.org/flink/flink-kubernetes-operator-{OPERATOR}/"
    it.run(["helm", "repo", "add", "--force-update", "flink-operator", repo])
    it.run(["helm", "upgrade", "--install", "flink-kubernetes-operator", "flink-operator/flink-kubernetes-operator",
            "--version", OPERATOR, "--kube-context", f"kind-{CLUSTER}", "--set", "webhook.create=false",
            "--wait", "--timeout", "10m"], timeout=900)
    ok(f"Flink Kubernetes Operator {OPERATOR} installed")


def deployment() -> dict:
    out = kubectl("get", "flinkdeployment", "shop-it", "-o", "json", check=False)
    return json.loads(out) if out.strip().startswith("{") else {}


def job_state() -> tuple[str | None, str | None, str | None]:
    st = deployment().get("status", {})
    js = st.get("jobStatus", {})
    return js.get("state"), js.get("jobId"), st.get("lifecycleState")


def apply_version(version: str, project: str, previous: str | None = None) -> str:
    it.package(version, project, previous)  # writes plan-<v> and deploy-<v> under .work
    deploy_dir = os.path.join(it.WORK, f"deploy-{version}")
    with open(os.path.join(deploy_dir, "secret.template.yaml")) as f:
        secret = yaml.safe_load(f)
    secret["stringData"] = {k: it.SECRET for k in secret["stringData"]}
    kubectl("apply", "-f", "-", input=yaml.safe_dump(secret))
    out = kubectl("apply", "-k", deploy_dir)
    ok(f"{version}: kubectl apply -k {os.path.relpath(deploy_dir, it.REPO)}: "
       + "; ".join(line.strip() for line in out.splitlines() if line.strip()))
    with open(os.path.join(deploy_dir, "configmap.yaml")) as f:
        return yaml.safe_load(f)["metadata"]["name"]


def wait_running(previous_job: str | None = None, timeout=420) -> str:
    def ready():
        state, job, life = job_state()
        if state in ("FAILED",) or life == "FAILED":
            # not Failed: wait_for would retry on that
            raise RuntimeError("FlinkDeployment failed:\n" + json.dumps(deployment().get("status"), indent=1)[-4000:])
        return job if state == "RUNNING" and life == "STABLE" and job != previous_job else None
    try:
        return it.wait_for("FlinkDeployment RUNNING/STABLE", ready, timeout=timeout, every=5)
    except Failed:
        st = deployment().get("status", {})
        events = kubectl("get", "events", "--sort-by=.lastTimestamp", check=False)[-3000:]
        logs = kubectl("logs", "deploy/shop-it", "--tail", "80", check=False)[-5000:]
        raise Failed(f"FlinkDeployment did not become stable:\n{json.dumps(st, indent=1)[-3000:]}\n"
                     f"{events}\n{logs}")


def phase_v1() -> str:
    step("v1: cascade deploy output applied to the operator")
    cm = apply_version("v1", "integration/pipeline")
    job = wait_running()
    ok(f"operator runs v1 in application mode: job {job[:8]} RUNNING (ConfigMap {cm})")
    it.produce("orders", [
        it.order("o1", "c1", 10.00, "10:00:05"),
        it.order("o2", "c2", 20.00, "10:00:10"),
        it.order("o3", "c1", -5.00, "10:00:20"),
        it.order("o4", "c1", 5.00, "10:00:30"),
        it.order("o5", "c2", 1.00, "10:02:00"),
    ])
    it.expect("JDBC upsert sink (Postgres customer_ltv)", it.ltv_table, {"c1": (15.0, 2), "c2": (21.0, 2)},
              timeout=180)
    it.expect("upsert-kafka sink", it.ltv_topic, {"c1": (15.0, 2), "c2": (21.0, 2)})
    it.expect("append Kafka sink (revenue)", it.revenue,
              {("10:00", "smb"): (15.0, 2), ("10:00", "enterprise"): (20.0, 1)})
    return job


def phase_upgrade(v1_job: str):
    step("upgrade: kubectl apply -k of v2; the operator does stop-with-savepoint + restore")
    it.change_customer_before_upgrade()
    v2 = it.write_v2()
    cm = apply_version("v2", v2, previous="v1")
    it.check_upgrade_report()
    job = wait_running(previous_job=v1_job, timeout=600)
    st = deployment()["status"]
    sp = (st.get("jobStatus", {}).get("savepointInfo", {}) or {}).get("lastSavepoint") or {}
    upgrade = st.get("jobStatus", {}).get("upgradeSavepointPath") or sp.get("location")
    spec_cm = deployment()["spec"]["podTemplate"]["metadata"]["annotations"]["cascade.dev/job-config"]
    if spec_cm != cm:
        raise Failed(f"FlinkDeployment still points at {spec_cm}, expected {cm}")
    ok(f"operator upgraded to v2: job {job[:8]} RUNNING from savepoint {upgrade or '(see status)'}")
    it.check_after_upgrade()


def phase_v3(v2_job: str):
    step("v3: two backfilled models, rescale 1 -> 2, state TTL 1d -> 2d, through the operator")
    v3 = it.write_v3()
    apply_version("v3", v3, previous="v2")
    it.check_v3_report()
    job = wait_running(previous_job=v2_job, timeout=600)
    par = deployment()["spec"]["job"]["parallelism"]
    if par != 2:
        raise Failed(f"FlinkDeployment parallelism is {par}, expected 2")
    ok(f"operator upgraded to v3 at parallelism 2: job {job[:8]} RUNNING")
    it.check_after_v3()


def phase_session():
    """deploy.target: session through the operator: a session cluster (FlinkDeployment without a job), the
    runner jar served over HTTP in the cluster, and the FlinkSessionJob `cascade deploy` writes."""
    step("session target: FlinkSessionJob on an operator-managed session cluster")
    kubectl("delete", "flinkdeployment", "shop-it", "--wait=true", "--timeout=180s", check=False)  # free the node
    import base64
    sys.path.insert(0, it.REPO)
    from cascade_dsl.deploy import _operator_version
    with open(os.path.join(it.REPO, "runtime", "target", "cascade-runtime.jar"), "rb") as f:
        jar = base64.b64encode(f.read()).decode()
    server = [
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "jar-server"},
         "binaryData": {"cascade-runtime.jar": jar}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "jar-server"},
         "spec": {"selector": {"matchLabels": {"app": "jar-server"}},
                  "template": {"metadata": {"labels": {"app": "jar-server"}},
                               "spec": {"containers": [{"name": "http", "image": "python:3.12-alpine",
                                                        "command": ["python", "-m", "http.server", "8000", "-d", "/srv"],
                                                        "ports": [{"containerPort": 8000}],
                                                        "volumeMounts": [{"name": "jar", "mountPath": "/srv"}]}],
                                        "volumes": [{"name": "jar", "configMap": {"name": "jar-server"}}]}}}},
        {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "jar-server"},
         "spec": {"selector": {"app": "jar-server"}, "ports": [{"port": 80, "targetPort": 8000}]}},
        {"apiVersion": "flink.apache.org/v1beta1", "kind": "FlinkDeployment", "metadata": {"name": "shared-flink"},
         "spec": {"image": "cascade-it:it", "flinkVersion": _operator_version("2.3.0"), "serviceAccount": "flink",
                  "flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"},
                  "podTemplate": {"spec": {"containers": [{"name": "flink-main-container", "volumeMounts": [
                      {"name": "flink-data", "mountPath": "/flink-data"}]}],
                      "volumes": [{"name": "flink-data", "hostPath": {"path": "/flink-data", "type": "Directory"}}]}},
                  "jobManager": {"resource": {"memory": "1024m", "cpu": 0.25}},
                  "taskManager": {"resource": {"memory": "1536m", "cpu": 0.5}}}},
    ]
    kubectl("apply", "-f", "-", input=yaml.safe_dump_all(server))
    kubectl("rollout", "status", "deploy/jar-server", "--timeout=180s")
    # the operator fetches session-job jars over https only by default; the test serves plain http
    cm = json.loads(kubectl("get", "configmap", "flink-operator-config", "-o", "json"))
    key = next(k for k in cm["data"] if k in ("config.yaml", "flink-conf.yaml"))
    if "allowed-schemes" not in cm["data"][key]:
        # and refuses hosts that resolve to private addresses (SSRF protection, FLINK-40400): the test's jar
        # server is in-cluster on purpose, so the test relaxes both
        cm["data"][key] += ("\nkubernetes.operator.user.artifacts.allowed-schemes: https;http\n"
                            "kubernetes.operator.user.artifacts.disallow-restricted-hosts: false\n")
        kubectl("apply", "-f", "-", input=json.dumps(cm))
        kubectl("rollout", "restart", "deploy/flink-kubernetes-operator")
        kubectl("rollout", "status", "deploy/flink-kubernetes-operator", "--timeout=180s")

    def session_ready():
        out = kubectl("get", "flinkdeployment", "shared-flink", "-o", "json", check=False)
        st = json.loads(out).get("status", {}) if out.strip().startswith("{") else {}
        return st.get("jobManagerDeploymentStatus") == "READY"
    it.wait_for("session cluster READY", session_ready, timeout=300, every=5)
    ok("session cluster shared-flink ready; cascade-runtime.jar served at http://jar-server")

    d = os.path.join(it.WORK, "pipeline-session")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    with open(os.path.join(d, "pipeline.yaml"), "w") as f:
        f.write("""
pipeline: session_probe
deploy: {target: session, session_cluster: shared-flink, jar_uri: "http://jar-server/cascade-runtime.jar",
         checkpoints: {interval: 10s, dir: "file:///flink-data/checkpoints"}, savepoints: "file:///flink-data/savepoints"}
sources:
  gen: {connection: datagen, rows_per_second: 5, columns: {k: INT, v: STRING}}
models:
  m: {sql: "SELECT k, UPPER(v) AS v FROM gen"}
sinks:
  out: {from: m, connection: blackhole}
""")
    out = os.path.join(it.WORK, "deploy-session")
    it.cascade("deploy", os.path.relpath(d, it.REPO), "-o", out, flink=False)
    kubectl("apply", "-k", out)

    def job_running():
        out_ = kubectl("get", "flinksessionjob", "session-probe", "-o", "json", check=False)
        st = json.loads(out_).get("status", {}) if out_.strip().startswith("{") else {}
        if (st.get("error") or "") and "Exception" in st.get("error", ""):
            raise RuntimeError("FlinkSessionJob failed: " + st["error"][-2000:])
        return st.get("jobStatus", {}).get("state") == "RUNNING"
    it.wait_for("FlinkSessionJob RUNNING", job_running, timeout=300, every=5)
    ok("FlinkSessionJob session-probe RUNNING: the operator fetched the runner, the scripts came inline")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the kind cluster running")
    ap.add_argument("--skip-build", action="store_true", help="reuse the image and local Flink home")
    a = ap.parse_args()
    for tool in ("kind", "kubectl", "helm"):
        if not shutil.which(tool):
            print(f"{tool} not found (install it, e.g. into ~/.local/bin)", file=sys.stderr)
            sys.exit(2)
    it.EXEC["fn"] = exec_pod
    started = time.time()
    code = 1
    try:
        it.build(a.skip_build)
        os.makedirs(os.path.join(it.WORK, "jobs"), exist_ok=True)
        cluster()
        v1 = phase_v1()
        phase_upgrade(v1)
        phase_v3(job_state()[1])
        phase_session()
        print(f"\nPASSED in {time.time() - started:.0f}s")
        code = 0
    except (Failed, RuntimeError) as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
    finally:
        if not a.keep:
            it.run(["kind", "delete", "cluster", "--name", CLUSTER], check=False)
    sys.exit(code)


if __name__ == "__main__":
    main()
