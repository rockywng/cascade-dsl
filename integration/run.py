"""End-to-end integration run against real Kafka, Postgres (CDC + JDBC) and a Flink 2.3 cluster.

    python3 integration/run.py [--keep] [--skip-build]

Phases:
  1. build: runtime jars against the cluster's Flink, the runtime image from the Dockerfile that
     `cascade deploy` generates, and a local Flink home with the image's connector jars (plans
     must be compiled by the same Flink version, with the real connectors)
  2. v1: `cascade plan` + `cascade deploy --plan-dir`, submit through dev.cascade.runner.SqlRunner
     with the secret from the environment, produce orders, check every sink from the outside
  3. upgrade: stop with a savepoint, plan v2 (adds a model) pinned to v1, restore it strictly,
     change a customer in Postgres, produce more orders. Checks that keyed state (LTV totals),
     Kafka offsets (no order counted twice) and the CDC position (the update is seen, nothing
     re-snapshotted) all survived.

Needs Docker (with compose) and Java 17. Everything runs under integration/.work.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WORK = os.path.join(HERE, ".work")
FLINK_BASE = os.path.join(WORK, "flink-base")  # pristine flink:2.3.0 /opt/flink
FLINK_HOME = os.path.join(WORK, "flink")  # same, plus the runtime image's connector jars
BASE_IMAGE = "flink:2.3.0-scala_2.12-java17"
SECRET = "flinkpw"


class Failed(Exception):
    pass


LAST_REPORT = {"text": ""}  # output of the last `cascade plan`
TOPICS = ("orders", "revenue", "customer_ltv", "orders_per_segment", "orders_per_customer", "orders_per_segment_bf")


# --- shell helpers ------------------------------------------------------------------------

def _docker_prefix() -> list[str]:
    """Run docker directly, or through `sg docker` if this shell predates joining the group."""
    sock = "/var/run/docker.sock"
    if os.path.exists(sock) and not os.access(sock, os.W_OK) and shutil.which("sg"):
        return ["sg", "docker", "-c"]
    return []


def run(cmd: list[str], check=True, capture=True, input=None, env=None, timeout=900) -> str:
    if cmd[0] in ("docker", "kind") and _docker_prefix():
        cmd = _docker_prefix() + [shlex.join(cmd)]
    proc = subprocess.run(cmd, capture_output=capture, text=True, input=input, timeout=timeout,
                          env={**os.environ, **(env or {})}, cwd=REPO)
    if check and proc.returncode != 0:
        raise Failed(f"command failed ({proc.returncode}): {shlex.join(cmd)}\n"
                     f"{(proc.stdout or '')[-2000:]}\n{(proc.stderr or '')[-3000:]}")
    return (proc.stdout or "") + (proc.stderr or "" if not check else "")


def compose(*args, **kw) -> str:
    return run(["docker", "compose", "-f", os.path.join(HERE, "docker-compose.yml"), *args], **kw)


def cascade(*args, flink=True) -> str:
    env = {"FLINK_HOME": FLINK_HOME} if flink else {}
    return run([sys.executable, "-m", "cascade_dsl", *args], env={**env, "PYTHONPATH": REPO})


def step(msg: str):
    print(f"\n== {msg}", flush=True)


def ok(msg: str):
    print(f"   ok  {msg}", flush=True)


def wait_for(what: str, fn, timeout=120, every=2):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            result = fn()
            if result:
                return result
        except Failed as e:
            last = e
        time.sleep(every)
    raise Failed(f"timed out waiting for {what}" + (f": {last}" if last else ""))


# --- systems ------------------------------------------------------------------------------

def exec_compose(service: str, *args, input=None, check=True) -> str:
    return compose("exec", "-T", service, *args, input=input, check=check)


# where Kafka/Postgres commands run: the compose stack, or pods in kind (k8s.py swaps this)
EXEC = {"fn": exec_compose}


def exec_in(service: str, *args, input=None, check=True) -> str:
    return EXEC["fn"](service, *args, input=input, check=check)


def kafka(*args, input=None) -> str:
    return exec_in("kafka", *args, input=input)


def produce(topic: str, rows: list[dict]):
    kafka("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "kafka:9092", "--topic", topic,
          input="\n".join(json.dumps(r) for r in rows) + "\n")


def consume(topic: str, keys: bool = False) -> list:
    args = ["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092", "--topic", topic,
            "--from-beginning", "--timeout-ms", "4000"]
    if keys:
        args += ["--property", "print.key=true", "--property", "key.separator=|"]
    out = exec_in("kafka", *args, check=False)
    rows = []
    for line in out.splitlines():
        line = line.strip()
        if keys and "|" in line and line.startswith("{"):
            k, _, v = line.partition("|")
            rows.append((json.loads(k), json.loads(v) if v != "null" else None))
        elif not keys and line.startswith("{"):
            rows.append(json.loads(line))
    return rows


def psql(sql: str) -> list[list[str]]:
    out = exec_in("postgres", "psql", "-U", "flink", "-d", "shop", "-At", "-F", "|", "-c", sql)
    return [line.split("|") for line in out.strip().splitlines() if line]


def flink_cli(*args, env: dict | None = None, check=True) -> str:
    e = []
    for k, v in (env or {}).items():
        e += ["-e", f"{k}={v}"]
    return compose("exec", "-T", *e, "jobmanager", "/opt/flink/bin/flink", *args, check=check)


def running_jobs() -> list[str]:
    out = flink_cli("list", "-r", check=False)
    return re.findall(r": ([0-9a-f]{32}) : .* \(RUNNING\)", out)


# --- phases -------------------------------------------------------------------------------

def build(skip: bool):
    step("build: runtime jars, runtime image, local Flink home")
    os.makedirs(WORK, exist_ok=True)
    if not os.path.exists(os.path.join(FLINK_BASE, "lib")):
        run(["docker", "pull", "-q", BASE_IMAGE])
        run(["docker", "rm", "-f", "cascade-it-extract"], check=False)
        run(["docker", "create", "--name", "cascade-it-extract", BASE_IMAGE])
        run(["docker", "cp", "cascade-it-extract:/opt/flink", FLINK_BASE])
        run(["docker", "rm", "cascade-it-extract"])
    if skip and os.path.exists(os.path.join(FLINK_HOME, "lib")):
        ok("skipped (--skip-build)")
        return
    run([os.path.join(REPO, "runtime", "build.sh")], env={"FLINK_HOME": FLINK_BASE})
    ok("runtime/target built against Flink 2.3.0")
    out = os.path.join(WORK, "deploy-dockerfile")
    cascade("deploy", "integration/pipeline", "-o", out, flink=False)
    shutil.copy(os.path.join(out, "Dockerfile"), os.path.join(WORK, "Dockerfile"))
    compose("build", "jobmanager", timeout=1800)
    ok("image cascade-it:it built from the generated Dockerfile")
    shutil.rmtree(FLINK_HOME, ignore_errors=True)
    shutil.copytree(FLINK_BASE, FLINK_HOME, symlinks=True)
    run(["docker", "rm", "-f", "cascade-it-extract"], check=False)
    run(["docker", "create", "--name", "cascade-it-extract", "cascade-it:it"])
    run(["docker", "cp", "cascade-it-extract:/opt/flink/lib/.", os.path.join(FLINK_HOME, "lib")])
    run(["docker", "rm", "cascade-it-extract"])
    jars = sorted(j for j in os.listdir(os.path.join(FLINK_HOME, "lib")) if "connector" in j or "postgres" in j)
    ok("local Flink home with " + ", ".join(jars))


def start():
    step("start: Kafka, Postgres, Flink session cluster")
    compose("down", "-v", "--remove-orphans", check=False)
    data = os.path.join(WORK, "flink-data")
    if os.path.exists(data):  # files from a previous run belong to the containers' flink user
        run(["docker", "run", "--rm", "--user", "root", "-v", f"{data}:/d", BASE_IMAGE, "sh", "-c", "rm -rf /d/*"])
    os.makedirs(data, exist_ok=True)
    os.chmod(data, 0o777)  # the Flink containers run as uid 9999
    os.makedirs(os.path.join(WORK, "jobs"), exist_ok=True)
    compose("up", "-d", "--wait", "kafka", "postgres", timeout=600)
    for topic in TOPICS:
        kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--create", "--if-not-exists",
              "--topic", topic, "--partitions", "1", "--replication-factor", "1")
    compose("up", "-d", "jobmanager", "taskmanager")
    wait_for("Flink cluster", lambda: "Running/Restarting Jobs" in flink_cli("list", "-r", check=False)
             or "No running jobs" in flink_cli("list", "-r", check=False), timeout=180)
    ok("cluster up; customers seeded in Postgres: " + str(psql("SELECT customer_id, segment FROM customers "
                                                                "ORDER BY 1")))


def package(version: str, project: str, previous: str | None = None) -> str:
    """cascade plan + cascade deploy --plan-dir; returns the job directory (inside the containers: /jobs/<v>)."""
    plan_dir = os.path.join(WORK, f"plan-{version}")
    args = ["plan", project, "-o", plan_dir]
    if previous:
        args += ["--previous", os.path.join(WORK, f"plan-{previous}", "plan.json")]
    report = cascade(*args)
    LAST_REPORT["text"] = report
    deploy_dir = os.path.join(WORK, f"deploy-{version}")
    shutil.rmtree(deploy_dir, ignore_errors=True)
    cascade("deploy", project, "-o", deploy_dir, "--plan-dir", plan_dir, flink=False)
    with open(os.path.join(deploy_dir, "configmap.yaml")) as f:
        files = yaml.safe_load(f)["data"]
    with open(os.path.join(deploy_dir, "flinkdeployment.yaml")) as f:
        fd = yaml.safe_load(f)
    job = os.path.join(WORK, "jobs", version)
    shutil.rmtree(job, ignore_errors=True)
    os.makedirs(job)
    for name, text in files.items():
        with open(os.path.join(job, name), "w") as f:
            f.write(text)
    assert "${secret:crm/password}" in files["setup.sql"] and SECRET not in files["setup.sql"]
    ok(f"{version}: plan.json + setup.sql from the generated ConfigMap (secret stays a placeholder)")
    if previous:
        for line in report.splitlines():
            if any(w in line for w in ("UID", "operator", "stateful", "reads restored", "fix:", "dropped")):
                print("      " + line.strip())
    return fd["spec"]["job"]["args"]


def submit(version: str, args: list[str], savepoint: str | None = None, expect_secret_error=False,
           env: dict | None = None):
    args = [a.replace("/opt/cascade/job", f"/jobs/{version}") for a in args]
    sp = ["-s", savepoint] if savepoint else []
    if expect_secret_error:
        out = flink_cli("run", "-d", *sp, "-c", "dev.cascade.runner.SqlRunner",
                        "/opt/flink/usrlib/cascade-runtime.jar", *args, check=False)
        if "missing secrets: crm/password (CASCADE_SECRET_CRM_PASSWORD)" not in out:
            raise Failed("runner did not refuse to start without the secret:\n" + out[-2000:])
        ok("without CASCADE_SECRET_CRM_PASSWORD the runner refuses to start and names the secret")
        return
    flink_cli("run", "-d", *sp, "-c", "dev.cascade.runner.SqlRunner", "/opt/flink/usrlib/cascade-runtime.jar",
              *args, env={"CASCADE_SECRET_CRM_PASSWORD": SECRET, **(env or {})})
    jobs = wait_for("job RUNNING", running_jobs, timeout=90)
    ok(f"{version} submitted through SqlRunner" + (f" from {savepoint.rsplit('/', 1)[-1]}" if savepoint else "")
       + f": job {jobs[0][:8]} RUNNING")
    return jobs[0]


def order(oid, cust, amount, ts):
    return {"order_id": oid, "customer_id": cust, "amount": amount, "order_ts": f"2026-01-01 {ts}"}


def ltv_table() -> dict:
    return {r[0]: (float(r[1]), int(r[2])) for r in psql("SELECT * FROM customer_ltv ORDER BY 1")}


def ltv_topic() -> dict:
    final = {}
    for k, v in consume("customer_ltv", keys=True):
        if v is None:
            final.pop(k["customer_id"], None)
        else:
            final[k["customer_id"]] = (float(v["lifetime_value"]), int(v["order_count"]))
    return final


def revenue() -> dict:
    out = {}
    for r in consume("revenue"):
        key = (r["window_start"][11:16], r["segment"])
        if key in out:
            raise Failed(f"revenue window {key} emitted twice: {out[key]} and {r}")
        out[key] = (float(r["revenue"]), int(r["orders"]))
    return out


def expect(what: str, actual_fn, expected, timeout=120):
    last = [None]

    def check():
        last[0] = actual_fn()
        return last[0] == expected
    try:
        wait_for(what, check, timeout=timeout, every=3)
    except Failed:
        raise Failed(f"{what}:\n   expected {expected}\n   actual   {last[0]}")
    ok(f"{what}: {expected}")


def phase_v1():
    step("v1: plan, deploy, run on real connectors")
    args = package("v1", "integration/pipeline")
    submit("v1", args, expect_secret_error=True)
    submit("v1", args)
    produce("orders", [
        order("o1", "c1", 10.00, "10:00:05"),
        order("o2", "c2", 20.00, "10:00:10"),
        order("o3", "c1", -5.00, "10:00:20"),   # dropped by the positive_amount expectation
        order("o4", "c1", 5.00, "10:00:30"),
        order("o5", "c2", 1.00, "10:02:00"),    # advances event time past the 10:00 window
    ])
    expect("JDBC upsert sink (Postgres customer_ltv)", ltv_table, {"c1": (15.0, 2), "c2": (21.0, 2)})
    expect("upsert-kafka sink (customer_ltv, latest per key)", ltv_topic, {"c1": (15.0, 2), "c2": (21.0, 2)})
    expect("append Kafka sink (revenue, enriched via CDC temporal join)", revenue,
           {("10:00", "smb"): (15.0, 2), ("10:00", "enterprise"): (20.0, 1)})


def write_v2() -> str:
    src = os.path.join(HERE, "pipeline", "pipeline.yaml")
    dst_dir = os.path.join(WORK, "pipeline-v2")
    shutil.rmtree(dst_dir, ignore_errors=True)
    os.makedirs(dst_dir)
    with open(src) as f:
        text = f.read()
    text = text.replace("\nsinks:\n", """
  orders_per_segment:
    sql: |
      SELECT segment, COUNT(*) AS orders FROM orders_enriched GROUP BY segment

sinks:
  segment_counts:
    from: orders_per_segment
    connection: bus
    topic: orders_per_segment
    mode: upsert
    primary_key: [segment]
""")
    with open(os.path.join(dst_dir, "pipeline.yaml"), "w") as f:
        f.write(text)
    return os.path.relpath(dst_dir, REPO)


def change_customer_before_upgrade():
    """Customer c1 changes while v1 runs; v1 consumes the change before the savepoint."""
    psql("UPDATE customers SET segment = 'enterprise', updated_at = '2026-01-01 10:02:30' WHERE customer_id = 'c1'")
    # a later, unrelated transaction: Postgres CDC may redeliver the last transaction before the
    # savepoint after a restore (at-least-once), which would hand c1's change to the new join
    psql("INSERT INTO customers VALUES ('c3', 'smb', '2026-01-01 10:02:40')")
    time.sleep(10)


def check_upgrade_report():
    """v2's new model makes the planner build a second TemporalJoin, which starts empty while the CDC
    source resumes from its saved position: `cascade plan` must say so."""
    text = LAST_REPORT["text"]
    if "TemporalJoin" not in text or "lookups miss" not in text or "backfill: true` on model 'orders_per_segment'" not in text:
        raise Failed("cascade plan did not warn about the new, empty TemporalJoin:\n" + text)
    ok("cascade plan warned: the new TemporalJoin starts empty and will miss unchanged customers")


def check_after_upgrade():
    produce("orders", [
        order("o6", "c1", 100.00, "10:03:10"),
        order("o7", "c2", 2.00, "10:05:00"),    # closes the 10:03 window
    ])
    # keyed state restored: totals continue from v1; offsets restored: o1..o5 not re-read
    expect("LTV continues from restored state, no order read twice", ltv_table,
           {"c1": (115.0, 3), "c2": (23.0, 3)}, timeout=180)
    expect("upsert-kafka sink after upgrade", ltv_topic, {"c1": (115.0, 3), "c2": (23.0, 3)})
    # the restored TemporalJoin knows c1's update from before the upgrade
    expect("restored temporal join enriches with the pre-upgrade CDC update", revenue,
           {("10:00", "smb"): (15.0, 2), ("10:00", "enterprise"): (20.0, 1),
            ("10:02", "enterprise"): (1.0, 1), ("10:03", "enterprise"): (100.0, 1)})
    # the new model's own TemporalJoin started empty: exactly the hazard the plan report warned about
    # (o7 at 10:05:00 is still buffered: the watermark is 10:04:58)
    counts = lambda: {k["segment"]: v["orders"] for k, v in consume("orders_per_segment", keys=True) if v}  # noqa
    expect("new model's empty TemporalJoin misses c1 (as warned)", counts, {"unknown": 1})


def phase_upgrade(job_id: str):
    step("upgrade: savepoint v1, restore v2 (adds a model) with pinned UIDs")
    v2 = write_v2()
    args = package("v2", v2, previous="v1")
    check_upgrade_report()
    change_customer_before_upgrade()
    out = flink_cli("stop", "--savepointPath", "file:///flink-data/savepoints", job_id)
    m = re.search(r"Savepoint completed\. Path: (\S+)", out)
    if not m:
        raise Failed("stop-with-savepoint failed:\n" + out[-2000:])
    savepoint = m.group(1)
    ok(f"v1 stopped with savepoint {savepoint.rsplit('/', 1)[-1]}")
    submit("v2", args, savepoint=savepoint)  # strict: no --allowNonRestoredState
    check_after_upgrade()


def write_v3() -> str:
    """v3 combines upgrade kinds: two backfilled models, a rescale (1 -> 2) and a longer state TTL."""
    src = os.path.join(WORK, "pipeline-v2", "pipeline.yaml")
    dst_dir = os.path.join(WORK, "pipeline-v3")
    shutil.rmtree(dst_dir, ignore_errors=True)
    os.makedirs(dst_dir)
    with open(src) as f:
        text = f.read()
    text = text.replace("defaults: {state_ttl: 1d}", "defaults: {state_ttl: 2d}")
    text = text.replace("scaling: {max_parallelism: 120, parallelism: 1}",
                        "scaling: {max_parallelism: 120, parallelism: 2}")
    text = text.replace("\nsinks:\n", """
  orders_per_customer:
    backfill: true
    sql: SELECT customer_id, COUNT(*) AS orders FROM clean_orders GROUP BY customer_id

  orders_per_segment_bf:
    backfill: true
    sql: |
      SELECT segment, COUNT(*) AS orders FROM orders_enriched GROUP BY segment

sinks:
  customer_counts:
    from: orders_per_customer
    connection: bus
    topic: orders_per_customer
    mode: upsert
    primary_key: [customer_id]
  segment_counts_bf:
    from: orders_per_segment_bf
    connection: bus
    topic: orders_per_segment_bf
    mode: upsert
    primary_key: [segment]
""")
    with open(os.path.join(dst_dir, "pipeline.yaml"), "w") as f:
        f.write(text)
    return os.path.relpath(dst_dir, REPO)


def check_v3_report():
    text = LAST_REPORT["text"]
    v3 = run([sys.executable, "-m", "cascade_dsl", "validate", os.path.relpath(os.path.join(WORK, "pipeline-v3"), REPO)],
             check=False, env={"PYTHONPATH": REPO})  # diagnostics go to stderr
    if "CD372" not in v3:
        raise Failed("expected the CD372 warning about backfilling a CDC temporal join:\n" + v3)
    ok("cascade validate warns (CD372) that the backfilled CDC temporal join can race the snapshot")
    if "WARNING: new operators" in text:
        raise Failed("backfilled models should not need pre-upgrade input:\n" + text)
    if "state dropped" in text:
        raise Failed("v3 should keep every operator's state:\n" + text)
    ok("cascade plan: every v2 operator keeps its UID; the backfilled models need no pre-upgrade input")


def check_after_v3():
    produce("orders", [order("o8", "c3", 50.00, "10:06:00")])
    expect("LTV continues across the rescale to parallelism 2", ltv_table,
           {"c1": (115.0, 3), "c2": (23.0, 3), "c3": (50.0, 1)}, timeout=180)
    # Kafka-only backfill is exact: every order since the beginning (o3 fails the expectation)
    per_customer = lambda: {k["customer_id"]: v["orders"] for k, v in consume("orders_per_customer", keys=True) if v}  # noqa
    expect("backfilled model counts every historical order", per_customer, {"c1": 3, "c2": 3, "c3": 1})
    # The backfilled temporal join replays history through a CDC snapshot. With the pipeline's idle
    # timeout, orders can be joined before the snapshot arrives (CD372), so the segment split depends
    # on timing (observed: compose 2 unknown / 4 enterprise, kind 5 / 1). Every order is counted
    # exactly once either way; o8 waits for the watermark.
    def total():  # final value per key (the upsert topic carries every update)
        final = {k["segment"]: (v["orders"] if v else 0) for k, v in consume("orders_per_segment_bf", keys=True)}
        return sum(final.values())
    # (v3 runs 2 orders readers on 1 partition: the empty one stands down after its 60 s idle timeout)
    expect("backfilled temporal join counts every historical order once", total, 6, timeout=300)


def phase_v3(job_id: str):
    step("v3: two backfilled models, rescale 1 -> 2, state TTL 1d -> 2d")
    v3 = write_v3()
    args = package("v3", v3, previous="v2")
    check_v3_report()
    out = flink_cli("stop", "--savepointPath", "file:///flink-data/savepoints", job_id)
    m = re.search(r"Savepoint completed\. Path: (\S+)", out)
    if not m:
        raise Failed("stop-with-savepoint failed:\n" + out[-2000:])
    submit("v3", args, savepoint=m.group(1))
    check_after_v3()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the stack running")
    ap.add_argument("--skip-build", action="store_true", help="reuse the image and local Flink home")
    a = ap.parse_args()
    started = time.time()
    try:
        build(a.skip_build)
        start()
        phase_v1()
        phase_upgrade(running_jobs()[0])
        phase_v3(running_jobs()[0])
        print(f"\nPASSED in {time.time() - started:.0f}s")
        code = 0
    except Failed as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        try:
            print(compose("logs", "--tail", "60", "jobmanager", "taskmanager", check=False)[-6000:], file=sys.stderr)
        except Exception:  # noqa: BLE001
            pass
        code = 1
    finally:
        if not a.keep:
            compose("down", "-v", "--remove-orphans", check=False)
    sys.exit(code)


if __name__ == "__main__":
    main()
