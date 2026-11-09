"""Soak and failure-injection run: the integration pipeline under continuous load for a while, with
failures injected, and every result checked exactly against ground truth afterwards.

    python3 integration/soak.py [--minutes 20] [--rate 100] [--keep] [--skip-build]

Load: orders streamed into Kafka at --rate per second; event time advances 100 ms per order, so
one-minute windows close every few seconds. Customers change in Postgres every few seconds (CDC).

Failures, spread over the run: TaskManager killed; Kafka broker restarted; Postgres restarted (the
CDC connection drops); stop-with-savepoint and resubmit under load; TaskManager killed twice in a
row. After each, the job must be RUNNING again.

Ground truth is what actually landed in the `orders` topic (a producer can lose or duplicate
messages while a broker restarts; the job sees exactly what is in the topic). After the run:
  - LTV per customer, in Postgres (JDBC upsert) and in the upsert-kafka topic, must equal the sum
    and count of positive orders: exactly-once state through every failure
  - the revenue sink writes exactly-once (Kafka transactions). Read with read_committed, every
    closed window appears once per segment, and its total equals the orders in that window
Segment attribution is not checked: it depends on when CDC updates arrive relative to orders.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

import run as it
from run import Failed, ok, step

BASE = datetime(2026, 1, 1, 10, 0, 0)
CUSTOMERS = [f"c{i}" for i in range(50)]
PROBE = "probe"  # --cdc-edge-cases: a customer whose CDC change is made while the replication slot is gone
SLOT = "shop_it_customers"  # the Postgres CDC slot cascade derives for source customers of shop_it
SEGMENTS = ["smb", "enterprise", "mid"]


# --- pipeline ---------------------------------------------------------------------------------

def write_pipeline(delivery: str = "exactly-once", parallelism: int = 1, heal: bool = False) -> str:
    """The integration pipeline, with the revenue sink writing exactly-once (Kafka transactions).
    `heal`: the customers source re-snapshots on slot loss, and customers per segment go to Postgres."""
    with open(os.path.join(it.HERE, "pipeline", "pipeline.yaml")) as f:
        text = f.read()
    if heal:
        text = text.replace("    primary_key: [customer_id]\n    # the CDC source",
                            "    primary_key: [customer_id]\n    on_slot_loss: resnapshot\n    # the CDC source", 1)
        text = text.replace("\nsinks:\n", """
  customers_per_segment:
    sql: SELECT COALESCE(segment, 'none') AS segment, COUNT(*) AS customers FROM customers GROUP BY COALESCE(segment, 'none')

sinks:
  segment_counts:
    from: customers_per_segment
    connection: warehouse
    table: segment_counts
    mode: upsert
    primary_key: [segment]
""", 1)
        assert "on_slot_loss: resnapshot" in text and "segment_counts:" in text
    text = text.replace("    topic: revenue\n", f"    topic: revenue\n    delivery: {delivery}\n")
    text = text.replace("parallelism: 1}", f"parallelism: {parallelism}}}")
    d = os.path.join(it.WORK, "pipeline-soak" + ("" if parallelism == 1 else f"-p{parallelism}"))
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    with open(os.path.join(d, "pipeline.yaml"), "w") as f:
        f.write(text)
    return os.path.relpath(d, it.REPO)


# --- load -------------------------------------------------------------------------------------

class Load:
    """Streams orders into Kafka through a long-lived console producer; restarts it if it dies."""

    def __init__(self, rate: int):
        self.rate = rate
        self.seq = 0
        self.stop = threading.Event()
        self.restarts = 0
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def order(self, i: int) -> dict:
        ts = BASE + timedelta(milliseconds=100 * i)
        amount = Decimal(-5) if i % 97 == 0 else Decimal(i % 50 + 1) + Decimal("0.25")
        return {"order_id": f"o{i}", "customer_id": CUSTOMERS[i % len(CUSTOMERS)], "amount": float(amount),
                "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}

    def _producer(self):
        cmd = ["docker", "compose", "-f", os.path.join(it.HERE, "docker-compose.yml"), "exec", "-T", "kafka",
               "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "kafka:9092", "--topic", "orders",
               "--producer-property", "acks=all", "--producer-property", "enable.idempotence=true"]
        if it._docker_prefix():
            cmd = it._docker_prefix() + [" ".join(cmd)]
        return subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                text=True, cwd=it.REPO)

    def _loop(self):
        proc = self._producer()
        next_at = time.time()
        while not self.stop.is_set():
            batch = max(1, self.rate // 10)
            try:
                proc.stdin.write("".join(json.dumps(self.order(self.seq + k)) + "\n" for k in range(batch)))
                proc.stdin.flush()
                self.seq += batch
            except (BrokenPipeError, OSError, ValueError):
                self.restarts += 1
                time.sleep(3)
                proc = self._producer()
                continue
            if proc.poll() is not None:
                self.restarts += 1
                time.sleep(3)
                proc = self._producer()
            next_at += 0.1
            time.sleep(max(0.0, next_at - time.time()))
        try:
            proc.stdin.close()
            proc.wait(timeout=30)
        except Exception:  # noqa: BLE001
            proc.kill()

    def event_time(self) -> datetime:
        return BASE + timedelta(milliseconds=100 * self.seq)


class CustomerChanges:
    """Changes a random customer's segment every few seconds, at the current event time."""

    def __init__(self, load: Load):
        self.load = load
        self.stop = threading.Event()
        self.count = 0
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        i = 0
        while not self.stop.wait(4):
            c = CUSTOMERS[(i * 7) % len(CUSTOMERS)]
            if c == PROBE:
                i += 1
                continue  # the slot-loss probe's segment is set only by that step
            seg = SEGMENTS[i % len(SEGMENTS)]
            ts = self.load.event_time().strftime("%Y-%m-%d %H:%M:%S")
            try:
                it.psql(f"INSERT INTO customers VALUES ('{c}', '{seg}', '{ts}') ON CONFLICT (customer_id) "
                        f"DO UPDATE SET segment = EXCLUDED.segment, updated_at = EXCLUDED.updated_at")
                self.count += 1
            except Failed:
                pass  # Postgres is being restarted
            i += 1


# --- Flink REST -------------------------------------------------------------------------------

def rest(path: str):
    with urllib.request.urlopen(f"http://localhost:8081{path}", timeout=10) as r:
        return json.loads(r.read())


def job_info() -> dict:
    jobs = [j for j in rest("/jobs/overview")["jobs"] if j["state"] in ("RUNNING", "RESTARTING", "CREATED")]
    if not jobs:
        return {}
    jid = jobs[0]["jid"]
    cp = rest(f"/jobs/{jid}/checkpoints")
    return {"jid": jid, "state": jobs[0]["state"], "completed": cp["counts"]["completed"],
            "failed": cp["counts"]["failed"], "restored": cp["counts"].get("restored", 0),
            "last_cp_ms": (cp.get("latest", {}).get("completed") or {}).get("end_to_end_duration")}


def wait_healthy(what: str, since_checkpoints: int | None = None, timeout=240):
    """RUNNING again, with all tasks running and a new completed checkpoint."""
    def check():
        info = job_info()
        if info.get("state") != "RUNNING":
            return None
        vertices = rest(f"/jobs/{info['jid']}")["vertices"]
        if any(v["status"] != "RUNNING" for v in vertices):
            return None
        if since_checkpoints is not None and info["completed"] <= since_checkpoints:
            return None
        return info
    info = it.wait_for(what, check, timeout=timeout, every=3)
    ok(f"{what}: RUNNING, {info['completed']} checkpoints completed, last took {info['last_cp_ms']} ms")
    return info


class Sampler:
    """Every minute: checkpoint size and duration, TaskManager heap. Slow growth shows up here."""

    def __init__(self):
        self.samples: list[dict] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        t0 = time.time()
        while not self.stop.wait(60):
            try:
                info = job_info()
                cp = rest(f"/jobs/{info['jid']}/checkpoints").get("latest", {}).get("completed") or {}
                heap = 0
                for tm in rest("/taskmanagers")["taskmanagers"]:
                    m = rest(f"/taskmanagers/{tm['id']}/metrics?get=Status.JVM.Memory.Heap.Used")
                    heap += int(float(m[0]["value"])) if m else 0
                self.samples.append({"min": round((time.time() - t0) / 60), "state_bytes": cp.get("state_size")
                                     or cp.get("checkpointed_size"), "cp_ms": cp.get("end_to_end_duration"),
                                     "heap_mb": heap // (1 << 20)})
            except Exception:  # noqa: BLE001 -- the cluster may be mid-failure
                continue

    def report(self):
        s = [x for x in self.samples if x["state_bytes"]]
        if len(s) < 4:
            ok(f"health samples: {len(s)} (too few for a trend)")
            return
        q = len(s) // 4
        first, last = s[:q], s[-q:]
        avg = lambda xs, k: sum(x[k] or 0 for x in xs) / len(xs)  # noqa: E731
        ok(f"health over {s[-1]['min']} min ({len(s)} samples): checkpoint state "
           f"{avg(first, 'state_bytes') / 1024:.0f} KiB -> {avg(last, 'state_bytes') / 1024:.0f} KiB, "
           f"checkpoint time {avg(first, 'cp_ms'):.0f} -> {avg(last, 'cp_ms'):.0f} ms, "
           f"TaskManager heap {avg(first, 'heap_mb'):.0f} -> {avg(last, 'heap_mb'):.0f} MiB "
           f"(first vs. last quarter)")
        growth = avg(last, "state_bytes") / max(avg(first, "state_bytes"), 1)
        if growth > 3:
            raise Failed(f"checkpointed state grew {growth:.1f}x over the run (bounded state expected)")


# --- failures ---------------------------------------------------------------------------------

def kill_taskmanager():
    it.compose("kill", "taskmanager")
    it.compose("up", "-d", "taskmanager")


def restart_kafka():
    it.compose("restart", "kafka", timeout=300)


def restart_postgres():
    it.compose("restart", "postgres", timeout=300)


def schema_change():
    """A column added to the CDC table mid-stream: the declared schema ignores it; changes keep flowing."""
    it.psql("ALTER TABLE customers ADD COLUMN IF NOT EXISTS note TEXT")
    it.psql("UPDATE customers SET note = 'after schema change' WHERE customer_id = 'c1'")


class Rescaler:
    """stop-with-savepoint and resubmit at the other parallelism (1 <-> 2), under load."""

    def __init__(self, packages: dict):
        self.packages = packages  # parallelism -> runner args
        self.current = 1

    def __call__(self, jid: str) -> str:
        out = it.flink_cli("stop", "--savepointPath", "file:///flink-data/savepoints", jid)
        m = re.search(r"Savepoint completed\. Path: (\S+)", out)
        if not m:
            raise Failed("stop-with-savepoint for rescale failed:\n" + out[-2000:])
        self.current = 2 if self.current == 1 else 1
        it.submit(f"soak-p{self.current}", self.packages[self.current], savepoint=m.group(1))
        return f"parallelism {3 - self.current} -> {self.current}"


def current_slots() -> list[str]:
    """The customers source's slot and any generations the runner moved it to (`<slot>_g<time>`)."""
    return [r[0] for r in it.psql(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '{SLOT}%'")]


def slot_loss_healed(jid: str, args: list, version: str) -> str:
    """on_slot_loss: resnapshot. The slot disappears while the job is stopped and customers change,
    arrive and leave in the gap; the runner moves the source to a new slot generation, the source
    re-snapshots and reconciles: downstream must equal Postgres again (checked at the end)."""
    out = it.flink_cli("stop", "--savepointPath", "file:///flink-data/savepoints", jid)
    m = re.search(r"Savepoint completed\. Path: (\S+)", out)
    if not m:
        raise Failed("stop-with-savepoint before slot loss failed:\n" + out[-2000:])
    for slot in current_slots():
        it.psql(f"SELECT pg_drop_replication_slot('{slot}')")
    existing = [r[0] for r in it.psql(f"SELECT customer_id FROM customers WHERE customer_id <> '{PROBE}' "
                                      "ORDER BY customer_id LIMIT 4")]
    if len(existing) < 4:
        raise Failed(f"expected at least 4 customers before the slot-loss step, got {existing}")
    moved, gone = existing[:2], existing[2:]
    it.psql(f"UPDATE customers SET segment = 'gap' WHERE customer_id IN ('{moved[0]}', '{moved[1]}')")
    it.psql("INSERT INTO customers VALUES ('gap-new', 'gap', '2026-01-01 00:00:00') ON CONFLICT DO NOTHING")
    it.psql(f"DELETE FROM customers WHERE customer_id IN ('{gone[0]}', '{gone[1]}')")
    it.submit(version, args, savepoint=m.group(1))
    slots = current_slots()
    if not slots or slots == [SLOT]:
        raise Failed(f"expected the runner to move the source to a new slot generation, slots: {slots}")
    return f"slot dropped while stopped: healed onto {slots[0]} (re-snapshot + reconcile)"


def check_segment_counts():
    truth = {r[0]: int(r[1]) for r in it.psql("SELECT COALESCE(segment, 'none'), count(*) FROM customers GROUP BY 1")}

    def converged():
        got = {r[0]: int(r[1]) for r in it.psql("SELECT segment, customers FROM segment_counts WHERE customers > 0")}
        return got if got == truth else None
    try:
        it.wait_for("customers per segment to converge", converged, timeout=180, every=5)
    except Failed:
        got = {r[0]: int(r[1]) for r in it.psql("SELECT segment, customers FROM segment_counts")}
        raise Failed(f"customers per segment differ from Postgres after slot-loss healing: {got} vs {truth}")
    ok(f"customers per segment equal Postgres after healed slot losses ({sum(truth.values())} customers, "
       f"{len(truth)} segments)")


def slot_loss(jid: str, args: list, load) -> str:
    """The replication slot disappears while the job is stopped (a database failover or an operator
    'cleaning up'); a customer changes in that gap. Does the change ever reach the job?"""
    out = it.flink_cli("stop", "--savepointPath", "file:///flink-data/savepoints", jid)
    m = re.search(r"Savepoint completed\. Path: (\S+)", out)
    if not m:
        raise Failed("stop-with-savepoint before slot loss failed:\n" + out[-2000:])
    for slot in current_slots():
        it.psql(f"SELECT pg_drop_replication_slot('{slot}')")
    ts = load.event_time().strftime("%Y-%m-%d %H:%M:%S")
    probe = (f"INSERT INTO customers VALUES ('{PROBE}', 'probe-seg', '{ts}') ON CONFLICT (customer_id) "
             f"DO UPDATE SET segment = EXCLUDED.segment, updated_at = EXCLUDED.updated_at")
    it.psql(probe)
    # the runner must refuse: restoring would resume from a new slot and skip the probe's change
    sub = [a.replace("/opt/cascade/job", "/jobs/soak") for a in args]
    out = it.flink_cli("run", "-d", "-s", m.group(1), "-c", "dev.cascade.runner.SqlRunner",
                       "/opt/flink/usrlib/cascade-runtime.jar", *sub,
                       env={"CASCADE_SECRET_CRM_PASSWORD": it.SECRET}, check=False)
    if f"replication slot '{SLOT}' of CDC source 'customers' is gone" not in out:
        raise Failed("the runner restored although the replication slot was gone (changes in the gap would be "
                     "lost silently):\n" + out[-2000:])
    ok("the runner refused to restore: the replication slot is gone")
    # the runbook: accept the gap explicitly, then replay what changed in it (here: the probe's row)
    it.submit("soak", args, savepoint=m.group(1), env={"CASCADE_ALLOW_SLOT_LOSS": "1"})
    time.sleep(5)
    it.psql(probe.replace("'probe-seg', '", "'probe-seg', '").replace(ts, load.event_time().strftime("%Y-%m-%d %H:%M:%S")))
    return "slot dropped while stopped: refused, then restored with the gap accepted and the change replayed"


def kill_taskmanager_twice():
    kill_taskmanager()
    time.sleep(8)
    kill_taskmanager()


# --- ground truth ------------------------------------------------------------------------------

def consume_all(topic: str, committed: bool = False) -> list[str]:
    args = ["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092", "--topic", topic,
            "--from-beginning", "--timeout-ms", "15000"]
    if committed:
        args += ["--isolation-level", "read_committed"]
    if topic in ("customer_ltv",):
        args += ["--property", "print.key=true", "--property", "key.separator=|"]
    out = it.exec_in("kafka", *args, check=False)
    return [line for line in out.splitlines() if line.startswith("{")]


def truth() -> tuple[dict, dict, datetime]:
    """From the orders topic: LTV per customer, revenue per window, and the max event time."""
    ltv: dict = defaultdict(lambda: [Decimal(0), 0])
    windows: dict = defaultdict(Decimal)
    max_ts = BASE
    for line in consume_all("orders"):
        o = json.loads(line)
        ts = datetime.strptime(o["order_ts"], "%Y-%m-%d %H:%M:%S.%f")
        max_ts = max(max_ts, ts)
        amount = Decimal(str(o["amount"]))
        if amount <= 0:
            continue
        ltv[o["customer_id"]][0] += amount
        ltv[o["customer_id"]][1] += 1
        windows[ts.replace(second=0, microsecond=0)] += amount
    return {k: (v[0], v[1]) for k, v in ltv.items()}, dict(windows), max_ts


def check_results(expected_ltv: dict, expected_windows: dict, max_ts: datetime):
    step("ground truth vs. sinks")

    def table():
        return {r[0]: (Decimal(r[1]), int(r[2])) for r in it.psql("SELECT * FROM customer_ltv")}
    last = {}

    def converged():
        last["t"] = table()
        return last["t"] if last["t"] == expected_ltv else None
    try:
        actual = it.wait_for("LTV table to converge", converged, timeout=300, every=5)
    except Failed:
        t = last.get("t", {})
        diff = {k: (t.get(k), expected_ltv.get(k)) for k in set(t) | set(expected_ltv) if t.get(k) != expected_ltv.get(k)}
        raise Failed(f"JDBC LTV differs from ground truth for {len(diff)} customers (actual, expected): "
                     f"{sorted(diff.items())[:5]}")
    ok(f"JDBC LTV equals ground truth for all {len(actual)} customers "
       f"({sum(v[1] for v in actual.values())} orders, exactly once)")

    final = {}
    for line in consume_all("customer_ltv"):
        k, _, v = line.partition("|")
        key = json.loads(k)["customer_id"]
        final[key] = None if v == "null" else (Decimal(str(json.loads(v)["lifetime_value"])),
                                               int(json.loads(v)["order_count"]))
    final = {k: v for k, v in final.items() if v is not None}
    if final != expected_ltv:
        diff = {k: (final.get(k), expected_ltv.get(k)) for k in set(final) | set(expected_ltv)
                if final.get(k) != expected_ltv.get(k)}
        raise Failed(f"upsert-kafka LTV differs from ground truth for {len(diff)} keys: {list(diff.items())[:5]}")
    ok("upsert-kafka LTV equals ground truth")

    rows = [json.loads(line) for line in consume_all("revenue", committed=True)]
    seen: dict = {}
    for r in rows:
        key = (r["window_start"], r["segment"])
        if key in seen:
            raise Failed(f"revenue sink emitted window {key} twice: {seen[key]} and {r}")
        seen[key] = r
    totals: dict = defaultdict(Decimal)
    for r in rows:
        totals[datetime.strptime(r["window_start"][:19], "%Y-%m-%d %H:%M:%S")] += Decimal(str(r["revenue"]))
    closed = {w: v for w, v in expected_windows.items() if w + timedelta(minutes=1) <= max_ts - timedelta(seconds=2)}
    wrong = {w: (totals.get(w), v) for w, v in closed.items() if totals.get(w) != v}
    if wrong:
        raise Failed(f"{len(wrong)} of {len(closed)} closed windows have the wrong revenue: "
                     f"{sorted(wrong.items())[:5]}")
    ok(f"exactly-once revenue sink: {len(rows)} rows, no duplicates, all {len(closed)} closed windows exact")


def check_probe():
    """The probe customer's change was made while the slot was gone; orders after it carry its segment
    only if the change reached the job."""
    segments = {json.loads(line)["segment"] for line in consume_all("revenue", committed=True)}
    if "probe-seg" not in segments:
        raise Failed("a CDC change made while the replication slot was gone never reached the job: on restore "
                     "the connector created a new slot at the current position (silent data loss)")
    ok("the change made while the replication slot was gone reached the job")


# --- main -------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=20)
    ap.add_argument("--rate", type=int, default=100, help="orders per second")
    ap.add_argument("--cycles", type=int, default=1, help="times each kind of failure is injected")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--customers", type=int, default=50, help="key space (keyed state grows with it)")
    ap.add_argument("--partitions", type=int, default=1, help="partitions of the orders topic")
    ap.add_argument("--rescale", action="store_true", help="also rescale 1 <-> 2 under load")
    ap.add_argument("--cdc-edge-cases", action="store_true",
                    help="also change the CDC table's schema, and lose the replication slot while stopped")
    ap.add_argument("--slot-loss", default="refuse", choices=["refuse", "resnapshot"],
                    help="with --cdc-edge-cases: the customers source's on_slot_loss policy")
    ap.add_argument("--delivery", default="exactly-once", choices=["exactly-once", "at-least-once"],
                    help="revenue sink guarantee (at-least-once is a control: the duplicate check must fail)")
    a = ap.parse_args()
    started = time.time()
    code = 1
    load = changes = None
    try:
        CUSTOMERS[:] = [f"c{i}" for i in range(a.customers)] + ([PROBE] if a.cdc_edge_cases else [])
        it.build(a.skip_build)
        it.start()
        if a.partitions > 1:
            it.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--alter",
                     "--topic", "orders", "--partitions", str(a.partitions))
        step(f"soak: {a.minutes:g} minutes at {a.rate} orders/s, {a.customers} customers, "
             f"{a.partitions} partition(s), with failures")
        heal = a.cdc_edge_cases and a.slot_loss == "resnapshot"
        project = write_pipeline(a.delivery, heal=heal)
        args = it.package("soak", project)
        rescaler = None
        if a.rescale:
            p2 = write_pipeline(a.delivery, parallelism=2, heal=heal)
            rescaler = Rescaler({1: args, 2: it.package("soak-p2", p2, previous="soak")})
            rescaler.packages[1] = it.package("soak-p1", project, previous="soak-p2")
        it.submit("soak", args)
        wait_healthy("job started", since_checkpoints=0)
        load, changes = Load(a.rate), None
        load.thread.start()
        changes = CustomerChanges(load)
        changes.thread.start()

        sampler = Sampler()
        sampler.thread.start()
        kinds = [("TaskManager killed", kill_taskmanager),
                 ("Kafka broker restarted", restart_kafka),
                 ("Postgres restarted (CDC connection drops)", restart_postgres),
                 ("stop-with-savepoint and resubmit under load", None),
                 ("TaskManager killed twice in a row", kill_taskmanager_twice)]
        if rescaler:
            kinds.append(("rescale under load", rescaler))
        if a.cdc_edge_cases:
            kinds.append(("CDC table schema change (ADD COLUMN)", schema_change))
            def lose_slot(jid):
                if heal:
                    version = f"soak-p{rescaler.current}" if rescaler else "soak"
                    return slot_loss_healed(jid, rescaler.packages[rescaler.current] if rescaler else args, version)
                if rescaler:
                    rescaler.current = 1
                return slot_loss(jid, args, load)
            kinds.append(("replication slot lost while stopped", lose_slot))
        n = len(kinds) * a.cycles
        schedule = [((k + 1) / (n + 1), *kinds[k % len(kinds)]) for k in range(n)]
        total = a.minutes * 60
        t0 = time.time()
        for frac, name, action in schedule:
            time.sleep(max(0.0, t0 + frac * total - time.time()))
            before = job_info()
            print(f"   ..  t+{time.time() - t0:.0f}s: {name} ({load.seq} orders produced, "
                  f"{changes.count} customer changes)", flush=True)
            if action is None:
                out = it.flink_cli("stop", "--savepointPath", "file:///flink-data/savepoints", before["jid"])
                m = re.search(r"Savepoint completed\. Path: (\S+)", out)
                if not m:
                    raise Failed("stop-with-savepoint under load failed:\n" + out[-2000:])
                it.submit(f"soak-p{rescaler.current}" if rescaler else "soak",
                          rescaler.packages[rescaler.current] if rescaler else args, savepoint=m.group(1))
                wait_healthy(f"after {name}", since_checkpoints=0)
            elif action is rescaler or name.startswith("replication slot"):  # these stop and resubmit
                what = action(before["jid"])
                ok(what)
                wait_healthy(f"after {name}", since_checkpoints=0)
            else:
                action()
                wait_healthy(f"after {name}", since_checkpoints=before.get("completed", 0)
                             if action is not kill_taskmanager and action is not kill_taskmanager_twice else None)
        time.sleep(max(0.0, t0 + total - time.time()))

        step("drain")
        sampler.stop.set()
        sampler.report()
        load.stop.set()
        changes.stop.set()
        load.thread.join(timeout=60)
        changes.thread.join(timeout=30)
        # +60s of event time closes the last windows -- on every partition: a partition without it holds
        # event time back until its idle timeout (60 s for Kafka sources). Distinct keys spread the
        # closing orders over the partitions (the console producer can't pick one).
        closing = [load.order(load.seq + 600 + k) for k in range(50)]
        it.kafka("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "kafka:9092", "--topic", "orders",
                 "--property", "parse.key=true", "--property", "key.separator=|",
                 input="".join(f"close-{k}|{json.dumps(o)}\n" for k, o in enumerate(closing)))
        info = job_info()
        ok(f"produced {load.seq} orders ({load.restarts} producer restarts), {changes.count} customer changes; "
           f"job {info.get('state')}, {info.get('completed')} checkpoints, {info.get('failed')} failed")
        expected_ltv, expected_windows, max_ts = truth()
        check_results(expected_ltv, expected_windows, max_ts)
        if heal:
            check_segment_counts()
        elif a.cdc_edge_cases:
            check_probe()
        print(f"\nPASSED in {time.time() - started:.0f}s")
        code = 0
    except (Failed, RuntimeError) as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        try:
            print(it.compose("logs", "--tail", "80", "jobmanager", "taskmanager", check=False)[-8000:], file=sys.stderr)
        except Exception:  # noqa: BLE001
            pass
    finally:
        for t in (load, changes):
            if t is not None:
                t.stop.set()
        if not a.keep:
            it.compose("down", "-v", "--remove-orphans", check=False)
    sys.exit(code)


if __name__ == "__main__":
    main()
