"""End-to-end checks for the catalog connectors the main integration run doesn't cover.

Each case is a small pipeline compiled by cascade and run by SqlRunner (local MiniCluster) against a
real system or its official emulator (integration/connectors-compose.yml), with the connector's jars
at their tested versions (cascade fetch-jars). Sinks: rows land in the system. Sources: what they
read lands in Postgres and must equal the source -- a CDC source through its snapshot and then live
inserts, updates and deletes, and again in batch mode (a bounded snapshot).

    python3 integration/connectors.py [--only mysql-cdc,elasticsearch] [--keep]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run as it  # noqa: E402
from run import Failed, ok, step  # noqa: E402

sys.path.insert(0, it.REPO)
from cascade_dsl.compiler import compile_pipeline  # noqa: E402
from cascade_dsl.jars import fetch  # noqa: E402
from cascade_dsl.jvm import script  # noqa: E402

COMPOSE = os.path.join(it.HERE, "connectors-compose.yml")
WORK = os.path.join(it.WORK, "connectors")
RUNTIME = os.path.join(it.REPO, "runtime", "target", "cascade-runtime.jar")
SECRETS = {"CASCADE_SECRET_PW": "pw", "CASCADE_SECRET_MSSQL": "Cascade-pw1", "CASCADE_SECRET_AWS_KEY": "x",
           "CASCADE_SECRET_AWS_SECRET": "x"}

RESULTS = """
  results: {type: jdbc, properties: {url: "jdbc:postgresql://localhost:15432/results", username: postgres,
            password: "${secret:pw}"}}"""
AWS = 'region: us-east-1, access_key_id: "${secret:aws_key}", secret_access_key: "${secret:aws_secret}"'
GEN = """
  gen:
    connection: datagen
    columns: {id: BIGINT NOT NULL, name: STRING, qty: INT}
    number_of_rows: 100
    options: {fields.id.kind: sequence, fields.id.start: "1", fields.id.end: "100", fields.name.length: "8",
              fields.qty.min: "1", fields.qty.max: "50"}"""


def dc(*args, check=True, input=None, timeout=900) -> str:
    return it.run(["docker", "compose", "-f", COMPOSE, *args], check=check, input=input, timeout=timeout)


def sh(service: str, *cmd, input=None, check=True) -> str:
    return dc("exec", "-T", service, *cmd, input=input, check=check)


def results(sql: str) -> list[list[str]]:
    out = sh("postgres", "psql", "-U", "postgres", "-d", "results", "-tAF", "|", "-c", sql)
    return [line.split("|") for line in out.splitlines() if line]


def http(method: str, url: str, body=None, headers=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read() or b"null")


# --- running a pipeline ------------------------------------------------------------------------

class Job:
    """A pipeline compiled by cascade, run by SqlRunner with its connector jars."""

    def __init__(self, name: str, yaml_text: str):
        self.dir = os.path.join(WORK, name)
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir)
        with open(os.path.join(self.dir, "pipeline.yaml"), "w") as f:
            f.write(yaml_text)
        r = compile_pipeline(self.dir)
        if not r.ok:
            raise Failed(f"{name}: does not compile: " + "; ".join(d.format() for d in r.diagnostics.errors))
        stmts = r.lowered.statements
        with open(os.path.join(self.dir, "setup.sql"), "w") as f:
            f.write(script([s for s in stmts if s.kind != "insert"]))
        with open(os.path.join(self.dir, "statements.sql"), "w") as f:
            f.write(script([s for s in stmts if s.kind == "insert"]))
        self.jars = fetch(r.lowered.manifest["artifacts"], "2.3")
        self.proc = None
        self.log = os.path.join(self.dir, "job.log")

    def _cmd(self, wait: bool, restore: str | None = None) -> list[str]:
        cp = os.pathsep.join([RUNTIME, os.path.join(it.FLINK_BASE, "lib", "*"), *self.jars])
        return ["java", "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "-cp", cp, "dev.cascade.runner.SqlRunner", "--setup", os.path.join(self.dir, "setup.sql"),
                "--statements", os.path.join(self.dir, "statements.sql")] + (["--await"] if wait else []) \
            + (["--restore", restore] if restore else [])

    def run(self, timeout=600):
        """A bounded job, to completion."""
        p = subprocess.run(self._cmd(True), capture_output=True, text=True, timeout=timeout,
                           env={**os.environ, **SECRETS})
        with open(self.log, "w") as f:
            f.write(p.stdout + p.stderr)
        if p.returncode != 0:
            raise Failed(f"job in {self.dir} failed:\n{_error(p.stdout + p.stderr)}")

    def start(self, restore: str | None = None):
        """A streaming job, in the background (stop() ends it)."""
        self.out = open(self.log, "a")
        self.proc = subprocess.Popen(self._cmd(True, restore), stdout=self.out, stderr=subprocess.STDOUT,
                                     env={**os.environ, **SECRETS})

    def latest_checkpoint(self) -> str:
        cps = []
        for root, dirs, files in os.walk(os.path.join(self.dir, "checkpoints")):
            if "_metadata" in files and os.path.basename(root).startswith("chk-"):
                cps.append((int(os.path.basename(root)[4:]), root))
        if not cps:
            raise Failed(f"no completed checkpoint under {self.dir}/checkpoints")
        return "file://" + max(cps)[1]

    def check_alive(self):
        if self.proc is not None and self.proc.poll() is not None:
            with open(self.log) as f:
                raise Failed(f"streaming job in {self.dir} exited:\n{_error(f.read())}")

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(30)


def _error(text: str) -> str:
    lines = [x for x in text.splitlines() if "Exception" in x or "Error" in x or "Caused by" in x]
    return "\n".join(lines[:25] + ["...", *text.splitlines()[-15:]]) if lines else text[-4000:]


def converge(what: str, actual, expected, job: Job | None = None, timeout=180):
    last = {}

    def check():
        if job:
            job.check_alive()
        last["v"] = actual()
        return last["v"] == expected
    try:
        it.wait_for(what, check, timeout=timeout, every=3)
    except Failed:
        raise Failed(f"{what}: got {last.get('v')!r}, expected {expected!r}")
    ok(what)


# --- CDC sources ---------------------------------------------------------------------------------

def mirror_case(name: str, connection: str, source: str, columns: str, key: str, truth, change, setup_results,
                batch: bool = True):
    """Snapshot + live changes (streaming), then a bounded snapshot (batch): Postgres must mirror the source."""
    table = name.replace("-", "_")
    setup_results(table)
    text = f"""
pipeline: {table}
flink: {{config: {{execution.checkpointing.interval: 2s}}}}
connections:
  src: {connection}{RESULTS}
sources:
  items:
    connection: src
{source}
    columns: {columns}
    primary_key: [{key}]
sinks:
  out: {{from: items, connection: results, table: {table}, mode: upsert, primary_key: [{key}]}}
"""
    job = Job(name, text)
    job.start()
    try:
        mirror = lambda: sorted(tuple(r) for r in results(f"SELECT * FROM {table} ORDER BY 1"))  # noqa: E731
        converge(f"{name}: snapshot mirrored into Postgres", mirror, truth(), job)
        change()
        converge(f"{name}: inserts, updates and deletes mirrored", mirror, truth(), job)
    finally:
        job.stop()
    if not batch:
        r = compile_once("mode: batch\n" + text, name + "-batch-check")
        if "CD120" not in r.diagnostics.codes():
            raise Failed(f"{name}: expected batch mode to be refused (CD120)")
        ok(f"{name}: batch mode is refused at compile time (CD120), as Flink can't run it")
        return
    setup_results(table + "_batch")
    batch = Job(name + "-batch", "mode: batch\n" + text.replace(f"table: {table},", f"table: {table}_batch,"))
    batch.run()
    converge(f"{name}: batch snapshot equals the source",
             lambda: sorted(tuple(r) for r in results(f"SELECT * FROM {table}_batch ORDER BY 1")), truth())


def compile_once(text: str, name: str):
    d = os.path.join(WORK, name)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    with open(os.path.join(d, "pipeline.yaml"), "w") as f:
        f.write(text)
    return compile_pipeline(d)


def mysql_ready() -> bool:
    try:
        return "1" in sh("mysql", "mysql", "-uroot", "-ppw", "-N", "-e", "SELECT 1")
    except Failed:
        return False


def pg_table(sql_cols: str):
    def make(table):
        results(f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({sql_cols})")
    return make


def case_mysql():
    sql = lambda q: sh("mysql", "mysql", "-uroot", "-ppw", "-N", "-B", "app", "-e", q)  # noqa: E731
    it.wait_for("MySQL", mysql_ready, timeout=240, every=3)
    sql("DROP TABLE IF EXISTS items; CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(20), qty INT); "
        "INSERT INTO items VALUES (1,'a',5),(2,'b',7),(3,'c',9)")
    truth = lambda: sorted(tuple(line.split("\t")) for line in sql("SELECT id, name, qty FROM items").splitlines())  # noqa: E731
    mirror_case("mysql-cdc", '{type: mysql-cdc, properties: {hostname: localhost, port: 13306, username: root, '
                'password: "${secret:pw}"}}', "    table: app.items", "{id: INT NOT NULL, name: STRING, qty: INT}",
                "id", truth,
                lambda: sql("INSERT INTO items VALUES (4,'d',1); UPDATE items SET qty = 70 WHERE id = 2; "
                            "DELETE FROM items WHERE id = 3"),
                pg_table("id INT PRIMARY KEY, name TEXT, qty INT"))


def case_postgres():
    sql = lambda q: sh("postgres", "psql", "-U", "postgres", "-d", "app", "-tAF", "\t", "-c", q)  # noqa: E731
    if "app" not in sh("postgres", "psql", "-U", "postgres", "-tAc", "SELECT datname FROM pg_database"):
        sh("postgres", "psql", "-U", "postgres", "-c", "CREATE DATABASE app")
    sql("DROP TABLE IF EXISTS items; CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT); "
        "ALTER TABLE items REPLICA IDENTITY FULL; INSERT INTO items VALUES (1,'a',5),(2,'b',7),(3,'c',9)")
    truth = lambda: sorted(tuple(line.split("\t")) for line in sql("SELECT id, name, qty FROM items").splitlines())  # noqa: E731
    mirror_case("postgres-cdc", '{type: postgres-cdc, properties: {hostname: localhost, port: 15432, '
                'database: app, username: postgres, password: "${secret:pw}"}}', "    table: public.items",
                "{id: INT NOT NULL, name: STRING, qty: INT}", "id", truth,
                lambda: sql("INSERT INTO items VALUES (4,'d',1); UPDATE items SET qty = 70 WHERE id = 2; "
                            "DELETE FROM items WHERE id = 3"),
                pg_table("id INT PRIMARY KEY, name TEXT, qty INT"))


def case_sqlserver():
    def sql(q, db="app"):
        return sh("sqlserver", "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost", "-U", "sa", "-P",
                  "Cascade-pw1", "-d", db, "-h", "-1", "-W", "-s", "\t", "-Q", "SET NOCOUNT ON; " + q)
    it.wait_for("SQL Server", lambda: "1" in sql("SELECT 1", db="master"), timeout=240, every=5)
    sql("IF DB_ID('app') IS NULL CREATE DATABASE app", db="master")
    sql("IF OBJECT_ID('dbo.items') IS NULL BEGIN EXEC sys.sp_cdc_enable_db; "
        "CREATE TABLE dbo.items (id INT PRIMARY KEY, name NVARCHAR(20), qty INT); "
        "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'items', @role_name = NULL; END; "
        "DELETE FROM dbo.items; INSERT INTO dbo.items VALUES (1,'a',5),(2,'b',7),(3,'c',9)")
    truth = lambda: sorted(tuple(x.strip() for x in line.split("\t"))  # noqa: E731
                           for line in sql("SELECT id, name, qty FROM dbo.items").splitlines() if line.strip())
    mirror_case("sqlserver-cdc", '{type: sqlserver-cdc, properties: {hostname: localhost, port: 11433, username: sa, '
                'password: "${secret:mssql}", database: app}}', "    table: dbo.items",
                "{id: INT NOT NULL, name: STRING, qty: INT}", "id", truth,
                lambda: sql("INSERT INTO dbo.items VALUES (4,'d',1); UPDATE dbo.items SET qty = 70 WHERE id = 2; "
                            "DELETE FROM dbo.items WHERE id = 3"),
                pg_table("id INT PRIMARY KEY, name TEXT, qty INT"))


def case_mongodb():
    def js(code):
        return sh("mongo", "mongosh", "--quiet", "--eval", code)
    it.wait_for("MongoDB", lambda: "1" in js("db.runCommand({ping: 1}).ok"), timeout=120)
    js("try { rs.status() } catch (e) { rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]}) }")
    it.wait_for("replica set primary", lambda: "true" in js("db.hello().isWritablePrimary"), timeout=60)
    js("db.getSiblingDB('app').items.drop(); db.getSiblingDB('app').items.insertMany("
       "[{_id: '1', name: 'a', qty: 5}, {_id: '2', name: 'b', qty: 7}, {_id: '3', name: 'c', qty: 9}])")
    truth = lambda: sorted(tuple(x.split("|")) for x in js(  # noqa: E731
        "db.getSiblingDB('app').items.find().toArray().map(d => d._id + '|' + d.name + '|' + d.qty).join('\\n')"
    ).splitlines() if x)
    # Flink CDC reads the replica set member as advertised (localhost:27017 inside the container); the
    # published port maps the host's 27018 there, so connect with directConnection
    mirror_case("mongodb-cdc", '{type: mongodb-cdc, properties: {hosts: "localhost:27018", '
                'connection_options: "directConnection=true"}}', "    database: app\n    collection: items",
                "{_id: STRING NOT NULL, name: STRING, qty: INT}", "_id", truth,
                lambda: js("const c = db.getSiblingDB('app').items; c.insertOne({_id: '4', name: 'd', qty: 1}); "
                           "c.updateOne({_id: '2'}, {$set: {qty: 70}}); c.deleteOne({_id: '3'})"),
                pg_table("_id TEXT PRIMARY KEY, name TEXT, qty INT"), batch=False)


# --- sinks (and read-backs) ------------------------------------------------------------------------

def sink_job(name: str, connection: str, sink: str, mode: str = "upsert") -> Job:
    pk = ", primary_key: [id]" if mode == "upsert" else ""
    return Job(name, f"""
pipeline: {name.replace('-', '_')}
mode: batch
connections:
  dst: {connection}
sources:{GEN}
sinks:
  out: {{from: gen, connection: dst, mode: {mode}{pk}, {sink}}}
""")


def case_elasticsearch():
    it.wait_for("Elasticsearch", lambda: http("GET", "http://localhost:19200/_cluster/health")["status"]
                in ("green", "yellow"), timeout=180, every=5)
    try:
        http("DELETE", "http://localhost:19200/cascade_items")
    except Exception:  # noqa: BLE001
        pass
    sink_job("elasticsearch", '{type: elasticsearch, properties: {hosts: "http://localhost:19200"}}',
             "index: cascade_items").run()
    http("POST", "http://localhost:19200/cascade_items/_refresh")
    converge("elasticsearch: 100 documents, one per key",
             lambda: http("GET", "http://localhost:19200/cascade_items/_count")["count"], 100)


def dynamo(target: str, body: dict):
    return http("POST", "http://localhost:18000", body, {
        "X-Amz-Target": f"DynamoDB_20120810.{target}", "Content-Type": "application/x-amz-json-1.0",
        "Authorization": "AWS4-HMAC-SHA256 Credential=x/20260101/us-east-1/dynamodb/aws4_request, "
                         "SignedHeaders=host, Signature=x"})


def case_dynamodb():
    it.wait_for("DynamoDB Local", lambda: dynamo("ListTables", {}) is not None, timeout=60)
    if "items" in dynamo("ListTables", {}).get("TableNames", []):
        dynamo("DeleteTable", {"TableName": "items"})
    dynamo("CreateTable", {"TableName": "items", "BillingMode": "PAY_PER_REQUEST",
                           "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
                           "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]})
    sink_job("dynamodb", "{type: dynamodb, properties: {" + AWS + ", endpoint: 'http://localhost:18000'}}",
             "table: items").run()
    converge("dynamodb: 100 items, one per key", lambda: dynamo("Scan", {"TableName": "items", "Select": "COUNT"})
             ["Count"], 100)


def case_kinesis():
    conn = "{type: kinesis, properties: {" + AWS + ", endpoint: 'http://localhost:14568'}}"
    arn = "arn:aws:kinesis:us-east-1:000000000000:stream/orders"
    sink_job("kinesis-sink", conn, f"stream_arn: '{arn}'", mode="append").run()
    ok("kinesis: 100 records written")
    pg_table("id BIGINT PRIMARY KEY, name TEXT, qty INT")("kinesis_items")
    job = Job("kinesis-source", f"""
pipeline: kinesis_source
flink: {{config: {{execution.checkpointing.interval: 2s}}}}
connections:
  src: {conn}{RESULTS}
sources:
  orders:
    connection: src
    stream_arn: '{arn}'
    startup: earliest
    columns: {{id: BIGINT NOT NULL, name: STRING, qty: INT}}
sinks:
  out: {{from: orders, connection: results, table: kinesis_items, mode: upsert, primary_key: [id]}}
""")
    job.start()
    try:
        converge("kinesis: the source reads all 100 back",
                 lambda: int(results("SELECT count(*) FROM kinesis_items")[0][0]), 100, job)
    finally:
        job.stop()


def case_hudi():
    base = os.path.join(WORK, "lake")
    shutil.rmtree(os.path.join(base, "hudi_items"), ignore_errors=True)
    conn = f"{{type: hudi, properties: {{base_path: 'file://{base}'}}}}"
    sink_job("hudi-sink", conn, "path: hudi_items").run()
    pg_table("id BIGINT PRIMARY KEY, name TEXT, qty INT")("hudi_items")
    Job("hudi-source", f"""
pipeline: hudi_source
mode: batch
connections:
  src: {conn}{RESULTS}
sources:
  items:
    connection: src
    path: hudi_items
    columns: {{id: BIGINT NOT NULL, name: STRING, qty: INT}}
    primary_key: [id]
sinks:
  out: {{from: items, connection: results, table: hudi_items, mode: upsert, primary_key: [id]}}
""").run()
    converge("hudi: written and read back (100 rows)", lambda: int(results("SELECT count(*) FROM hudi_items")[0][0]), 100)


def lake_roundtrip(name: str, conn: str, sink_keys: str, source_keys: str, mode: str = "upsert"):
    """Write 100 rows (batch), then read them back through the source (batch) into Postgres."""
    table = name.replace("-", "_")
    sink_job(name + "-sink", conn, sink_keys, mode=mode).run()
    pg_table("id BIGINT PRIMARY KEY, name TEXT, qty INT")(table)
    pk = "\n    primary_key: [id]" if mode == "upsert" else ""
    Job(name + "-source", f"""
pipeline: {table}_source
mode: batch
connections:
  src: {conn}{RESULTS}
sources:
  items:
    connection: src
{source_keys}
    columns: {{id: BIGINT NOT NULL, name: STRING, qty: INT}}{pk}
sinks:
  out: {{from: items, connection: results, table: {table}, mode: upsert, primary_key: [id]}}
""").run()
    converge(f"{name}: written and read back (100 rows)", lambda: int(results(f"SELECT count(*) FROM {table}")[0][0]),
             100)


def case_paimon():
    wh = os.path.join(WORK, "lake", "paimon")
    shutil.rmtree(wh, ignore_errors=True)
    # the table doesn't exist yet: the runner creates it from the declared columns and key
    lake_roundtrip("paimon", f"{{type: paimon, properties: {{warehouse: 'file://{wh}'}}}}",
                   "table: shop.items, table_options: {bucket: '2'}", "    table: shop.items")


def case_iceberg():
    wh = os.path.join(WORK, "lake", "iceberg")
    shutil.rmtree(wh, ignore_errors=True)
    lake_roundtrip("iceberg", f"{{type: iceberg, properties: {{catalog_type: hadoop, warehouse: 'file://{wh}'}}}}",
                   "table: shop.items", "    table: shop.items", mode="append")


def case_delta():
    path = os.path.join(WORK, "lake", "delta_items")
    shutil.rmtree(path, ignore_errors=True)
    sink_job("delta", "{type: delta}", f"table_path: 'file://{path}'", mode="append").run()

    def rows():
        n = 0
        for f in sorted(os.listdir(os.path.join(path, "_delta_log"))):
            if f.endswith(".json"):
                for line in open(os.path.join(path, "_delta_log", f)):
                    add = json.loads(line).get("add")
                    if add and add.get("stats"):
                        n += json.loads(add["stats"])["numRecords"]
        return n
    converge("delta: 100 rows committed to the Delta log", rows, 100)


# --- lost change history: loud failure, or silent loss? -------------------------------------------

def history_loss_case(name: str, connection: str, source: str, seed, lose, truth):
    """A CDC source whose change history disappears while the job is down (MySQL: binlog purged; SQL
    Server: change table cleaned up). Restored from its last checkpoint, it must either fail loudly or
    still deliver the lost change. Running on without it is silent data loss."""
    table = name.replace("-", "_")
    pg_table("id INT PRIMARY KEY, v TEXT")(table)
    seed()
    job = Job(name, f"""
pipeline: {table}
flink:
  config:
    execution.checkpointing.interval: 1s
    execution.checkpointing.dir: "file://{os.path.join(WORK, name, 'checkpoints')}"
    execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
connections:
  src: {connection}{RESULTS}
sources:
  items:
    connection: src
{source}
    columns: {{id: INT NOT NULL, v: STRING}}
    primary_key: [id]
sinks:
  out: {{from: items, connection: results, table: {table}, mode: upsert, primary_key: [id]}}
""")
    mirror = lambda: sorted(tuple(r) for r in results(f"SELECT id, v FROM {table} ORDER BY 1"))  # noqa: E731
    job.start()
    try:
        converge(f"{name}: mirrored before the outage", mirror, truth(), job)
        time.sleep(6)  # a checkpoint after the snapshot and stream switch
    finally:
        job.stop()
    checkpoint = job.latest_checkpoint()
    lose()  # a change while the job is down, then its history is destroyed
    job.start(restore=checkpoint)
    deadline = time.time() + 120
    try:
        while time.time() < deadline:
            with open(job.log) as f:
                log = f.read()
            loud = next((x for x in log.splitlines() if "no longer available" in x or "Caused by" in x
                         and "IllegalStateException" in x), None)
            if job.proc.poll() is not None or loud:
                ok(f"{name}: restored without the lost history, the source fails loudly (the job restarts "
                   f"until fixed): {(loud or _error(log).splitlines()[-1]).strip()[:240]}")
                return
            if mirror() == truth():
                ok(f"{name}: the change made while its history was lost still arrived")
                return
            time.sleep(3)
        with open(job.log) as f:
            log = f.read()
        raise Failed(f"{name}: SILENT DATA LOSS: restored after its change history was lost, the job keeps "
                     f"running without the change (mirror {mirror()} vs source {truth()}). Log:\n{_error(log)}")
    finally:
        job.stop()


def case_mysql_binlog_purge():
    sql = lambda q: sh("mysql", "mysql", "-uroot", "-ppw", "-N", "-B", "app", "-e", q)  # noqa: E731
    it.wait_for("MySQL", mysql_ready, timeout=240, every=3)

    def seed():
        sql("DROP TABLE IF EXISTS hist; CREATE TABLE hist (id INT PRIMARY KEY, v VARCHAR(20)); "
            "INSERT INTO hist VALUES (1,'a'),(2,'b')")

    def lose():
        sql("INSERT INTO hist VALUES (3,'made while down'); FLUSH BINARY LOGS")
        current = sql("SHOW BINARY LOG STATUS").split("\t")[0].strip()
        sql(f"PURGE BINARY LOGS TO '{current}'")
    history_loss_case("mysql-binlog-purged", '{type: mysql-cdc, properties: {hostname: localhost, port: 13306, '
                      'username: root, password: "${secret:pw}"}}', "    table: app.hist", seed, lose,
                      lambda: sorted(tuple(x.split("\t")) for x in sql("SELECT id, v FROM hist").splitlines()))


def case_sqlserver_cleanup():
    def sql(q, db="app"):
        return sh("sqlserver", "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost", "-U", "sa", "-P",
                  "Cascade-pw1", "-d", db, "-h", "-1", "-W", "-s", "\t", "-Q", "SET NOCOUNT ON; " + q)
    it.wait_for("SQL Server", lambda: "1" in sql("SELECT 1", db="master"), timeout=240, every=5)
    sql("IF DB_ID('app') IS NULL CREATE DATABASE app", db="master")

    def seed():
        sql("IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'app') = 0 EXEC sys.sp_cdc_enable_db; "
            "IF OBJECT_ID('dbo.hist') IS NULL BEGIN CREATE TABLE dbo.hist (id INT PRIMARY KEY, v NVARCHAR(20)); "
            "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'hist', @role_name = NULL; END; "
            "DELETE FROM dbo.hist; INSERT INTO dbo.hist VALUES (1,'a'),(2,'b')")

    def lose():
        sql("INSERT INTO dbo.hist VALUES (3,'made while down')")
        time.sleep(8)  # the capture job copies it into the change table
        sql("DECLARE @lsn binary(10) = sys.fn_cdc_get_max_lsn(); "
            "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = 'dbo_hist', @low_water_mark = @lsn, "
            "@threshold = 5000")
    history_loss_case("sqlserver-cleaned-up", '{type: sqlserver-cdc, properties: {hostname: localhost, port: 11433, '
                      'username: sa, password: "${secret:mssql}", database: app}}', "    table: dbo.hist", seed, lose,
                      lambda: sorted(tuple(x.strip() for x in line.split("\t"))
                                     for line in sql("SELECT id, v FROM dbo.hist").splitlines() if line.strip()))


CASES = {"postgres-cdc": case_postgres, "mysql-cdc": case_mysql, "sqlserver-cdc": case_sqlserver, "mongodb-cdc": case_mongodb,
         "elasticsearch": case_elasticsearch, "dynamodb": case_dynamodb, "kinesis": case_kinesis,
         "hudi": case_hudi, "delta": case_delta, "paimon": case_paimon, "iceberg": case_iceberg, "mysql-binlog-purged": case_mysql_binlog_purge,
         "sqlserver-cleaned-up": case_sqlserver_cleanup}


def prepare():
    """Flink 2.3 (from the official image, as integration/run.py does) and the cascade runtime."""
    step("prepare: Flink 2.3 and the cascade runtime")
    if not os.path.exists(os.path.join(it.FLINK_BASE, "lib")):
        it.run(["docker", "pull", "-q", it.BASE_IMAGE])
        it.run(["docker", "rm", "-f", "cascade-conn-extract"], check=False)
        it.run(["docker", "create", "--name", "cascade-conn-extract", it.BASE_IMAGE])
        it.run(["docker", "cp", "cascade-conn-extract:/opt/flink", it.FLINK_BASE])
        it.run(["docker", "rm", "cascade-conn-extract"])
    if not os.path.exists(RUNTIME):
        it.run([os.path.join(it.REPO, "runtime", "build.sh")], env={"FLINK_HOME": it.FLINK_BASE})
    ok("ready")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated cases: " + ", ".join(CASES))
    ap.add_argument("--keep", action="store_true", help="leave the systems running")
    a = ap.parse_args()
    names = a.only.split(",") if a.only else list(CASES)
    os.makedirs(WORK, exist_ok=True)
    prepare()
    failed = []
    try:
        step("systems: " + ", ".join(sorted({"postgres"} | {n.split("-")[0] for n in names})))
        services = {"mysql-cdc": "mysql", "sqlserver-cdc": "sqlserver", "mongodb-cdc": "mongo",
                    "mysql-binlog-purged": "mysql", "sqlserver-cleaned-up": "sqlserver",
                    "elasticsearch": "elasticsearch", "dynamodb": "dynamodb", "kinesis": "kinesis"}
        dc("up", "-d", "postgres", *sorted({services[n] for n in names if n in services}), timeout=1200)
        it.wait_for("Postgres", lambda: results("SELECT 1"), timeout=60)
        for n in names:
            step(n)
            try:
                CASES[n]()
            except Failed as e:
                print(f"   FAIL {n}: {e}", flush=True)
                failed.append(n)
    finally:
        if not a.keep:
            dc("down", "-v", "--remove-orphans", check=False)
    if failed:
        print(f"\nFAILED: {', '.join(failed)}")
        sys.exit(1)
    print(f"\nPASSED: {', '.join(names)}")


if __name__ == "__main__":
    main()
