"""Connector smoke tests: plan a job against each catalog connector's real jar (at its tested version)
with the target Flink. Planning instantiates the table factories, so option names, required options
and binary compatibility with this Flink are all checked; no external system is contacted.

Downloads jars from Maven Central (cached): opt in with CASCADE_CONNECTOR_SMOKE=1."""
import os
import shutil
import tempfile
import textwrap
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.jars import fetch
from cascade_dsl.jvm import TARGET, find_flink_home
from cascade_dsl.plans import PlanError, compile_plan

FLINK = find_flink_home()
READY = (FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))
         and os.environ.get("CASCADE_CONNECTOR_SMOKE") == "1")

GEN = """
  gen:
    connection: datagen
    columns: {id: STRING NOT NULL, v: BIGINT}
"""


def sink_case(conn: str, sink: str, mode: str = "upsert") -> str:
    pk = "\n    primary_key: [id]" if mode == "upsert" else ""
    return f"""
pipeline: smoke
defaults: {{state_ttl: 1d}}
connections:
{textwrap.indent(conn.strip(), '  ')}
sources:{GEN}
models:
  m:
    sql: {"SELECT id, SUM(v) AS v FROM gen GROUP BY id" if mode == "upsert" else "SELECT id, v FROM gen"}
sinks:
  out:
    from: m
    connection: c
    mode: {mode}{pk}
{textwrap.indent(sink.strip(), '    ')}
"""


def source_case(conn: str, source: str, columns: str, pk: str | None) -> str:
    return f"""
pipeline: smoke
connections:
{textwrap.indent(conn.strip(), '  ')}
sources:
  src:
    connection: c
    columns: {columns}
{f'    primary_key: [{pk}]' if pk else ''}
{textwrap.indent(source.strip(), '    ')}
models:
  m:
    sql: SELECT * FROM src
sinks:
  out: {{from: m, connection: print{', mode: upsert, primary_key: [' + pk + ']' if pk else ''}}}
"""


def cdc(kind: str, extra: str = "", table: str = "app.orders") -> str:
    return source_case(f"""
c:
  type: {kind}
  properties: {{hostname: db, username: u, password: "${{secret:db/pw}}"{extra}}}
""", f"table: {table}", "{id: BIGINT NOT NULL, v: STRING}", "id")


CASES = {
    "kinesis-source": source_case("c: {type: kinesis, properties: {region: us-east-1}}",
                                  "stream_arn: arn:aws:kinesis:us-east-1:000000000000:stream/orders\nstartup: earliest",
                                  "{id: STRING, v: BIGINT}", None),
    "kinesis-sink": sink_case("c: {type: kinesis, properties: {region: us-east-1}}",
                              "stream_arn: arn:aws:kinesis:us-east-1:000000000000:stream/out", mode="append"),
    "dynamodb": sink_case("c: {type: dynamodb, properties: {region: us-east-1}}", "table: out"),
    "elasticsearch": sink_case("c: {type: elasticsearch, properties: {hosts: 'http://es:9200'}}", "index: out"),
    "mysql-cdc": cdc("mysql-cdc"),
    "oracle-cdc": cdc("oracle-cdc", ", database: ORCL"),
    "sqlserver-cdc": cdc("sqlserver-cdc", ", database: app", table="dbo.orders"),
    "db2-cdc": cdc("db2-cdc", ", database: app", table="DB2INST1.ORDERS"),
    "mongodb-cdc": source_case("c: {type: mongodb-cdc, properties: {hosts: 'mongo:27017'}}",
                               "database: app\ncollection: orders", "{_id: STRING NOT NULL, v: STRING}", "_id"),
    "paimon": sink_case("c: {type: paimon, properties: {base_path: 'file://{tmp}'}}", "path: out"),
    "hudi": sink_case("c: {type: hudi, properties: {base_path: 'file://{tmp}'}}", "path: out"),
    "delta": sink_case("c: {type: delta}", "table_path: 'file://{tmp}/out'", mode="append"),
}


@unittest.skipUnless(READY, "set CASCADE_CONNECTOR_SMOKE=1 (downloads connector jars) with a Flink home")
class ConnectorSmokeTest(unittest.TestCase):
    def test_every_connector_plans_against_its_real_jar(self):
        for name, text in CASES.items():
            with self.subTest(connector=name):
                root = tempfile.mkdtemp(prefix="cascade-smoke-")
                data = tempfile.mkdtemp(prefix="cascade-smoke-data-")
                try:
                    with open(os.path.join(root, "pipeline.yaml"), "w") as f:
                        f.write(text.replace("{tmp}", data))
                    r = compile_pipeline(root)
                    self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
                    jars = fetch(r.lowered.manifest["artifacts"], "2.3")
                    old = os.environ.get("CASCADE_EXTRA_JARS")
                    os.environ["CASCADE_EXTRA_JARS"] = os.pathsep.join(jars)
                    try:
                        compile_plan(root, FLINK, os.path.join(root, "plan.json"))
                    except PlanError as e:
                        # Paimon's factory loaded and accepted the options, then looked for the table
                        if not (name == "paimon" and "Please create table first" in str(e)):
                            self.fail(f"{name}: {str(e)[-1500:]}")
                    finally:
                        if old is None:
                            os.environ.pop("CASCADE_EXTRA_JARS", None)
                        else:
                            os.environ["CASCADE_EXTRA_JARS"] = old
                finally:
                    shutil.rmtree(root, ignore_errors=True)
                    shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
