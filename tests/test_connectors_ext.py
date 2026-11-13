"""Custom connectors and the catalog additions (compile level; real jars: test_connector_smoke.py)."""
import os
import shutil
import tempfile
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.connectors import TESTED_VERSIONS

from helpers import project

CUSTOM = """
pipeline: ext
defaults: {state_ttl: 1d}
connections:
  ch:
    type: custom
    connector: clickhouse
    artifacts: ["com.example:flink-connector-clickhouse:1.2.3"]
    sink_modes: [append, upsert]
    properties: {url: "clickhouse://ch:8123", password: "${secret:ch/pw}"}
sources:
  gen: {connection: datagen, columns: {id: STRING NOT NULL, v: BIGINT}}
models:
  totals:
    sql: SELECT id, SUM(v) AS v FROM gen GROUP BY id
sinks:
  out:
    from: totals
    connection: ch
    mode: upsert
    primary_key: [id]
    table-name: totals
"""


def compiled(text, base=False):
    root = project(text, base=base)
    try:
        return compile_pipeline(root)
    finally:
        shutil.rmtree(root)


class CustomConnectorTest(unittest.TestCase):
    def test_options_pass_through_and_artifacts_are_pinned(self):
        r = compiled(CUSTOM)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertEqual(r.checked.options["out"], {"connector": "clickhouse", "url": "clickhouse://ch:8123",
                                                    "password": "${secret:ch/pw}", "table-name": "totals"})
        self.assertIn("com.example:flink-connector-clickhouse:1.2.3", r.lowered.manifest["artifacts"])

    def test_dockerfile_uses_the_pinned_version(self):
        from cascade_dsl import deploy
        r = compiled(CUSTOM)
        self.assertIn("ARG FLINK_CONNECTOR_CLICKHOUSE_VERSION=1.2.3", deploy.dockerfile(r, {}))

    def test_literal_secret_is_rejected(self):
        r = compiled(CUSTOM.replace('"${secret:ch/pw}"', "hunter2"))
        self.assertIn("CD201", r.diagnostics.codes())

    def test_mode_not_declared(self):
        r = compiled(CUSTOM.replace("    sink_modes: [append, upsert]\n", ""))
        self.assertIn("CD404", r.diagnostics.codes())

    def test_needs_connector_and_versioned_artifacts(self):
        r = compiled(CUSTOM.replace("    connector: clickhouse\n", "").replace(":1.2.3", ""))
        self.assertEqual([d.code for d in r.diagnostics.errors][:2], ["CD005", "CD005"])

    def test_custom_keys_only_on_custom_connections(self):
        r = compiled(CUSTOM.replace("type: custom", "type: kafka"))
        self.assertTrue(r.diagnostics.has_errors)


class CatalogAdditionsTest(unittest.TestCase):
    def test_mysql_cdc_ships_the_driver(self):
        r = compiled("""
pipeline: m
connections:
  db: {type: mysql-cdc, properties: {hostname: h, username: u, password: "${secret:db/pw}"}}
sources:
  t: {connection: db, table: app.t, columns: {id: BIGINT NOT NULL}, primary_key: [id]}
sinks:
  out: {from: t, connection: print, mode: upsert, primary_key: [id]}
""")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("com.mysql:mysql-connector-j", r.lowered.manifest["artifacts"])

    def test_lakehouse_sources_are_changelogs_only_when_keyed(self):
        text = """
pipeline: lake
connections:
  lake: {type: paimon, properties: {warehouse: "s3://lake"}}
sources:
  t:
    connection: lake
    table: db.t
    columns: {id: BIGINT NOT NULL, v: BIGINT}
    PK
models:
  m:
    sql: SELECT * FROM t
sinks:
  out: {from: m, connection: print, mode: upsert, primary_key: [id]}
"""
        keyed = compiled(text.replace("PK", "primary_key: [id]"))
        self.assertEqual(keyed.checked.pipeline.relations["t"].changelog, "updating")
        self.assertEqual(keyed.checked.options["t"]["path"], "s3://lake/db.db/t")
        plain = compiled(text.replace("    PK\n", ""))
        self.assertEqual(plain.checked.pipeline.relations["t"].changelog, "append")

    def test_every_catalog_artifact_has_a_tested_version(self):
        from cascade_dsl.connectors import CATALOG
        coords = set()
        for spec in CATALOG.values():
            if spec.artifact:
                coords.add(spec.artifact.format(flink_minor="2.3"))
        coords.update({"com.mysql:mysql-connector-j", "org.apache.hadoop:hadoop-client-api"})
        self.assertEqual(sorted(c for c in coords if c not in TESTED_VERSIONS), [])


if __name__ == "__main__":
    unittest.main()


class SlotGuardTest(unittest.TestCase):
    def test_postgres_cdc_sources_get_a_restore_guard(self):
        import json
        r = compiled("""
pipeline: g
connections:
  db: {type: postgres-cdc, properties: {hostname: h, database: d, username: u, password: "${secret:crm/password}"}}
sources:
  t: {connection: db, table: public.t, columns: {id: BIGINT NOT NULL}, primary_key: [id]}
sinks:
  out: {from: t, connection: print, mode: upsert, primary_key: [id]}
""")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        g = next(s for s in r.lowered.statements if s.kind == "guard")
        spec = json.loads(g.sql[len("-- @@require-slot"):])
        self.assertEqual((spec["slot"], spec["password_env"], spec["source"]),
                         ("g_t", "CASCADE_SECRET_CRM_PASSWORD", "t"))
        self.assertNotIn("password", spec)  # the secret itself never lands in the script


class SlotLossHealingTest(unittest.TestCase):
    PIPE = """
pipeline: g
connections:
  db: {type: postgres-cdc, properties: {hostname: h, database: d, username: u, password: "${secret:crm/password}"}}
sources:
  t: {connection: db, table: public.t, columns: {id: BIGINT NOT NULL}, primary_key: [id]POLICY}
sinks:
  out: {from: t, connection: print, mode: upsert, primary_key: [id]}
"""

    def test_resnapshot_compiles_to_the_healing_source(self):
        import json
        r = compiled(self.PIPE.replace("POLICY", ", on_slot_loss: resnapshot"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        o = r.checked.options["t"]
        self.assertEqual((o["connector"], o["delegate"], o["slot.name"]), ("cascade-healing-cdc", "postgres-cdc", "g_t"))
        g = next(s for s in r.lowered.statements if s.kind == "guard")
        self.assertEqual(json.loads(g.sql[len("-- @@require-slot"):])["on_slot_loss"], "resnapshot")

    def test_refuse_is_the_default_and_batch_reads_plain_cdc(self):
        self.assertEqual(compiled(self.PIPE.replace("POLICY", "")).checked.options["t"]["connector"], "postgres-cdc")
        r = compiled("mode: batch\n" + self.PIPE.replace("POLICY", ", on_slot_loss: resnapshot"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertEqual(r.checked.options["t"]["connector"], "jdbc")  # batch reads the table with JDBC
        self.assertNotIn("delegate", r.checked.options["t"])


class PatchedJarsTest(unittest.TestCase):
    def test_dynamodb_jar_is_fixed_locally_and_in_the_image(self):
        import zipfile
        from cascade_dsl import deploy, jars
        from cascade_dsl.connectors import PATCHES
        d = tempfile.mkdtemp()
        try:
            src = os.path.join(d, "x.jar")
            res = PATCHES["org.apache.flink:flink-sql-connector-dynamodb"][0]
            with zipfile.ZipFile(src, "w") as z:
                z.writestr(res, "# Licensed to the ASF\n# more\nsoftware.amazon.Foo\n")
                z.writestr("other.txt", "# keep\n")
            out = jars.patch(src, [res])
            with zipfile.ZipFile(out) as z:
                self.assertEqual(z.read(res), b"software.amazon.Foo\n")
                self.assertEqual(z.read("other.txt"), b"# keep\n")
        finally:
            shutil.rmtree(d)
        r = compiled("""
pipeline: p
connections:
  dst: {type: dynamodb, properties: {region: us-east-1}}
sources:
  gen: {connection: datagen, columns: {id: BIGINT NOT NULL}}
sinks:
  out: {from: gen, connection: dst, mode: upsert, primary_key: [id], table: items}
""")
        df = deploy.dockerfile(r, {})
        self.assertIn("FROM eclipse-temurin:17-jdk AS cascade-patches", df)
        self.assertIn("COPY --from=cascade-patches /patches/flink-sql-connector-dynamodb-", df)
        self.assertNotIn("wget -q -P /opt/flink/lib \"https://repo1.maven.org/maven2/org/apache/flink/"
                         "flink-sql-connector-dynamodb", df)


class BundledDriversTest(unittest.TestCase):
    def test_cdc_connectors_that_ship_without_their_driver_get_it(self):
        from cascade_dsl.connectors import CATALOG, LowerCtx
        from cascade_dsl.ir import Connection
        for kind, driver in (("mysql-cdc", "com.mysql:mysql-connector-j"), ("oracle-cdc", "com.oracle.database.jdbc:ojdbc8"),
                             ("db2-cdc", "com.ibm.db2:jcc")):
            with self.subTest(connector=kind):
                self.assertIn(driver, CATALOG[kind].extra_artifacts({}))
