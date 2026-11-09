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
  lake: {type: paimon, properties: {base_path: "s3://lake"}}
sources:
  t:
    connection: lake
    path: db.db/t
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
