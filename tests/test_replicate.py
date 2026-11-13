"""replicate: whole-database replication compiled to a Flink CDC pipeline definition."""
import os
import shutil
import tempfile
import unittest

import yaml

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.deploy import build

from helpers import compile_text

REPL = """
pipeline: crm_replica
connections:
  crm: {type: postgres-cdc, properties: {hostname: db, username: cdc, password: "${secret:crm/pw}", database: app}}
  lake: {type: paimon, properties: {warehouse: "s3://lake/wh"}}
  bus: {type: kafka, properties: {bootstrap_servers: "k:9092"}}
  shopdb: {type: mysql-cdc, properties: {hostname: m, username: u, password: "${secret:m}"}}
replicate:
  from: crm
  tables: [public.orders, "public.customer_*"]
  exclude: public.audit_log
  to: lake
  route: [{from: "public.orders_*", to: shop.orders}]
  table_options: {bucket: 4}
deploy: {image: x, checkpoints: {dir: "s3://st/cp"}}
"""


def errors(r):
    return sorted(d.code for d in r.diagnostics.items if d.severity == "error")


class ReplicateTest(unittest.TestCase):
    def test_postgres_to_paimon_definition(self):
        r = compile_text(REPL, base=False)
        self.assertEqual(errors(r), [], [d.format() for d in r.diagnostics.items])
        doc = yaml.safe_load(r.lowered.cdc_pipeline)
        self.assertEqual(doc["source"]["type"], "postgres")
        self.assertEqual(doc["source"]["tables"], r"app.public.orders,app.public.customer_\.*")
        self.assertEqual(doc["source"]["tables.exclude"], "app.public.audit_log")
        self.assertEqual(doc["source"]["slot.name"], "cascade_crm_replica")
        self.assertEqual(doc["source"]["password"], "${secret:crm/pw}")
        self.assertIn("password: '${secret:crm/pw}'", r.lowered.cdc_pipeline)  # quoted for substitution
        self.assertEqual(doc["sink"], {"type": "paimon", "catalog.properties.metastore": "filesystem",
                                       "catalog.properties.warehouse": "s3://lake/wh",
                                       "table.properties.bucket": "4"})
        self.assertEqual(doc["route"], [{"source-table": r"app.public.orders_\.*", "sink-table": "shop.orders"}])
        self.assertEqual(doc["pipeline"]["schema.change.behavior"], "EVOLVE")
        self.assertIn('"on_slot_loss": "refuse"', r.lowered.cdc_guard)
        self.assertIn("org.apache.flink:flink-cdc-pipeline-connector-postgres", r.lowered.manifest["artifacts"])

    def test_new_tables_are_opt_in_with_a_warning(self):
        r = compile_text(REPL, base=False)
        self.assertNotIn("scan.newly-added-table.enabled", yaml.safe_load(r.lowered.cdc_pipeline)["source"])
        r = compile_text(REPL.replace("  to: lake\n", "  to: lake\n  capture_new_tables: true\n"), base=False)
        self.assertIs(yaml.safe_load(r.lowered.cdc_pipeline)["source"]["scan.newly-added-table.enabled"], True)
        self.assertIn("CD603", [d.code for d in r.diagnostics.items])

    def test_mysql_to_kafka(self):
        r = compile_text(REPL.replace("from: crm", "from: shopdb").replace("to: lake", "to: bus")
                         .replace("public.", "shop."), base=False)
        self.assertEqual(errors(r), [], [d.format() for d in r.diagnostics.items])
        doc = yaml.safe_load(r.lowered.cdc_pipeline)
        self.assertEqual(doc["source"]["tables"], r"shop.orders,shop.customer_\.*")
        self.assertNotIn("slot.name", doc["source"])
        self.assertEqual(doc["sink"]["type"], "kafka")
        self.assertIsNone(r.lowered.cdc_guard)
        self.assertIn("com.mysql:mysql-connector-j", r.lowered.manifest["artifacts"])

    def test_errors(self):
        for text, code in ((REPL + "sinks:\n  out: {from: x, connection: print}\n", "CD601"),
                           (REPL.replace("to: lake", "to: crm"), "CD602"),
                           (REPL.replace("  tables: [public.orders, \"public.customer_*\"]\n", ""), "CD005"),
                           (REPL.replace("  to: lake\n", "  to: lake\n  schema_changes: sometimes\n"), "CD014")):
            with self.subTest(code=code):
                self.assertIn(code, errors(compile_text(text, base=False)))

    def test_deploy_runs_cdc_runner(self):
        r = compile_text(REPL, base=False)
        out = tempfile.mkdtemp()
        try:
            info = build(r, out)
            self.assertEqual(info["mode"], "replicate")
            with open(os.path.join(out, "flinkdeployment.yaml")) as f:
                fd = yaml.safe_load(f)
            self.assertEqual(fd["spec"]["job"]["entryClass"], "dev.cascade.runner.CdcRunner")
            self.assertEqual(fd["spec"]["job"]["args"][:2], ["--pipeline", "/opt/cascade/job/cdc-pipeline.yaml"])
            with open(os.path.join(out, "configmap.yaml")) as f:
                cm = yaml.safe_load(f)
            self.assertEqual(set(cm["data"]), {"cdc-pipeline.yaml", "guard.sql"})
            with open(os.path.join(out, "Dockerfile")) as f:
                docker = f.read()
            self.assertIn("ADD --checksum=sha512:", docker)
            self.assertIn("flink-cdc-pipeline-connector-postgres", docker)
        finally:
            shutil.rmtree(out)


if __name__ == "__main__":
    unittest.main()
