"""cascade init; deploy.secrets_from (External Secrets Operator)."""
import os
import shutil
import tempfile
import unittest

import yaml

from cascade_dsl import deploy
from cascade_dsl.cli import main
from cascade_dsl.compiler import compile_pipeline

from helpers import project


class InitTest(unittest.TestCase):
    def test_init_creates_a_valid_project(self):
        d = os.path.join(tempfile.mkdtemp(), "my-orders")
        try:
            self.assertEqual(main(["init", d]), 0)
            r = compile_pipeline(d)
            self.assertTrue(r.ok, [x.format() for x in r.diagnostics.items])
            self.assertEqual(r.checked.pipeline.name, "my_orders")
            self.assertTrue(os.path.exists(os.path.join(d, "tests", "revenue_test.yaml")))
            self.assertEqual(main(["init", d]), 1)  # never overwrites
        finally:
            shutil.rmtree(os.path.dirname(d))


class ExternalSecretTest(unittest.TestCase):
    def test_external_secret_replaces_the_template(self):
        root = project("""
defaults: {state_ttl: 1d}
deploy: {image: img, secrets_from: {store: vault, kind: ClusterSecretStore, prefix: "pipelines/", property_from_key: true}}
models:
  m: {sql: SELECT order_id FROM orders}
sinks:
  out: {from: m, connection: print}
""")
        out = tempfile.mkdtemp()
        try:
            deploy.build(compile_pipeline(root), out)
            self.assertFalse(os.path.exists(os.path.join(out, "secret.template.yaml")))
            with open(os.path.join(out, "externalsecret.yaml")) as f:
                es = yaml.safe_load(f)
            self.assertEqual(es["spec"]["secretStoreRef"], {"name": "vault", "kind": "ClusterSecretStore"})
            self.assertIn({"secretKey": "db.pw", "remoteRef": {"key": "pipelines/db", "property": "pw"}},
                          es["spec"]["data"])
            with open(os.path.join(out, "kustomization.yaml")) as f:
                self.assertIn("externalsecret.yaml", yaml.safe_load(f)["resources"])
        finally:
            shutil.rmtree(root)
            shutil.rmtree(out)


if __name__ == "__main__":
    unittest.main()
