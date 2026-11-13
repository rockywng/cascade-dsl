"""deploy.monitoring: Prometheus reporter, PodMonitor, alert rules and a Grafana dashboard."""
import json
import os
import shutil
import tempfile
import unittest

import yaml

from cascade_dsl import deploy, monitoring
from cascade_dsl.compiler import compile_pipeline

from helpers import project

PIPE = """
defaults: {state_ttl: 1d}
deploy: {image: img, checkpoints: {interval: 30s}, monitoring: {alerts: {max_violation_ratio: 0.05}}}
models:
  clean:
    sql: SELECT order_id, amount, ts FROM orders
    expectations:
      - {name: positive, check: amount > 0, on_violation: drop}
sinks:
  out: {from: clean, connection: print}
"""


class MonitoringTest(unittest.TestCase):
    def setUp(self):
        self.root = project(PIPE)
        self.out = tempfile.mkdtemp()
        self.r = compile_pipeline(self.root)
        self.assertTrue(self.r.ok, [d.format() for d in self.r.diagnostics.items])

    def tearDown(self):
        shutil.rmtree(self.root)
        shutil.rmtree(self.out)

    def test_rules_follow_the_pipeline(self):
        rules = monitoring.rules(self.r, self.r.checked.pipeline.deploy)["groups"][0]["rules"]
        names = {x["alert"] for x in rules}
        self.assertTrue({"CascadeJobDown", "CascadeJobRestarting", "CascadeCheckpointsFailing",
                         "CascadeCheckpointsStalled", "CascadeBackpressure", "CascadeSourceLag",
                         "CascadeEventTimeStalled", "CascadeExpectationViolations"} <= names)
        stalled = next(x for x in rules if x["alert"] == "CascadeCheckpointsStalled")
        self.assertIn("> 150", stalled["expr"])  # 5 x the 30 s interval
        ratio = next(x for x in rules if x["alert"] == "CascadeExpectationViolations")
        self.assertIn("> 0.05", ratio["expr"])
        flink = [x for x in rules if not x["alert"].startswith("CascadeReplicationSlot")]
        self.assertTrue(all('job_name="test_pipe"' in x["expr"] for x in flink))
        # the customers source is Postgres CDC: its slot (and later generations) are watched too
        slot = [x for x in rules if x["alert"].startswith("CascadeReplicationSlot")]
        self.assertEqual(len(slot), 4)
        self.assertTrue(all('slot_name=~"test_pipe_customers(_g[0-9a-z]+)?"' in x["expr"] for x in slot))

    def test_deploy_writes_monitoring_and_enables_the_reporter(self):
        deploy.build(self.r, self.out)
        with open(os.path.join(self.out, "flinkdeployment.yaml")) as f:
            fd = yaml.safe_load(f)
        conf = fd["spec"]["flinkConfiguration"]
        self.assertEqual(conf["metrics.reporter.prom.port"], "9249")
        self.assertEqual(fd["spec"]["podTemplate"]["spec"]["containers"][0]["ports"][0]["name"], "metrics")
        with open(os.path.join(self.out, "kustomization.yaml")) as f:
            res = yaml.safe_load(f)["resources"]
        self.assertTrue({"podmonitor.yaml", "prometheusrule.yaml", "grafana-dashboard.yaml"} <= set(res))
        with open(os.path.join(self.out, "monitoring", "dashboard.json")) as f:
            self.assertGreater(len(json.load(f)["panels"]), 8)

    def test_off_by_default(self):
        root = project(PIPE.replace(", monitoring: {alerts: {max_violation_ratio: 0.05}}", ""))
        out = tempfile.mkdtemp()
        try:
            deploy.build(compile_pipeline(root), out)
            self.assertFalse(os.path.exists(os.path.join(out, "prometheusrule.yaml")))
        finally:
            shutil.rmtree(root)
            shutil.rmtree(out)


if __name__ == "__main__":
    unittest.main()
