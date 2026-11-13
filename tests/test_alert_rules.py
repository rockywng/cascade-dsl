"""The generated alerts fire, and only when they should: `promtool test rules` on synthetic series.

Needs promtool (PROMTOOL, or on PATH): CI downloads it from the Prometheus release."""
import os
import shutil
import subprocess
import tempfile
import unittest

import yaml

from cascade_dsl import monitoring
from cascade_dsl.compiler import compile_pipeline

from helpers import project

PROMTOOL = os.environ.get("PROMTOOL") or shutil.which("promtool")

PIPE = """
defaults: {state_ttl: 1d}
deploy: {checkpoints: {interval: 30s}, monitoring: {alerts: {max_violation_ratio: 0.05, max_pending_records: 1000}}}
models:
  clean:
    sql: SELECT order_id, amount, ts FROM orders
    expectations:
      - {name: positive, check: amount > 0, on_violation: drop}
  enriched:
    sql: |
      SELECT o.order_id, c.tier FROM clean AS o
      LEFT JOIN customers FOR SYSTEM_TIME AS OF o.ts AS c ON o.order_id = c.customer_id
sinks:
  out: {from: enriched, connection: print}
"""

JOB = 'job_name="test_pipe"'
SLOT = 'slot_name="test_pipe_customers"'
NEVER = "flink_jobmanager_job_uptime{job_name=\"test_pipe\"}"  # a healthy, running job


def labels(rule, extra=None):
    return {**rule["labels"], **(extra or {})}


def annotations(rule, extra=None):
    """The rule's annotations as Prometheus renders them for an alert with these labels."""
    import re
    values = labels(rule, extra)
    return {k: re.sub(r"\{\{\s*\$labels\.(\w+)\s*\}\}", lambda m: values.get(m.group(1), ""), v)
            for k, v in rule["annotations"].items()}


class AlertRulesTest(unittest.TestCase):
    @unittest.skipUnless(PROMTOOL, "needs promtool (set PROMTOOL or put it on PATH)")
    def test_every_alert_fires_on_its_condition_and_not_when_healthy(self):
        root = project(PIPE)
        d = tempfile.mkdtemp(prefix="cascade-alerts-")
        try:
            r = compile_pipeline(root)
            self.assertTrue(r.ok, [x.format() for x in r.diagnostics.items])
            rules = monitoring.rules(r, r.checked.pipeline.deploy)
            by_name = {x["alert"]: x for x in rules["groups"][0]["rules"]}
            with open(os.path.join(d, "alerts.yml"), "w") as f:
                yaml.safe_dump(rules, f)
            up = {"series": NEVER, "values": "1+60000x30"}
            sel = f"{{{JOB}}}"
            # alert -> (input series, eval time, expected labels added by the expression)
            cases = {
                "CascadeJobDown": ([], "6m", {}),
                "CascadeJobRestarting": ([up, {"series": f"flink_jobmanager_job_numRestarts{sel}",
                                               "values": "0 0 0 1 1 1"}], "4m", {"job_name": "test_pipe"}),
                "CascadeCheckpointsFailing": ([up, {"series": f"flink_jobmanager_job_numberOfFailedCheckpoints{sel}",
                                                    "values": "0 0 2 2 2"}], "3m", {"job_name": "test_pipe"}),
                "CascadeCheckpointsStalled": ([up, {"series": f"flink_jobmanager_job_lastCheckpointCompletedTimestamp{sel}",
                                                    "values": "0x20"}], "6m", {}),
                "CascadeBackpressure": ([up, {"series": f"flink_taskmanager_job_task_backPressuredTimeMsPerSecond{sel}",
                                              "values": "900x30"}], "16m", {}),
                "CascadeSourceLag": ([up, {"series": f"flink_taskmanager_job_task_operator_pendingRecords{sel}",
                                           "values": "5000x30"}], "11m", {}),
                "CascadeEventTimeStalled": ([up, {"series": f"flink_taskmanager_job_task_currentInputWatermark{sel}",
                                                  "values": "1000x40"}], "20m", {}),
                "CascadeExpectationViolations": ([up, {
                    "series": f'{monitoring.EXPECT}_checked{{{JOB}, cascade_model="clean", expectation="positive"}}',
                    "values": "0+6000x30"}, {
                    "series": f'{monitoring.EXPECT}_violations{{{JOB}, cascade_model="clean", expectation="positive"}}',
                    "values": "0+600x30"}], "16m", {"cascade_model": "clean", "expectation": "positive"}),
                "CascadeReplicationSlotLag": ([up, {"series": f"{monitoring.SLOT_CURRENT}{{{SLOT}}}", "values": "3e9x30"},
                                               {"series": f"{monitoring.SLOT_FLUSHED}{{{SLOT}}}", "values": "1e6x30"}],
                                              "11m", {"slot_name": "test_pipe_customers"}),
                "CascadeReplicationSlotInactive": ([up, {"series": f"{monitoring.SLOT_ACTIVE}{{{SLOT}}}",
                                                         "values": "0x30"}], "11m",
                                                   {"slot_name": "test_pipe_customers"}),
                "CascadeReplicationSlotAtRisk": ([up, {"series": f"{monitoring.SLOT_SAFE}{{{SLOT}}}",
                                                       "values": "1e6x30"}], "6m",
                                                 {"slot_name": "test_pipe_customers"}),
                "CascadeReplicationSlotGone": ([up], "11m", {}),
            }
            self.assertEqual(set(cases), set(by_name), "a generated alert has no firing test (or vice versa)")
            tests = []
            for name, (series, at, extra) in cases.items():
                tests.append({"interval": "1m", "input_series": series, "alert_rule_test": [
                    {"eval_time": at, "alertname": name,
                     "exp_alerts": [{"exp_labels": labels(by_name[name], extra),
                                     "exp_annotations": annotations(by_name[name], extra)}]}]})
            # healthy: a running job, checkpoints completing, slots fine -> nothing fires
            healthy = [up,
                       {"series": f"flink_jobmanager_job_numRestarts{sel}", "values": "0x30"},
                       {"series": f"flink_jobmanager_job_numberOfFailedCheckpoints{sel}", "values": "0x30"},
                       {"series": f"flink_jobmanager_job_lastCheckpointCompletedTimestamp{sel}",
                        "values": "0+60000x30"},
                       {"series": f"flink_taskmanager_job_task_backPressuredTimeMsPerSecond{sel}", "values": "10x30"},
                       {"series": f"flink_taskmanager_job_task_operator_pendingRecords{sel}", "values": "10x30"},
                       {"series": f"flink_taskmanager_job_task_currentInputWatermark{sel}", "values": "0+60000x30"},
                       {"series": f"{monitoring.SLOT_CURRENT}{{{SLOT}}}", "values": "2e6x30"},
                       {"series": f"{monitoring.SLOT_FLUSHED}{{{SLOT}}}", "values": "1e6x30"},
                       {"series": f"{monitoring.SLOT_ACTIVE}{{{SLOT}}}", "values": "1x30"},
                       {"series": f"{monitoring.SLOT_SAFE}{{{SLOT}}}", "values": "5e9x30"}]
            tests.append({"interval": "1m", "input_series": healthy, "alert_rule_test": [
                {"eval_time": "25m", "alertname": n, "exp_alerts": []} for n in sorted(by_name)]})
            with open(os.path.join(d, "test.yml"), "w") as f:
                yaml.safe_dump({"rule_files": ["alerts.yml"], "evaluation_interval": "1m", "tests": tests}, f)
            p = subprocess.run([PROMTOOL, "test", "rules", os.path.join(d, "test.yml")], capture_output=True,
                               text=True, timeout=120)
            self.assertEqual(p.returncode, 0, (p.stdout + p.stderr)[-6000:])
        finally:
            shutil.rmtree(root)
            shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
