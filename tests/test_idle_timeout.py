"""Per-source idle timeouts: only sources that legitimately go quiet may stop holding back event time."""
import shutil
import unittest

from cascade_dsl.compiler import compile_pipeline

from helpers import project

MODEL = """
defaults: {state_ttl: 1d}
models:
  enriched:
    sql: |
      SELECT o.order_id, o.amount, o.ts, c.tier
      FROM orders AS o
      LEFT JOIN customers FOR SYSTEM_TIME AS OF o.ts AS c ON o.customer_id = c.customer_id
sinks:
  out: {from: enriched, connection: print}
"""


def compiled(yaml_extra: str = "", replace: tuple[str, str] | None = None):
    text = MODEL + yaml_extra
    root = project(text)
    if replace:
        import os
        path = os.path.join(root, "pipeline.yaml")
        with open(path) as f:
            body = f.read()
        assert replace[0] in body
        with open(path, "w") as f:
            f.write(body.replace(replace[0], replace[1], 1))
    try:
        return compile_pipeline(root)
    finally:
        shutil.rmtree(root)


CDC_IDLE = ("    event_time: updated_at\n", "    event_time: {column: updated_at, idle_timeout: 2s}\n")


class IdleTimeoutTest(unittest.TestCase):
    def test_no_idle_timeout_by_default(self):
        r = compiled()
        self.assertTrue(r.ok)
        self.assertNotIn("idle-timeout", r.lowered.sql)

    def test_cdc_idle_timeout_is_job_wide_and_kafka_sources_get_a_long_one(self):
        r = compiled(replace=CDC_IDLE)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("SET 'table.exec.source.idle-timeout' = '2000 ms';", r.lowered.sql)
        self.assertEqual(r.checked.options["orders"]["scan.watermark.idle-timeout"], "60000 ms")
        self.assertEqual(r.checked.options["payments"]["scan.watermark.idle-timeout"], "60000 ms")
        self.assertNotIn("scan.watermark.idle-timeout", r.checked.options["customers"])

    def test_kafka_source_can_opt_out_of_idling(self):
        r = compiled(replace=("    event_time: {column: ts, max_out_of_orderness: 5s}\n",
                              "    event_time: {column: ts, max_out_of_orderness: 5s, idle_timeout: 0s}\n"))
        self.assertTrue(r.ok)
        self.assertEqual(r.checked.options["orders"]["scan.watermark.idle-timeout"], "0 ms")

    def test_kafka_source_with_its_own_idle_timeout(self):
        r = compiled(replace=("    event_time: {column: pay_ts, max_out_of_orderness: 5s}\n",
                              "    event_time: {column: pay_ts, max_out_of_orderness: 5s, idle_timeout: 30s}\n"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertNotIn("table.exec.source.idle-timeout", r.lowered.sql)
        self.assertEqual(r.checked.options["payments"]["scan.watermark.idle-timeout"], "30000 ms")
        self.assertNotIn("scan.watermark.idle-timeout", r.checked.options["orders"])

    def test_job_wide_setting_warns_with_several_event_time_sources(self):
        r = compiled("flink:\n  config:\n    table.exec.source.idle-timeout: 2s\n")
        self.assertTrue(r.ok)
        d = next(x for x in r.diagnostics.items if x.code == "CD108")
        self.assertIn("orders", d.message)
        self.assertIn("drop them as late", d.message)

    def test_job_wide_setting_and_per_source_conflict(self):
        r = compiled("flink:\n  config:\n    table.exec.source.idle-timeout: 2s\n", replace=CDC_IDLE)
        self.assertIn("CD109", r.diagnostics.codes())
        self.assertFalse(r.ok)

    def test_bad_duration(self):
        r = compiled(replace=("    event_time: updated_at\n",
                              "    event_time: {column: updated_at, idle_timeout: soon}\n"))
        self.assertIn("CD006", r.diagnostics.codes())


if __name__ == "__main__":
    unittest.main()


class WatermarkOptionsTest(unittest.TestCase):
    def test_custom_expression(self):
        r = compiled(replace=("    event_time: {column: ts, max_out_of_orderness: 5s}\n",
                              "    event_time: {column: ts, watermark: \"ts - INTERVAL '1' MINUTE\"}\n"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("WATERMARK FOR `ts` AS ts - INTERVAL '1' MINUTE", r.lowered.sql)

    def test_expression_and_delay_are_exclusive(self):
        r = compiled(replace=("    event_time: {column: ts, max_out_of_orderness: 5s}\n",
                              "    event_time: {column: ts, max_out_of_orderness: 5s, watermark: ts}\n"))
        self.assertIn("CD005", r.diagnostics.codes())

    def test_alignment_and_emit_on_kafka(self):
        r = compiled(replace=("    event_time: {column: ts, max_out_of_orderness: 5s}\n",
                              "    event_time: {column: ts, max_out_of_orderness: 5s, emit: on_event, "
                              "alignment: {group: g1, max_drift: 30s}}\n"))
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        o = r.checked.options["orders"]
        self.assertEqual((o["scan.watermark.alignment.group"], o["scan.watermark.alignment.max-drift"],
                          o["scan.watermark.emit.strategy"]), ("g1", "30000 ms", "on-event"))

    def test_alignment_needs_push_down(self):
        r = compiled(replace=("    event_time: updated_at\n",
                              "    event_time: {column: updated_at, alignment: {group: g, max_drift: 1s}}\n"))
        self.assertIn("CD110", r.diagnostics.codes())

    def test_source_watermark_needs_support(self):
        r = compiled(replace=("    event_time: {column: ts, max_out_of_orderness: 5s}\n",
                              "    event_time: {column: ts, watermark: source}\n"))
        self.assertIn("CD111", r.diagnostics.codes())
