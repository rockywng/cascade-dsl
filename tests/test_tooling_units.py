"""Unit tests for the JVM-independent parts of the tooling: UID pinning, Avro import, schema lock,
and test-result evaluation."""
import http.server
import json
import os
import shutil
import tempfile
import threading
import unittest
from decimal import Decimal

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.plans import UID_KEY, pin_uids
from cascade_dsl.registry import SchemaImportError, avro_columns, fetch
from cascade_dsl.testing import TestCase, _evaluate, _materialize
from cascade_dsl.diagnostics import Location

from helpers import one_model


def node(i, typ, **kw):
    return {"id": i, "type": typ, "description": f"{typ}#{i}", **kw}


def plan(nodes, edges):
    return {"flinkVersion": "2.5", "nodes": nodes,
            "edges": [{"source": s, "target": t, "shuffle": {"type": "FORWARD"}} for s, t in edges]}


SRC = node(1, "stream-exec-table-source-scan_2", scanTableSource={"table": {"identifier": "orders"}})
CALC = node(2, "stream-exec-calc_1", condition="amount > 0")
AGG = node(3, "stream-exec-group-aggregate_1", grouping=[0], state=[{"index": 0}])
SINK = node(4, "stream-exec-sink_2", dynamicTableSink={"table": {"identifier": "out"}})
OLD = plan([SRC, CALC, AGG, SINK], [(1, 2), (2, 3), (3, 4)])


def uid_of(p, typ_prefix):
    n = next(n for n in p["nodes"] if n["type"].startswith(typ_prefix))
    return n["configuration"][UID_KEY].replace("<id>", str(n["id"]))


class PinUidsTest(unittest.TestCase):
    def test_inserted_node_keeps_downstream_uids(self):
        # a new branch is inserted before the aggregate, renumbering every node after it
        new = plan([dict(SRC), node(2, "stream-exec-calc_1", condition="currency = 'EUR'"),
                    node(3, "stream-exec-sink_2", dynamicTableSink={"table": {"identifier": "eur"}}),
                    dict(CALC, id=4), dict(AGG, id=5), dict(SINK, id=6)],
                   [(1, 2), (2, 3), (1, 4), (4, 5), (5, 6)])
        pinned, report = pin_uids(OLD, new, tag="v2")
        self.assertEqual(uid_of(pinned, "stream-exec-group-aggregate"), "3_<transformation>")
        self.assertEqual(uid_of(pinned, "stream-exec-table-source-scan"), "1_<transformation>")
        new_sink = next(n for n in pinned["nodes"] if n["id"] == 3)
        self.assertEqual(new_sink["configuration"][UID_KEY], "v2-<id>_<transformation>")
        self.assertEqual(report.dropped_stateful, [])
        self.assertEqual(len(report.new_stateful), 1)

    def test_changed_aggregate_gets_a_new_uid(self):
        new = plan([SRC, CALC, dict(AGG, grouping=[1]), SINK], [(1, 2), (2, 3), (3, 4)])
        pinned, report = pin_uids(OLD, new, tag="v2")
        self.assertEqual(uid_of(pinned, "stream-exec-group-aggregate"), "v2-3_<transformation>")
        self.assertEqual(report.dropped_stateful, ["stream-exec-group-aggregate_1#3 #3"])
        self.assertEqual(len(report.drift), 1)  # the sink keeps its state; only its input changed

    def test_upstream_filter_change_keeps_state_as_drift(self):
        new = plan([SRC, dict(CALC, condition="amount > 1"), AGG, SINK], [(1, 2), (2, 3), (3, 4)])
        pinned, report = pin_uids(OLD, new, tag="v2")
        self.assertEqual(uid_of(pinned, "stream-exec-group-aggregate"), "3_<transformation>")
        self.assertEqual(len(report.drift), 2)  # aggregate and sink
        _, strict = pin_uids(OLD, new, allow_drift=False, tag="v2")
        self.assertEqual(strict.drift, [])
        self.assertEqual(len(strict.dropped_stateful), 1)  # the aggregate
        self.assertEqual(len(strict.dropped_sinks), 1)  # a sink without declared state

    def test_pins_chain_across_versions(self):
        v2, _ = pin_uids(OLD, plan([SRC, CALC, AGG, SINK], [(1, 2), (2, 3), (3, 4)]), tag="v2")
        renumbered = plan([dict(SRC, id=10), dict(CALC, id=11), dict(AGG, id=12), dict(SINK, id=13)],
                          [(10, 11), (11, 12), (12, 13)])
        v3, _ = pin_uids(v2, renumbered, tag="v3")
        self.assertEqual(uid_of(v3, "stream-exec-group-aggregate"), "3_<transformation>")


class StatefulDetectionTest(unittest.TestCase):
    def test_new_temporal_join_needs_history(self):
        # temporal joins carry no "state" entry in plan JSON; they must still count as stateful
        tj = node(5, "stream-exec-temporal-join_1")
        new = plan([SRC, CALC, AGG, SINK, tj], [(1, 2), (2, 3), (3, 4), (2, 5)])
        _, report = pin_uids(OLD, new, tag="v2")
        self.assertIn("stream-exec-temporal-join_1#5 #5", report.new_stateful)
        self.assertEqual([x[0] for x in report.needs_history], ["stream-exec-temporal-join_1#5 #5"])
        self.assertIn("lookups miss", report.needs_history[0][1])

    def test_first_deployment_needs_no_history(self):
        _, report = pin_uids({"nodes": [], "edges": []}, OLD, tag="v1")
        self.assertEqual(report.needs_history, [])


class AvroImportTest(unittest.TestCase):
    SCHEMA = {
        "type": "record", "name": "Order", "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
            {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "note", "type": ["null", "string"], "default": None},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
            {"name": "address", "type": ["null", {"type": "record", "name": "Address", "fields": [
                {"name": "city", "type": "string"}, {"name": "zip", "type": ["null", "int"]}]}]},
            {"name": "prev_status", "type": ["null", "Status"]},
        ]}

    def test_mapping(self):
        cols = dict(avro_columns(self.SCHEMA))
        self.assertEqual(cols, {
            "order_id": "STRING NOT NULL",
            "amount": "DECIMAL(12, 2) NOT NULL",
            "ts": "TIMESTAMP_LTZ(3) NOT NULL",
            "note": "STRING",
            "tags": "ARRAY<STRING NOT NULL> NOT NULL",
            "status": "STRING NOT NULL",
            "address": "ROW<`city` STRING NOT NULL, `zip` INT>",
            "prev_status": "STRING",
        })
        from cascade_dsl.types import parse_type
        for t in cols.values():
            parse_type(t)  # every generated type is valid DSL syntax

    def test_rejects_multi_type_union(self):
        with self.assertRaises(SchemaImportError):
            avro_columns({"type": "record", "name": "x", "fields": [{"name": "v", "type": ["int", "string"]}]})

    def test_fetch_from_registry(self):
        schema = json.dumps(self.SCHEMA)

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/subjects/orders-value/versions/latest":
                    body = json.dumps({"subject": "orders-value", "version": 3, "schema": schema}).encode()
                    self.send_response(200)
                else:
                    body = b'{"error_code": 40401}'
                    self.send_response(404)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            url = f"http://127.0.0.1:{server.server_port}"
            kind, text = fetch(url, "orders-value")
            self.assertEqual(kind, "AVRO")
            self.assertEqual(json.loads(text)["name"], "Order")
            with self.assertRaises(SchemaImportError):
                fetch(url, "missing")
        finally:
            server.shutdown()
            server.server_close()


class SchemaLockTest(unittest.TestCase):
    SQL = "SELECT order_id AS k, amount * 2 AS doubled FROM orders"

    def test_lock_supplies_types_and_detects_staleness(self):
        r = one_model(self.SQL, sink_mode="append")
        self.assertIn("CD408", r.diagnostics.codes())
        root = r.checked.pipeline.root_dir
        from cascade_dsl.analyze import model_fingerprint
        p = r.checked.pipeline
        fp = model_fingerprint(p, "m", ["orders"], {"orders": p.relations["orders"].columns})
        with open(os.path.join(root, "cascade.lock.json"), "w") as f:
            json.dump({"models": {"m": {"fingerprint": fp, "columns": [
                {"name": "k", "type": "STRING NOT NULL", "time_attribute": None},
                {"name": "doubled", "type": "DECIMAL(21, 2)", "time_attribute": None}]}}}, f)
        r2 = compile_pipeline(root)
        self.assertTrue(r2.ok, [d.format() for d in r2.diagnostics.items])
        self.assertIn("`doubled` DECIMAL(21, 2)", r2.lowered.sql)
        self.assertIn("CD360", r2.diagnostics.codes())
        # edit the model: the lock no longer applies
        path = os.path.join(root, "pipeline.yaml")
        with open(path) as f:
            text = f.read()
        with open(path, "w") as f:
            f.write(text.replace("amount * 2", "amount * 3"))
        r3 = compile_pipeline(root)
        self.assertIn("CD361", r3.diagnostics.codes())
        self.assertIn("CD408", r3.diagnostics.codes())
        shutil.rmtree(root)


class EvaluateTest(unittest.TestCase):
    def case(self, rows=None, error=None, ordered=False):
        return TestCase("t", "m", {}, rows, error, ordered, Location("tests/x.yaml", 1))

    def test_materialize_changelog(self):
        rows = [{"kind": "+I", "values": ["a", 1]}, {"kind": "-U", "values": ["a", 1]},
                {"kind": "+U", "values": ["a", 2]}, {"kind": "+I", "values": ["b", 1]},
                {"kind": "-D", "values": ["b", 1]}]
        self.assertEqual(_materialize(rows), [("a", 2)])

    def test_type_aware_comparison_and_diff(self):
        out = {"columns": [{"name": "k", "type": "STRING"}, {"name": "v", "type": "DECIMAL(38, 2)"},
                           {"name": "ts", "type": "TIMESTAMP(3)"}],
               "rows": [{"kind": "+I", "values": ["a", "15.50", "2026-01-01T10:00"]}]}
        ok = _evaluate(self.case([{"k": "a", "v": 15.5, "ts": "2026-01-01 10:00:00"}]), out)
        self.assertTrue(ok.passed, ok.message)
        bad = _evaluate(self.case([{"k": "a", "v": 15.4}]), out)
        self.assertFalse(bad.passed)
        self.assertIn("missing (expected, not produced)", bad.message)
        self.assertIn("v: 15.4", bad.message)
        unknown = _evaluate(self.case([{"nope": 1}]), out)
        self.assertIn("not an output column", unknown.message)

    def test_expected_error(self):
        out = {"error": "JobExecutionException\n  caused by ExpectationViolation: m.positive failed"}
        self.assertTrue(_evaluate(self.case(error="positive"), out).passed)
        self.assertFalse(_evaluate(self.case(error="other"), out).passed)
        self.assertFalse(_evaluate(self.case(rows=[]), out).passed)

    def test_decimal_normalization(self):
        from cascade_dsl.testing import normalize
        self.assertEqual(normalize("15.50", "DECIMAL(38, 2)"), normalize(15.5, "DECIMAL(10, 1)"))
        self.assertEqual(normalize("15.50", "DECIMAL(38, 2)"), Decimal("15.5"))


if __name__ == "__main__":
    unittest.main()
