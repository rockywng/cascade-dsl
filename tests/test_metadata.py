"""Metadata and computed columns, topic lists and patterns."""
import os
import shutil
import unittest

from cascade_dsl import testing
from cascade_dsl.jvm import TARGET, find_flink_home

from helpers import compile_text, project

FLINK = find_flink_home()
READY = FLINK is not None and os.path.exists(os.path.join(TARGET, "cascade-tools.jar"))

KAFKA = """
pipeline: meta
connections:
  k: {type: kafka, properties: {bootstrap_servers: "b:9092"}}
"""


def codes(r):
    return [d.code for d in r.diagnostics.items]


def table(r, name):
    sql = r.lowered.sql
    start = sql.index(f"CREATE TEMPORARY TABLE `{name}`")
    return sql[start:sql.index(");", start)]


class TopicTest(unittest.TestCase):
    def test_topic_list_is_semicolon_separated(self):
        r = compile_text(KAFKA + """
sources:
  ev: {connection: k, topic: [a.v1, b.v1], columns: {id: STRING}}
sinks:
  out: {from: ev, connection: print}
""", base=False)
        self.assertTrue(r.ok, codes(r))
        self.assertIn("'topic' = 'a.v1;b.v1'", table(r, "ev"))

    def test_topic_pattern(self):
        r = compile_text(KAFKA + """
sources:
  ev: {connection: k, topic_pattern: "orders\\\\..*", columns: {id: STRING}}
sinks:
  out: {from: ev, connection: print}
""", base=False)
        self.assertTrue(r.ok, codes(r))
        self.assertIn("'topic-pattern' = 'orders\\..*'", table(r, "ev"))

    def test_exactly_one_of_topic_and_pattern(self):
        for src in ("{connection: k, columns: {id: STRING}}",
                    "{connection: k, topic: a, topic_pattern: b, columns: {id: STRING}}"):
            r = compile_text(KAFKA + f"sources:\n  ev: {src}\nsinks:\n  out: {{from: ev, connection: print}}\n",
                             base=False)
            self.assertIn("CD209", codes(r))


class MetadataTest(unittest.TestCase):
    def test_source_metadata_and_computed_columns(self):
        r = compile_text(KAFKA + """
sources:
  ev:
    connection: k
    topic: a
    columns:
      id: STRING
      epoch_ms: BIGINT
      kts: "TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'"
      part: {type: INT, metadata: partition, virtual: true}
      offset: {type: BIGINT, metadata: true}
      ts: {type: TIMESTAMP_LTZ(3), expression: "TO_TIMESTAMP_LTZ(epoch_ms, 3)"}
    event_time: {column: ts, max_out_of_orderness: 5s}
sinks:
  out: {from: ev, connection: print}
""", base=False)
        self.assertTrue(r.ok, codes(r))
        ddl = table(r, "ev")
        self.assertIn("`kts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'", ddl)
        self.assertIn("`part` INT METADATA FROM 'partition' VIRTUAL", ddl)
        self.assertIn("`offset` BIGINT METADATA FROM 'offset'", ddl)
        self.assertIn("`ts` AS TO_TIMESTAMP_LTZ(epoch_ms, 3)", ddl)
        self.assertIn("WATERMARK FOR `ts`", ddl)

    def test_unknown_key_wrong_type_and_read_only(self):
        r = compile_text(KAFKA + """
sources:
  ev:
    connection: k
    topic: a
    columns:
      id: STRING
      bad: {type: STRING, metadata: tiemstamp}
      ts: {type: STRING, metadata: timestamp}
models:
  m: {sql: "SELECT id, CAST(id AS INT) AS p FROM ev"}
sinks:
  out:
    from: m
    connection: k
    topic: b
    metadata: {p: partition}
""", base=False)
        self.assertEqual(sorted(codes(r)), ["CD220", "CD220", "CD221"])

    def test_cdc_metadata_and_resnapshot(self):
        r = compile_text("""
models:
  m: {sql: "SELECT customer_id, op_ts FROM customers2"}
sinks:
  out: {from: m, connection: print, mode: upsert, primary_key: [customer_id]}
sources:
  customers2:
    connection: db
    table: public.c2
    primary_key: [customer_id]
    on_slot_loss: resnapshot
    columns:
      customer_id: STRING NOT NULL
      op_ts: TIMESTAMP_LTZ(3) METADATA
""")
        self.assertIn("CD222", codes(r))

    def test_metadata_key_cannot_be_primary_key(self):
        r = compile_text("""
sources:
  c3:
    connection: db
    table: public.c3
    primary_key: [tbl]
    columns:
      tbl: STRING METADATA FROM 'table_name'
sinks:
  out: {from: c3, connection: print, mode: upsert, primary_key: [tbl]}
""")
        self.assertIn("CD106", codes(r))

    def test_sink_metadata_and_topic_routing(self):
        r = compile_text(KAFKA + """
sources:
  ev: {connection: k, topic: a, columns: {id: STRING, region: STRING NOT NULL}}
models:
  m: {sql: "SELECT id, region FROM ev"}
sinks:
  out:
    from: m
    connection: k
    topic: [orders.eu, orders.us]
    metadata: {region: topic}
""", base=False)
        self.assertTrue(r.ok, codes(r))
        self.assertIn("`region` STRING METADATA FROM 'topic'", table(r, "out"))
        self.assertIn("'topic' = 'orders.eu;orders.us'", table(r, "out"))
        single = compile_text(KAFKA + """
sources:
  ev: {connection: k, topic: a, columns: {id: STRING, region: STRING NOT NULL}}
sinks:
  out: {from: ev, connection: k, topic: orders, metadata: {region: topic}}
""", base=False)
        self.assertIn("CD209", codes(single))

    def test_contract_casts_without_expectations(self):
        # used to compile to `WHERE ;`
        r = compile_text(KAFKA + """
sources:
  ev: {connection: k, topic: a, columns: {id: STRING}}
models:
  m:
    sql: SELECT id, MAP['h', id] AS hs FROM ev
    columns: {id: STRING, hs: "MAP<STRING, STRING>"}
sinks:
  out: {from: m, connection: print}
""", base=False)
        self.assertTrue(r.ok, codes(r))
        self.assertNotIn("WHERE ;", r.lowered.sql)


@unittest.skipUnless(READY, "set FLINK_HOME and run runtime/build.sh")
class MetadataFixtureTest(unittest.TestCase):
    def test_fixtures_supply_metadata_and_computed_columns_derive(self):
        d = project(KAFKA + """
defaults: {state_ttl: 1d}
sources:
  ev:
    connection: k
    topic: a
    columns:
      id: STRING
      epoch_ms: BIGINT
      kts: "TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'"
      part: {type: INT, metadata: partition, virtual: true}
      doubled: {type: BIGINT, expression: "epoch_ms * 2"}
    event_time: {column: kts, max_out_of_orderness: 0s}
models:
  per_minute:
    sql: |
      SELECT window_start, COUNT(*) AS n, MAX(part) AS max_part, SUM(doubled) AS d
      FROM TABLE(TUMBLE(TABLE ev, DESCRIPTOR(kts), INTERVAL '1' MINUTE))
      GROUP BY window_start, window_end
sinks:
  out: {from: per_minute, connection: print}
""", base=False, files={"tests/t.yaml": """
            tests:
              - name: metadata_from_fixtures
                target: per_minute
                given:
                  ev:
                    - {id: a, epoch_ms: 1, kts: "2026-01-01 00:00:10", part: 3}
                    - {id: b, epoch_ms: 2, kts: "2026-01-01 00:00:20", part: 1}
                    - {id: c, epoch_ms: 5, kts: "2026-01-01 00:01:30", part: 0}
                expect:
                  rows:
                    - {window_start: "2026-01-01 00:00:00", n: 2, max_part: 3, d: 6}
                    - {window_start: "2026-01-01 00:01:00", n: 1, max_part: 0, d: 10}
            """})
        r, results = testing.run(d, FLINK)
        self.assertIsNotNone(results, [x.format() for x in r.diagnostics.errors])
        for t in results:
            self.assertTrue(t.passed, f"{t.case.name}: {t.message}")
        shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
