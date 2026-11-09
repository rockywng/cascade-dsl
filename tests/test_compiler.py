import re
import json
import os
import unittest

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.types import TypeParseError, parse_type

from helpers import compile_text, one_model

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "examples", "orders")


def codes(result):
    return result.diagnostics.codes()


class TypesTest(unittest.TestCase):
    def test_normalizes(self):
        self.assertEqual(parse_type("decimal(10,2)").text, "DECIMAL(10, 2)")
        self.assertEqual(parse_type("string not null").text, "STRING NOT NULL")
        self.assertEqual(parse_type("long").text, "BIGINT")
        self.assertEqual(parse_type("map<string, array<int>>").text, "MAP<STRING, ARRAY<INT>>")
        self.assertEqual(parse_type("ROW<a INT, b STRING>").text, "ROW<`a` INT, `b` STRING>")

    def test_rejects(self):
        for bad in ("DECIMAL(40, 2)", "VARCHARZ", "MAP<INT>", "INT(3)", "TIMESTAMP(12)"):
            with self.assertRaises(TypeParseError, msg=bad):
                parse_type(bad)


class ExampleTest(unittest.TestCase):
    def test_example_compiles_cleanly(self):
        r = compile_pipeline(EXAMPLE)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertEqual([d for d in r.diagnostics.items if d.severity.value != "info"], [])
        m = r.lowered.manifest
        self.assertEqual(m["nodes"]["customer_ltv"]["changelog"], "updating")
        self.assertEqual(m["nodes"]["orders_enriched"]["changelog"], "append")
        self.assertEqual(m["nodes"]["orders_enriched"]["time_attributes"], ["order_ts"])
        self.assertEqual(m["nodes"]["revenue_per_minute"]["time_attributes"], [])
        self.assertIn("org.apache.flink:flink-sql-connector-kafka", m["artifacts"])
        self.assertEqual(m["secrets"], ["crm/password"])
        self.assertIsNone(m["nodes"]["clean_orders"]["state_id"])  # stateless
        self.assertIsNotNone(m["nodes"]["customer_ltv"]["state_id"])
        json.dumps(m)

    def test_example_sql_shape(self):
        sql = compile_pipeline(EXAMPLE).lowered.sql
        self.assertIn("'connector' = 'upsert-kafka'", sql)
        self.assertIn("WATERMARK FOR `order_ts` AS `order_ts` - INTERVAL '5' SECOND", sql)
        self.assertIn("CASCADE_ASSERT((order_id IS NOT NULL) IS TRUE", sql)
        self.assertIn("INSERT INTO `clean_orders__quarantine`", sql)
        self.assertIn("'slot.name' = 'orders_analytics_customers'", sql)
        self.assertNotIn("${var:", sql)
        self.assertTrue(sql.rstrip().endswith("END;"))

    def test_env_overrides_vars(self):
        r = compile_pipeline(EXAMPLE, env="prod")
        self.assertTrue(r.ok)
        self.assertIn("s3://prod-lake/warehouse", r.lowered.sql)
        self.assertIn("kafka/prod/bootstrap", r.lowered.manifest["secrets"])

    def test_unknown_env(self):
        self.assertIn("CD007", codes(compile_pipeline(EXAMPLE, env="staging")))


class StructureTest(unittest.TestCase):
    def test_unknown_key_suggests(self):
        r = compile_text("modles: {}\n")
        d = r.diagnostics.items[0]
        self.assertEqual(d.code, "CD001")
        self.assertIn("models", d.hint)
        with open(d.location.file) as f:  # line numbers point into pipeline.yaml
            self.assertTrue(f.read().splitlines()[d.location.line - 1].startswith("modles"))

    def test_undefined_var(self):
        r = compile_text("pipeline: x\nconnections:\n  k: {type: kafka, properties: {bootstrap_servers: '${var:nope}'}}\n",
                         base=False)
        self.assertIn("CD004", codes(r))

    def test_literal_secret_rejected(self):
        r = compile_text("""
            pipeline: x
            connections:
              db: {type: postgres-cdc, properties: {hostname: h, database: d, username: u, password: hunter2}}
            sources:
              t: {connection: db, table: public.t, columns: {id: INT}, primary_key: [id]}
            sinks:
              o: {from: t, connection: print, mode: upsert, primary_key: [id]}
            """, base=False)
        self.assertIn("CD201", codes(r))

    def test_bad_type(self):
        r = compile_text("""
            pipeline: x
            sources:
              t: {connection: datagen, columns: {id: INTEGERR}}
            sinks:
              o: {from: t, connection: print}
            """, base=False)
        self.assertIn("CD010", codes(r))


class SemanticTest(unittest.TestCase):
    def test_clean_model(self):
        r = one_model("SELECT customer_id AS k, SUM(amount) AS total FROM orders GROUP BY customer_id")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("`total` DECIMAL(38, 2)", r.lowered.sql)

    def test_unknown_table_suggests(self):
        r = one_model("SELECT order_id AS k FROM ordrs", sink_mode="append")
        d = next(x for x in r.diagnostics.items if x.code == "CD302")
        self.assertIn("orders", d.hint)

    def test_sql_error_location(self):
        r = one_model("SELECT order_id AS k\nFROM orders\nWHERE (amount > 0", sink_mode="append")
        d = next(x for x in r.diagnostics.items if x.code == "CD300")
        self.assertIsNotNone(d.location.line)

    def test_cycle(self):
        r = compile_text("""
            defaults: {state_ttl: 1d}
            models:
              a: {sql: SELECT * FROM b}
              b: {sql: SELECT * FROM a}
            sinks:
              o: {from: a, connection: print}
            """)
        self.assertIn("CD303", codes(r))

    def test_append_sink_rejects_updates(self):
        r = one_model("SELECT customer_id AS k, COUNT(*) AS n FROM orders GROUP BY customer_id", sink_mode="append")
        d = next(x for x in r.diagnostics.items if x.code == "CD407")
        self.assertIn("GROUP BY", d.message)

    def test_cdc_source_is_updating(self):
        r = one_model("SELECT customer_id AS k, tier FROM customers", sink_mode="append")
        self.assertIn("CD407", codes(r))

    def test_unbounded_state_needs_ttl(self):
        r = one_model("SELECT customer_id AS k, COUNT(*) AS n FROM orders GROUP BY customer_id", defaults="")
        self.assertIn("CD350", codes(r))
        r = one_model("SELECT customer_id AS k, COUNT(*) AS n FROM orders GROUP BY customer_id", defaults="",
                      extra="    allow_unbounded_state: true\n")
        self.assertNotIn("CD350", codes(r))

    def test_window_needs_time_attribute(self):
        r = one_model("""
            SELECT window_start AS k, COUNT(*) AS n
            FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(customer_id), INTERVAL '1' MINUTE))
            GROUP BY window_start, window_end""", sink_mode="append")
        self.assertIn("CD330", codes(r))

    def test_time_attribute_lost_after_regular_join(self):
        r = compile_text("""
            defaults: {state_ttl: 1d}
            models:
              j:
                sql: SELECT o.order_id, o.ts FROM orders o JOIN payments p ON o.order_id = p.order_id
              w:
                sql: |
                  SELECT window_start, COUNT(*) AS n
                  FROM TABLE(TUMBLE(TABLE j, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
                  GROUP BY window_start, window_end
            sinks:
              o: {from: w, connection: print}
            """)
        self.assertIn("CD331", codes(r))

    def test_interval_join_keeps_time_and_is_append(self):
        r = compile_text("""
            models:
              j:
                sql: |
                  SELECT o.order_id, o.ts, p.paid
                  FROM orders o JOIN payments p ON o.order_id = p.order_id
                   AND p.pay_ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR
            sinks:
              o: {from: j, connection: print}
            """)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertEqual(r.lowered.manifest["nodes"]["j"]["time_attributes"], ["ts"])

    def test_temporal_join_requires_pk(self):
        r = compile_text("""
            models:
              j:
                sql: |
                  SELECT o.order_id, p.paid FROM orders o
                  JOIN payments FOR SYSTEM_TIME AS OF o.ts AS p ON o.order_id = p.order_id
            sinks:
              o: {from: j, connection: print}
            """)
        self.assertIn("CD340", codes(r))

    def test_contract_mismatch(self):
        r = one_model("SELECT order_id AS k, amount FROM orders", sink_mode="append",
                      extra="    columns: {k: STRING, amt: 'DECIMAL(10, 2)'}\n")
        self.assertIn("CD310", codes(r))

    def test_contract_casts_unknown_types(self):
        r = one_model("SELECT order_id AS k, amount * 2 AS doubled FROM orders", sink_mode="append",
                      extra="    columns: {k: STRING NOT NULL, doubled: 'DECIMAL(12, 2)'}\n")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        self.assertIn("CAST(`doubled` AS DECIMAL(12, 2)) AS `doubled`", r.lowered.sql)

    def test_uninferable_type_needs_contract(self):
        r = one_model("SELECT order_id AS k, amount * 2 AS doubled FROM orders", sink_mode="append")
        d = next(x for x in r.diagnostics.items if x.code == "CD408")
        self.assertIn("doubled", d.message)

    def test_expectation_unknown_column(self):
        r = one_model("SELECT order_id AS k, amount FROM orders", sink_mode="append",
                      extra="    expectations:\n      - {name: pos, check: amout > 0, on_violation: drop}\n")
        d = next(x for x in r.diagnostics.items if x.code == "CD320")
        self.assertIn("amount", d.hint)

    def test_quarantine_needs_destination(self):
        r = one_model("SELECT order_id AS k, amount FROM orders", sink_mode="append",
                      extra="    expectations:\n      - {name: pos, check: amount > 0, on_violation: quarantine}\n")
        self.assertIn("CD322", codes(r))

    def test_group_key_proves_upsert_key(self):
        r = one_model("SELECT customer_id AS k, COUNT(*) AS n FROM orders GROUP BY customer_id")
        self.assertNotIn("CD410", codes(r))
        self.assertEqual(r.checked.pipeline.relations["m"].upsert_key, ["k"])
        r = one_model("SELECT customer_id AS k, tier FROM customers", pk="k")
        self.assertNotIn("CD410", codes(r))  # CDC primary key passes through the rename
        r = one_model("SELECT customer_id, tier AS k, COUNT(*) AS n FROM customers GROUP BY customer_id, tier")
        self.assertIn("CD410", codes(r))

    def test_upsert_requires_pk_in_output(self):
        r = one_model("SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id", pk="cust")
        self.assertIn("CD406", codes(r))

    def test_unused_source_warns(self):
        r = one_model("SELECT order_id AS k FROM orders", sink_mode="append")
        self.assertTrue(r.ok)
        self.assertEqual(sorted(d.node for d in r.diagnostics.items if d.code == "CD501"), ["customers", "payments"])

    def test_connector_property_validation(self):
        r = compile_text("""
            sinks:
              o: {from: orders, connection: bus, topik: x}
            """)
        self.assertIn("CD210", codes(r))  # unknown property, with suggestion
        self.assertIn("CD209", codes(r))  # missing topic
        self.assertTrue(any("did you mean 'topic'" in d.message for d in r.diagnostics.items))

    def test_star_and_cte(self):
        r = compile_text("""
            models:
              m:
                sql: |
                  WITH big AS (SELECT * FROM orders WHERE amount > 100)
                  SELECT * FROM big
            sinks:
              o: {from: m, connection: print}
            """)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        rel = r.checked.pipeline.relations["m"]
        self.assertEqual(rel.inputs, ["orders"])
        self.assertEqual([c.name for c in rel.columns], ["order_id", "customer_id", "amount", "ts"])
        self.assertEqual(rel.rowtime, {"ts"})

    def test_subquery_dedup_inference(self):
        r = one_model("""
            SELECT order_id AS k, amount, rn FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ts DESC) AS rn FROM orders
            ) t WHERE rn = 1""")
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.items])
        cols = {c.name: c.type.text for c in r.checked.pipeline.relations["m"].columns}
        self.assertEqual(cols, {"k": "STRING NOT NULL", "amount": "DECIMAL(10, 2)", "rn": "BIGINT NOT NULL"})
        self.assertEqual(r.checked.pipeline.relations["m"].changelog, "updating")


class SinkSafetyTest(unittest.TestCase):
    """Connector defaults found by integration/soak.py."""

    def test_exactly_once_kafka_gets_a_broker_compatible_timeout_and_prefix(self):
        r = compile_text("""
            models:
              m: {sql: SELECT order_id FROM orders}
            sinks:
              out: {from: m, connection: bus, topic: t, delivery: exactly-once}
            """)
        opts = r.checked.options["out"]
        # the Flink default (1 h) exceeds the broker default maximum (15 min) and crash-loops the job
        self.assertEqual(opts["properties.transaction.timeout.ms"], "900000")
        self.assertEqual(opts["sink.transactional-id-prefix"], "test_pipe.out")

    def test_at_least_once_kafka_has_no_transactions(self):
        r = compile_text("""
            models:
              m: {sql: SELECT order_id FROM orders}
            sinks:
              out: {from: m, connection: bus, topic: t}
            """)
        self.assertNotIn("properties.transaction.timeout.ms", r.checked.options["out"])

    def test_jdbc_sink_outage_behaviour_is_reported(self):
        r = compile_text("""
            pipeline: jdbc_out
            connections:
              wh: {type: jdbc, properties: {url: "jdbc:postgresql://db/x", password: "${secret:p}"}}
            sources:
              ticks:
                connection: datagen
                columns: {k: STRING}
            sinks:
              out: {from: ticks, connection: wh, table: t}
            """, base=False)
        self.assertTrue(r.ok, [d.format() for d in r.diagnostics.errors])
        self.assertIn("CD215", codes(r))

    def test_postgres_jdbc_connections_keep_idle_connections_alive(self):
        """Found on EKS: PrivateLink drops a flow idle for 350 s silently, and the Postgres driver
        (keepalive off by default) then blocked the sink forever on its next write."""
        def url_of(u):
            r = compile_text(f"""
                pipeline: jdbc_ka
                connections:
                  wh: {{type: jdbc, properties: {{url: "{u}", password: "${{secret:p}}"}}}}
                sources:
                  ticks:
                    connection: datagen
                    columns: {{k: STRING}}
                sinks:
                  out: {{from: ticks, connection: wh, table: t}}
                """, base=False)
            self.assertTrue(r.ok, [d.format() for d in r.diagnostics.errors])
            return re.search(r"'url' = '([^']*)'", r.lowered.sql).group(1)
        self.assertEqual(url_of("jdbc:postgresql://db:5432/x"), "jdbc:postgresql://db:5432/x?tcpKeepAlive=true")
        self.assertEqual(url_of("jdbc:postgresql://db/x?sslmode=require"),
                         "jdbc:postgresql://db/x?sslmode=require&tcpKeepAlive=true")
        self.assertEqual(url_of("jdbc:postgresql://db/x?tcpKeepAlive=false"), "jdbc:postgresql://db/x?tcpKeepAlive=false")
        self.assertEqual(url_of("jdbc:mysql://db/x"), "jdbc:mysql://db/x")

    def test_the_same_source_compiles_to_the_same_sql_wherever_it_is(self):
        """The generated SQL named the source by its path relative to the working directory, so one
        pipeline pushed from two checkouts (or built server side in temp dirs) got two versions."""
        import tempfile
        src = "pipeline: same\nsources:\n  s: {connection: datagen, columns: {k: STRING}}\n" \
              "sinks:\n  out: {from: s, connection: blackhole}\n"
        sqls = []
        for _ in range(2):
            d = tempfile.mkdtemp()
            with open(os.path.join(d, "pipeline.yaml"), "w") as f:
                f.write(src)
            r = compile_pipeline(d, None)
            self.assertTrue(r.ok, [x.format() for x in r.diagnostics.errors])
            sqls.append(r.lowered.sql)
        self.assertEqual(sqls[0], sqls[1])
        self.assertIn("from pipeline.yaml", sqls[0].splitlines()[0])


if __name__ == "__main__":
    unittest.main()
