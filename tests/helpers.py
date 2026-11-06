import os
import tempfile
import textwrap

from cascade_dsl.compiler import compile_pipeline

BASE = """
pipeline: test_pipe
connections:
  bus:
    type: kafka
    properties:
      bootstrap_servers: localhost:9092
  db:
    type: postgres-cdc
    properties:
      hostname: db
      database: app
      username: u
      password: ${secret:db/pw}
sources:
  orders:
    connection: bus
    topic: orders
    columns:
      order_id: STRING NOT NULL
      customer_id: STRING
      amount: DECIMAL(10, 2)
      ts: TIMESTAMP(3)
    event_time: {column: ts, max_out_of_orderness: 5s}
  payments:
    connection: bus
    topic: payments
    columns:
      order_id: STRING
      paid: DECIMAL(10, 2)
      pay_ts: TIMESTAMP(3)
    event_time: {column: pay_ts, max_out_of_orderness: 5s}
  customers:
    connection: db
    table: public.customers
    columns:
      customer_id: STRING NOT NULL
      tier: STRING
      updated_at: TIMESTAMP(3)
    primary_key: [customer_id]
    event_time: updated_at
"""


def project(yaml_text: str, files: dict | None = None, base: bool = True) -> str:
    """Write a temp project; `yaml_text` is appended to BASE unless base=False."""
    d = tempfile.mkdtemp(prefix="cascade-test-")
    body = (BASE if base else "") + textwrap.dedent(yaml_text)
    with open(os.path.join(d, "pipeline.yaml"), "w") as f:
        f.write(body)
    for name, content in (files or {}).items():
        path = os.path.join(d, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(textwrap.dedent(content))
    return d


def compile_text(yaml_text: str, files=None, base=True, env=None):
    return compile_pipeline(project(yaml_text, files, base), env)


def one_model(sql: str, sink_mode="upsert", pk="k", extra="", defaults="defaults: {state_ttl: 1d}\n",
              on_conflict=None):
    """Single model feeding a print sink."""
    sql_block = textwrap.indent(textwrap.dedent(sql).strip(), "      ")
    pk_line = f"    primary_key: [{pk}]\n" if sink_mode == "upsert" else ""
    if on_conflict:
        pk_line += f"    on_conflict: {on_conflict}\n"
    return compile_text(
        defaults + "models:\n  m:\n    sql: |\n" + sql_block + "\n" + extra
        + f"sinks:\n  out:\n    from: m\n    connection: print\n    mode: {sink_mode}\n" + pk_line)
