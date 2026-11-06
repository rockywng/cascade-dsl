# cascade-dsl language reference

A pipeline is one directory with a `pipeline.yaml` and, optionally, SQL files. It has
four kinds of nodes that share one namespace:

| Node | What it is | Lowers to |
|---|---|---|
| **connection** | Credentials and endpoint for an external system | (inlined into tables) |
| **source** | A typed table read from a connection | `CREATE TEMPORARY TABLE` with watermark / PK |
| **model** | A pure SQL `SELECT` over sources and other models | `CREATE TEMPORARY VIEW` (+ expectation views) |
| **sink** | Writes one model or source to a connection | `CREATE TEMPORARY TABLE` + `INSERT INTO` |

All sinks are compiled into **one Flink job** (`EXECUTE STATEMENT SET`), so shared upstream
work runs once.

See [`examples/orders/pipeline.yaml`](../examples/orders/pipeline.yaml) for a complete example.

---

## Top level

```yaml
pipeline: orders_analytics        # required, snake_case; becomes pipeline.name
description: ...
owner: data-eng
defaults:
  state_ttl: 30d                  # table.exec.state.ttl; see "State" below
flink:
  config:                         # passthrough SET statements
    table.exec.mini-batch.enabled: "true"
vars:                             # ${var:name} substitution in connections/sources/sinks
  warehouse: s3://dev-lake
environments:                     # cascade compile --env prod
  prod:
    vars: {warehouse: s3://prod-lake}
connections: {...}
sources: {...}
models: {...}
sinks: {...}
quarantine: {...}
```

Durations: `500ms`, `5s`, `10m`, `1h`, `7d`.

### Variables and secrets

- `${var:name}` is resolved at compile time from `vars`, then overridden by `environments.<env>.vars`.
- `${secret:path}` is **never resolved by the compiler**. It is passed through to the generated SQL as
  a placeholder and listed in `manifest.json → secrets`; the platform agent injects it at deploy
  time. Properties marked as secrets (`password`, `sasl_jaas_config`) must use a `${secret:...}`
  reference or compilation fails (CD201).

## Connections

```yaml
connections:
  events:
    type: kafka
    properties:
      bootstrap_servers: ${var:kafka_bootstrap}
```

`cascade connectors` lists every connector, its properties (required ones marked `*`), formats and sink
modes. Zero-config connectors (`datagen`, `print`, `blackhole`) can be used directly as
`connection: print` without declaring them.

| type | source | sink modes | notes |
|---|---|---|---|
| `kafka` | ✓ (changelog with CDC formats) | append, upsert | upsert sinks lower to `upsert-kafka` |
| `postgres-cdc`, `mysql-cdc` | ✓ updating | — | requires `primary_key` |
| `iceberg` | ✓ (streaming read) | append, upsert | `table: db.table` |
| `jdbc` | ✓ (lookup joins) | append, upsert | |
| `filesystem` | ✓ | append | `path`, many formats |
| `datagen` / `print` / `blackhole` | dev | dev | |

## Sources

```yaml
sources:
  orders:
    connection: events
    topic: orders.v1            # connector-specific keys (see `cascade connectors`)
    format: json
    startup: earliest
    columns:                    # required; Flink SQL types
      order_id: STRING NOT NULL
      amount: DECIMAL(12, 2)
      order_ts: TIMESTAMP(3)
    event_time:                 # or just `event_time: order_ts`
      column: order_ts
      max_out_of_orderness: 5s  # -> WATERMARK FOR order_ts AS order_ts - INTERVAL '5' SECOND
    processing_time: proc       # optional: adds `proc AS PROCTIME()`
    primary_key: [order_id]     # required for CDC / versioned tables
    options: {...}              # raw Flink WITH options, applied last
```

> In YAML flow style (`{a: DECIMAL(10, 2)}`) the comma splits the mapping; quote such types or
> use block style.

## Models

```yaml
models:
  customer_ltv:
    description: Running lifetime value per customer
    sql: |                       # or sql_file: models/customer_ltv.sql
      SELECT customer_id, SUM(amount) AS lifetime_value
      FROM clean_orders
      GROUP BY customer_id
    columns:                     # optional output contract
      customer_id: STRING
      lifetime_value: DECIMAL(38, 2)
    expectations:
      - name: positive_ltv
        check: lifetime_value >= 0
        on_violation: fail        # fail | drop | quarantine
    allow_unbounded_state: false
```

- A model is exactly one `SELECT` (CTEs allowed). Reference other sources and models by bare name.
- **Columns contract.** When given, the names must match the query output in order (CD310).
  Expressions whose type the compiler can't infer, or that differ from the contract, are wrapped in
  `CAST` in a generated `<model>__typed` view. Time attributes are never cast (CD311).
- **Expectations** compile into the model view. A NULL check result counts as a violation.
  - `drop`: the row is filtered out through `CASCADE_EXPECT(...)`, which also counts it.
  - `quarantine`: filtered out *and* written, with `_violated_expectation` and `_quarantined_at`,
    to the pipeline's `quarantine:` destination (append-only models only). The rows are also
    available as the view `<model>__quarantined`, which tests can target as `<model>.quarantined`.
  - `fail`: `CASCADE_ASSERT(...)` throws `ExpectationViolation` and fails the task.

  Both UDFs live in `cascade-runtime.jar`, which ships with every job. They export the counters
  `cascade_expectation_checked` and `cascade_expectation_violations`, labeled `cascade_model`,
  `expectation` and `action`.

## Sinks

```yaml
sinks:
  ltv_to_kafka:
    from: customer_ltv
    connection: events
    topic: customer_ltv.v1
    format: json
    mode: upsert                 # append (default) | upsert
    primary_key: [customer_id]   # required for upsert
    on_conflict: error           # upsert only: error | nothing | deduplicate
```

`on_conflict` lowers to Flink's `INSERT ... ON CONFLICT DO ...` (Flink ≥ 2.3). Flink requires it
when it can't prove that the query's upsert key equals the sink's primary key. The compiler proves
this statically for GROUP BY keys and pass-through CDC keys and warns otherwise (CD410). `cascade
verify` gives the planner's answer.

## Quarantine

```yaml
quarantine:
  connection: events
  topic: "{pipeline}.{model}.quarantine"   # templated per model
  format: json
```

---

## What the compiler checks

**Changelog semantics.** Every relation is classified as `append` or `updating`. Updating comes
from CDC sources and formats, non-windowed GROUP BY, outer/regular joins, keep-last dedup and global
aggregates. Temporal joins don't propagate the versioned side's updates. Sending an updating
relation to an `append` sink is an error (CD407) that explains which operator causes it.

**Time attributes.** Event-time columns are tracked through models. They survive projections,
interval joins and the probe side of temporal joins. They're lost through regular joins and
non-windowed aggregates. A window TVF over a relation without one is an error (CD330/CD331).

**State.** Each model's stateful operators are classified as bounded (windows, interval/temporal
joins) or unbounded (GROUP BY, regular joins, dedup, DISTINCT, unbounded OVER). Unbounded state
requires `defaults.state_ttl`, a `/*+ STATE_TTL(...) */` hint, or an explicit
`allow_unbounded_state: true` (CD350).

**Types.** Sink DDL needs concrete types, so the compiler infers output types through column
references, `*`, CTEs, subqueries, CAST, aggregates and window functions. When it can't, it asks for
a contract (CD408) instead of guessing.

## Upgrades

`manifest.json` gives every stateful node (sources and stateful models) a `state_id`, derived from
its normalized SQL and its input schemas. Comments, whitespace and keyword case don't affect it.
`cascade check-upgrade old/manifest.json .` classifies a change as:

| verdict | meaning | exit code |
|---|---|---|
| `ok` | savepoint restores fully | 0 |
| `warning` | restores; e.g. upstream logic changed under a stateful model (`SEMANTIC_DRIFT`), new stateful nodes, added sink columns | 0 |
| `state_loss` | some operator state is discarded (`STATE_INCOMPATIBLE`, `STATE_DROPPED`); needs `--allow-state-loss` | 3 |
| `breaking` | sink contract broken (removed/retyped columns, changed key or mode) | 1 |

## Python API

`pipeline.py` defines a `Pipeline`. It builds the same document as `pipeline.yaml`, so every check,
the generated SQL and the manifest are identical. A directory with both files uses the YAML.

```python
from cascade_dsl.api import Pipeline, expect, kafka, secret, sum_, count

pipeline = p = Pipeline("shop", owner="data-eng", state_ttl="30d",
                        vars={"bootstrap": "localhost:9092"},
                        environments={"prod": {"bootstrap": secret("kafka/prod")}})

bus = p.connection("bus", kafka(bootstrap_servers=p.var("bootstrap")))
orders = p.source("orders", bus, topic="orders", format="json",
                  columns={"order_id": "STRING NOT NULL", "amount": "DECIMAL(10, 2)",
                           "ts": "TIMESTAMP(3)"},
                  event_time=("ts", "5s"))

@p.model(expectations=[expect.drop("positive", "amount > 0")])
def paid(orders):                       # parameters name the inputs
    return f"SELECT * FROM {orders} WHERE amount IS NOT NULL"

@p.model
def revenue(paid):                      # or return a builder query
    return paid.tumble("ts", "1m").group_by().agg(revenue=sum_("amount"), orders=count())

p.sink("revenue_out", revenue, bus, topic="revenue")
```

- **Declarations.** `connection(name, kafka(...) | postgres_cdc(...) | iceberg(...) | jdbc(...) |
  filesystem(...) | connector(type, ...))`, `source(...)`, `model` (decorator), `sql_model(name,
  sql | sql_file=...)`, `sink(...)` and `quarantine(...)` take the same options as the YAML keys.
- **Builder** (`cascade_dsl.builder`). It has `where`, `select`, `with_columns`, `group_by(...).agg(...)`,
  `tumble` / `hop` / `cumulate`, `join(other, on, how, within=(left_time, right_time, upper[, lower]))`
  for regular or interval joins, `temporal_join(versioned, on, as_of)` and
  `dedup(keys, order_by, keep="first"|"last")`. Helpers: `sum_`, `count`, `count_distinct`, `min_`,
  `max_`, `avg`, `lit`, `interval`. Generated aliases are backtick-quoted, because names like
  `views` are reserved words. Subquery aliases are deterministic (`q1`, `q2`, ...), so rebuilding
  never changes a model's state identity.
- **Errors.** Diagnostics point at `pipeline.py` lines. An exception while building a model is
  reported as CD304 rather than a traceback.
- **In-process use.** `p.compile()` and `p.validate()` work in a notebook.

## Tests

Test files are `tests/*.yaml` in the project. They work for YAML and Python projects alike.

```yaml
tests:
  - name: ltv_sums_amounts_per_customer
    target: customer_ltv            # a model, or <model>.quarantined
    given:                          # rows per source; unlisted sources are empty
      orders:
        - {order_id: o1, customer_id: c1, amount: 10.00, order_ts: "2026-01-01 10:00:00"}
      customers:                    # changelog sources: _op insert | update | delete
        - {_op: update, customer_id: c1, segment: enterprise}
    expect:
      rows:                         # only the listed columns are compared
        - {customer_id: c1, lifetime_value: 10.00}
      # empty: true                 # no rows
      # error: positive_amount      # the job must fail, mentioning this text
    ordered: false                  # default: multiset comparison
```

`cascade test PATH [-k substring]` runs each test in a fresh local Flink (embedded MiniCluster)
with the sources replaced by bounded files:
- **Event time.** It advances to the end of input, so every window fires.
- **Updating models.** These are compared by their final state: the produced changelog
  (`+I/-U/+U/-D`) is applied first.
- **Values.** They're compared by Flink column type: decimals numerically, timestamps parsed, and
  so on.
- **Failures.** A failing test prints the missing and unexpected rows and points at the test's
  file and line.

## Exact schemas: `cascade infer`

`cascade infer PATH` asks Flink's validator for every model's exact output schema and writes
`cascade.lock.json`; commit it. While a model's fingerprint (its SQL, contract and input schemas)
matches the lock, the compiler uses those types (CD360). That resolves CD408 for expressions the
static analysis can't type. When the model or an input changes, the entry is ignored and the
compiler warns (CD361). Compilation itself never needs a JVM.

## Compiled plans and restore tests

- `cascade plan PATH -o out [--previous old/plan.json]` writes `plan.json` (Flink's compiled plan)
  and `plan-setup.sql`. The setup script holds the tables, functions and settings the plan
  references by name; secrets stay in it rather than in the plan. Deploy by running
  `plan-setup.sql`, then `EXECUTE PLAN 'plan.json'`.
  - With `--previous`, nodes are matched against the running version's plan. A node matches when
    its definition and everything upstream of it are identical. A stateful node whose own
    definition is unchanged also matches when only its upstream changed and it feeds the same
    sinks; this is reported. Matched nodes keep their old operator UIDs through a per-node
    `table.exec.uid.format`. Changed and new nodes get fresh, collision-free UIDs.
  - Production connectors must be on the tools classpath. `--local-connectors` uses stand-ins.
- `cascade restore-test OLD NEW [--no-pin] [--seconds N]` runs OLD locally with stand-in
  connectors, stops it with a savepoint, and starts NEW from that savepoint with
  `ignore-unclaimed-state=false`. It reports whether every piece of state mapped, and which
  operators start empty.

## Schema import

`cascade import-schema --registry URL --subject SUBJECT [--version N] [--source NAME]` (or
`--avsc FILE`) prints a `columns:` block converted from Avro: logical types, nullable unions,
records → `ROW`, arrays, maps, enums. Basic auth comes from `CASCADE_REGISTRY_USER_INFO=user:pass`.
It's a reviewable, explicit step, so compilation never depends on the registry.

## Diagnostic codes

| Range | Area |
|---|---|
| CD0xx | YAML structure, unknown keys (with suggestions), vars, types |
| CD1xx | names, sources, event time, primary keys |
| CD2xx | connectors, properties, formats, secrets |
| CD30x–CD32x | model SQL, references, cycles, Python model build errors (CD304), contracts, expectations |
| CD33x–CD34x | windows and time attributes, temporal joins |
| CD35x | state / TTL |
| CD36x | schema lock (CD360 used, CD361 stale, CD362 Flink could not resolve a model) |
| CD4xx | sinks: changelog compatibility, keys, types, ON CONFLICT |
| CD5xx | pipeline-level (unused nodes, no sinks) |

`cascade validate --json` emits machine-readable diagnostics for editors and CI.
