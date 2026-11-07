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
scaling:                          # parallelism and autoscaler bounds; see "Scaling" below
  max_parallelism: 720
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
functions: {...}                  # user-defined functions; see "Functions" below
deploy: {...}                     # Kubernetes deployment; see "Deployment" below
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
      idle_timeout: 30s         # optional; see "Idle sources" below
    processing_time: proc       # optional: adds `proc AS PROCTIME()`
    primary_key: [order_id]     # required for CDC / versioned tables
    options: {...}              # raw Flink WITH options, applied last
```

> In YAML flow style (`{a: DECIMAL(10, 2)}`) the comma splits the mapping; quote such types or
> use block style.

### Idle sources

An operator with several event-time inputs only moves event time forward as far as its slowest input.
A source that goes quiet, such as a CDC dimension table after its snapshot, would hold back event
time forever. `event_time.idle_timeout` lets that source stop counting after it has been quiet for
the given time. Set it **only on sources that legitimately go quiet**. Sources without one never
go idle.

Don't use Flink's job-wide `table.exec.source.idle-timeout` for this; it raises CD108. It marks
*every* source idle, including the main fact stream. When that stream stalls (a broker restart, a
slow restore after a TaskManager loss), another source's watermark moves event time past rows still
waiting in Kafka. Windows then drop those rows as late. The soak test reproduced this: 7 of 897
revenue windows were under-counted across 15 injected failures.

How it compiles:
- Kafka sources take a per-source `scan.watermark.idle-timeout`.
- Other connectors (CDC) can only use the job-wide setting, so the compiler sets it from their
  `idle_timeout`. They must all agree (CD109). Every Kafka source then gets an explicit
  per-source value (`0 ms` meaning never idle when it declares none), so the job-wide timeout
  doesn't reach it.
- Setting both the job-wide option and a per-source `idle_timeout` is an error (CD109).

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

**Delivery guarantees.** Kafka sinks are at-least-once by default: after a failure, rows written
since the last checkpoint are written again. Upsert sinks converge, but append topics get
duplicates. With `delivery: exactly-once`, the sink writes in Kafka transactions committed on
checkpoints, and consumers must read with `isolation.level=read_committed`. The compiler sets
`properties.transaction.timeout.ms` to 15 minutes, because Flink's default is higher than what
brokers accept. It also sets a stable `sink.transactional-id-prefix` per sink. Output becomes
visible once per checkpoint interval.

## Quarantine

```yaml
quarantine:
  connection: events
  topic: "{pipeline}.{model}.quarantine"   # templated per model
  format: json
```

---

## Functions

User-defined scalar functions are declared once and then called by name in any model's SQL.

```yaml
functions:
  mask_email:
    language: java
    class: com.acme.udf.MaskEmail     # a ScalarFunction
    jar: udfs/acme-udfs.jar           # optional: relative to the project; otherwise on the image classpath
    returns: STRING                   # optional for Java: lets the compiler type the column
  churn_risk:
    language: python
    file: udfs/scoring.py             # a plain module in the project
    function: churn_risk              # default: the key
    returns: DOUBLE                   # required for Python
    deterministic: true               # default
```

- **Java** lowers to `CREATE TEMPORARY SYSTEM FUNCTION ... LANGUAGE JAVA [USING JAR '...']`. A jar
  that doesn't exist yet is a warning (CD164); build it before `test`, `verify` or `deploy`.
- **Python** functions are plain Python: no PyFlink imports, so they can be unit-tested anywhere.
  The compiler writes `.cascade/python/` with a copy of each module and a generated shim,
  `cascade_udfs.py`, which wraps each function with PyFlink's `udf(...)` and the declared result
  type. Modules must be single files; two modules with the same name are an error (CD163). The
  compiler parses (never imports) each module to check that the function exists (CD161).
- **Typing.** Declared `returns` types flow into schema inference, so sinks fed by UDF columns
  don't need a contract. `cascade infer` gets exact types either way.
- **Local runs** (`test`, `verify`, `infer`, `plan`) add Flink's `opt/flink-python` jar to the
  classpath and run Python workers with `$CASCADE_PYTHON`, or the current interpreter if it has
  `apache-flink` installed. The PyFlink version should match the Flink distribution.
- **Built-in names.** A function that shadows a Flink built-in such as `upper` is a warning (CD162).

In Python: `mask = p.java_function("mask_email", "com.acme.udf.MaskEmail", jar=..., returns="STRING")`
and `churn = p.python_function(scoring.churn_risk, returns="DOUBLE")`. The handles render calls,
for example `churn("days_inactive", "orders_90d")`, for f-strings and builder expressions. See
[`examples/udfs`](../examples/udfs) and [`examples/udfs_py`](../examples/udfs_py).

## Scaling

```yaml
scaling:
  max_parallelism: 720      # pipeline.max-parallelism: number of key groups
  parallelism: 2            # initial parallelism (parallelism.default)
  min: 1                    # autoscaler bounds per operator
  max: 48
  target_utilization: 0.7   # busy-time fraction the autoscaler aims for
  max_lag: 1m               # SLO: source lag to stay below
  autoscale: true           # default
models:
  customer_ltv:
    scaling: {min: 2}       # per-model bounds, within the pipeline's
```

**Max parallelism** is fixed when a job first starts with keyed state, and it caps how far the
job can ever scale. Changing it later discards that state. So:
- A pipeline with keyed state and no `max_parallelism` gets a warning (CD520).
- Choose a value with many divisors, so that more parallelisms split the key groups evenly. With
  few even splits in `[min, max]`, you get a warning (CD522).
- `check-upgrade` reports a changed value as breaking (`MAX_PARALLELISM_CHANGED`). Pinning it for
  the first time is a warning showing the value the running job most likely derived.

The parallelism settings must come from `scaling`, not `flink.config` (CD521), so the autoscaler
and the upgrade checks see them. The manifest's `scaling` section lists them, the per-model
bounds, and `aligned_parallelisms` (the parallelisms that split key groups evenly).

### Which operators belong to which model

Flink inlines views, so a running job has no notion of "model". `cascade plan` therefore also
writes `operators.json`. It holds the JobGraph built exactly the way `EXECUTE PLAN` builds it:
- **Vertices** with the IDs used by Flink metrics and the REST API.
- **Operators** of each vertex, each attributed to a source, model or sink.
- **Per-model lists** of exec nodes, operators and vertices.

The attribution works in three steps:
1. Scans and sinks name their tables.
2. Each model's stateful operators (aggregates, joins, dedup, ...) are matched to plan nodes of a
   compatible type that read the same sources and feed the same sinks.
3. Stateless operators (Calc, Exchange, local pre-aggregation) go to the stateless models fused
   into them, or otherwise to the neighbouring model.

Because of operator chaining, one vertex can run several models, and `vertices[].models` lists
them. Models that have no operator of their own are listed under `fused_away`.

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
| `breaking` | sink contract broken (removed/retyped columns, changed key or mode), or max parallelism changed | 1 |

State identities don't depend on names, so renaming a model or one of its inputs is reported as
`RENAMED` and keeps its state. Changing the TTL duration keeps state. Turning TTL on or off is
`STATE_TTL_TOGGLED`. Changing options that re-plan stateful operators is `PLANNER_CONFIG_CHANGED`:
mini-batch, aggregation phase strategy, distinct-aggregate split, upsert materialization, and so on.

[`tests/test_upgrade_matrix.py`](../tests/test_upgrade_matrix.py) runs 15 kinds of upgrade through
`check-upgrade`, the plan pin report, and real strict and lossy savepoint restores. It requires
all of them to agree.

### Adding a stateful model to a running pipeline: `backfill`

A new stateful model starts empty, but its sources resume from the savepoint. So it only sees
input that arrives after the upgrade. Its counts start at zero, and a join has no rows to match.
The planner can also give the new model its own copy of an *existing* operator, such as a temporal
join, which starts empty too. `cascade plan --previous` names every such operator, the restored
sources it reads, and the model to fix.

```yaml
models:
  orders_per_customer:
    backfill: true
    sql: SELECT customer_id, COUNT(*) AS orders FROM clean_orders GROUP BY customer_id
```

With `backfill: true`, the model and everything upstream of it are compiled over dedicated copies
of their sources (`<source>__bf_<model>`), read from the beginning:
- **Kafka** from the earliest offset, with its own consumer group.
- **CDC** with a fresh snapshot and its own replication slot.
- **JDBC and datagen** sources aren't copied. They're lookup tables or generators, with nothing to
  replay.

Upstream models are recomputed with their expectations; quarantine becomes drop, so rows aren't
quarantined twice. The rest of the job keeps its state, and the new branch catches up from history
to live (CD370 lists what is copied).

- **Keep the flag.** The copies stay part of the job. Removing `backfill` later is a
  state-incompatible change.
- **History is limited** to what the sources still hold, such as Kafka retention.
- **A CDC snapshot holds only each row's current version.** So a backfilled *event-time* temporal
  join finds no version for events older than a row's last change.
- **Idle timeouts make temporal-join backfills timing-dependent (CD372).** With an idle timeout on
  the CDC source, the replayed fact history (available at once) can be joined
  before the CDC snapshot arrives, and then finds no version. The integration runs saw different
  splits under compose and kind. Rows arriving after the snapshot are joined correctly.

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
- **Settings.** `Pipeline(..., scaling={...}, deploy={...})` and `@p.model(scaling={...})` take the
  same keys as YAML. For functions, see "Functions".
- **Errors.** Diagnostics point at `pipeline.py` lines. An exception while building a model is
  reported as CD304 rather than a traceback.
- **In-process use.** `p.compile()` and `p.validate()` work in a notebook.

## Tests

Test files are `tests/*.yaml` in the project. They work for YAML and Python projects alike.

```yaml
tests:
  - name: ltv_sums_amounts_per_customer
    target: customer_ltv            # a model, <model>.quarantined, or a sink
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
    watermarks: per_row             # per_row (default) | manual | end_of_input
```

`cascade test PATH [-k substring]` runs each test in a fresh local Flink (embedded MiniCluster).
Sources are replaced by the tools jar's `cascade-fixture` connector, which replays the given rows
in order:
- **Watermarks are deterministic.** With `watermarks: per_row` (the default), each event-time
  source emits its declared strategy (`max event time - max_out_of_orderness`) after every row.
  Rows older than that are late, exactly as in production. A `{_watermark: "2026-01-01 10:05:00"}`
  row emits a watermark explicitly. With `watermarks: manual`, explicit ones are the only ones.
  `end_of_input` emits none until the end. At the end of input, event time jumps to +∞ and every
  window fires. Interleaving *between* sources isn't controlled, so cross-source lateness, for
  example in an interval join, can vary.
- **Sinks as targets.** `target: <sink>` runs the real `INSERT`, including the sink's upsert
  materialization and `ON CONFLICT` policy, into a capture table. For upsert sinks, the comparison
  is with the final table by primary key, which is what the external system would hold. For
  append sinks, it's every row received.
- **Lookup joins.** A source used in `FOR SYSTEM_TIME AS OF proc_time` (e.g. a JDBC table) is
  served from its fixture rows.
- **Updating models.** These are compared by their final state: the produced changelog
  (`+I/-U/+U/-D`) is applied first.
- **Values.** They're compared by Flink column type: decimals numerically, timestamps parsed, and
  so on. Timestamps without an offset are UTC.
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
  - New stateful operators start empty, but their sources resume from saved positions, so they
    only see post-upgrade input. The plan report names those whose results depend on earlier
    input and says why: a new TemporalJoin, for example, has an empty versioned table and misses
    every key that hasn't changed since the upgrade. The planner can create such operators even
    for unchanged models. Adding a model that reads a joined model can duplicate the join for the
    new branch.
- `cascade restore-test OLD NEW [--no-pin] [--seconds N]` runs OLD locally with stand-in
  connectors, stops it with a savepoint, and starts NEW from that savepoint with
  `ignore-unclaimed-state=false`. It reports whether every piece of state mapped, and which
  operators start empty.

## Deployment

```yaml
deploy:
  image: ghcr.io/acme/cascade-orders:${var:image_tag}   # vars resolve per environment
  namespace: data-pipelines
  service_account: flink                                # default
  flink_version: 2.3.0                                  # default; base image and operator version
  jobmanager: {memory: 2g, cpu: 1}
  taskmanager: {memory: 4g, cpu: 2, slots: 2}
  checkpoints: {interval: 30s, dir: s3://state/checkpoints, mode: exactly_once}
  savepoints: s3://state/savepoints
  upgrade_mode: savepoint                               # savepoint | last-state | stateless
  secrets: orders-secrets                               # Kubernetes Secret name (default <name>-secrets)
  artifacts: {org.apache.flink:flink-sql-connector-kafka: <version>}   # connector versions for the image
  jars: [libs/extra.jar]                                # more jars for /opt/flink/usrlib
  flink_config: {}                                      # extra flinkConfiguration entries
  pod_template:                                         # merged into the generated pod template
    spec:
      nodeSelector: {pool: flink}
      volumes: [...]                                    # lists append; containers merge by name
  labels: {team: data-eng}
```

`cascade deploy PATH --env prod -o deploy/ [--plan-dir out/] [--image IMG]` writes resources for
the Flink Kubernetes Operator:

| File | |
|---|---|
| `flinkdeployment.yaml` | `FlinkDeployment` that runs `dev.cascade.runner.SqlRunner` from the runtime jar. It carries the resources and checkpointing, and maps `scaling` to the operator autoscaler (`job.autoscaler.*`). The `max_lag` SLO and per-model bounds go in annotations for the platform's own autoscaler. |
| `configmap.yaml` | The job: `setup.sql` plus `plan.json` with `--plan-dir`, whose operator UIDs are pinned and survive upgrades; otherwise `statements.sql`, planned at startup |
| `python-configmap.yaml` | The Python UDF modules and shim, when there are any |
| `secret.template.yaml` | The Secret the job expects: keys listed, values empty, never filled in by cascade |
| `kustomization.yaml` | `kubectl apply -k`, or for GitOps |
| `Dockerfile` | Runtime image: `flink:<version>`, the connector jars the manifest lists (one `ARG` per version), `cascade-runtime.jar`, UDF jars, and PyFlink when needed |
| `operators.json` | Copied from `--plan-dir`: vertex → model mapping |

**Upgrades through the operator.** The job ConfigMap is named after its content and referenced
from the pod template. So `kubectl apply -k` of a new version changes the FlinkDeployment spec,
and the operator does a savepoint upgrade (`upgrade_mode: savepoint`). A running job never sees
the next version's files. Old ConfigMaps stay behind until you delete them.

**Secrets.** A secret `${secret:kafka/prod/bootstrap}` stays a placeholder everywhere on disk. The
pod gets the env var `CASCADE_SECRET_KAFKA_PROD_BOOTSTRAP` from key `kafka.prod.bootstrap` of the
Secret. At startup the runner substitutes the value into the SQL, escaped for a string literal,
and fails listing every missing secret. Secrets aren't allowed in `deploy:` settings (CD530),
since only the job's SQL is substituted. `SqlRunner ... --validate` plans the job without
submitting it.

## Schema import

`cascade import-schema --registry URL --subject SUBJECT [--version N] [--source NAME]` (or
`--avsc FILE`) prints a `columns:` block converted from Avro: logical types, nullable unions,
records → `ROW`, arrays, maps, enums. Basic auth comes from `CASCADE_REGISTRY_USER_INFO=user:pass`.
It's a reviewable, explicit step, so compilation never depends on the registry.

## Diagnostic codes

| Range | Area |
|---|---|
| CD0xx | YAML structure, unknown keys (with suggestions), vars, types |
| CD1xx | names, sources, event time, primary keys (CD108 job-wide idle timeout, CD109 conflicting idle timeouts); functions (CD160 missing module, CD161 function not defined, CD162 shadows a built-in, CD163 module name clash, CD164 jar not built) |
| CD2xx | connectors, properties, formats, secrets (CD215: a JDBC sink restarts the TaskManager during a database outage) |
| CD30x–CD32x | model SQL, references, cycles, Python model build errors (CD304), contracts, expectations |
| CD33x–CD34x | windows and time attributes, temporal joins |
| CD35x | state / TTL |
| CD36x | schema lock (CD360 used, CD361 stale, CD362 Flink could not resolve a model) |
| CD4xx | sinks: changelog compatibility, keys, types, ON CONFLICT (CD412: DO ERROR / DO NOTHING need event time on every scanned source) |
| CD50x | pipeline-level (unused nodes, no sinks) |
| CD52x | scaling (CD520 no max parallelism with keyed state, CD521 invalid bounds, CD522 uneven key groups) |
| CD53x | deployment (CD530 secret in deploy settings) |

`cascade validate --json` emits machine-readable diagnostics for editors and CI.
