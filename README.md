# cascade-dsl

[![ci](https://github.com/rockywng/cascade-dsl/actions/workflows/ci.yml/badge.svg)](https://github.com/rockywng/cascade-dsl/actions/workflows/ci.yml)

A declarative pipeline DSL for Flink, in YAML + SQL or in Python. The `cascade` compiler checks
the semantics that usually only surface in production: changelog modes, time attributes, unbounded
state, upsert-key mismatches. It then compiles the pipeline to a single Flink job. Around the
compiler is the tooling to ship that job safely: tests on fixture data, exact schemas from Flink,
compiled plans with stable operator UIDs, an actual savepoint-restore check before you deploy, and
Kubernetes resources for the Flink operator. Java and plain-Python UDFs are supported.

```
pipeline.yaml / pipeline.py ─► load ─► analyze ─► lower ─► pipeline.sql + manifest.json
                                  (same IR for both front ends)
cascade validate        static checks, line-accurate diagnostics (no JVM)
cascade verify          plan with the real Flink planner, cross-check the static analysis
cascade test            run tests/*.yaml on fixture rows in a local Flink
cascade infer           exact model schemas from Flink → cascade.lock.json
cascade plan            compiled plan for EXECUTE PLAN, UIDs pinned to the previous version,
                        plus operators.json (Flink vertex IDs -> models, for metrics/autoscaling)
cascade restore-test    run old version, savepoint, restore new version from it
cascade check-upgrade   logical state-compatibility diff between versions
cascade deploy          FlinkDeployment + job ConfigMap + Secret template + runtime Dockerfile
cascade import-schema   source columns from a schema registry subject / .avsc
```

Language reference: [docs/DSL.md](docs/DSL.md).

## Quick start

Requires Python ≥ 3.10 and PyYAML. `pip install -e .` installs the `cascade` command. The
examples use `python3 -m cascade_dsl`, which works without installing. Commands that run Flink
need a Flink 2.3+ distribution and the JVM helpers:

```bash
export FLINK_HOME=~/code/flink/build-target
```

```bash
runtime/build.sh
```

```bash
python3 -m cascade_dsl validate examples/orders
```

```bash
python3 -m cascade_dsl test examples/orders
```

```bash
python3 -m cascade_dsl verify examples/clickstream_py
```

```bash
python3 -m cascade_dsl compile examples/orders --env prod -o target
```

## Two front ends, one compiler

YAML + SQL ([examples/orders](examples/orders/pipeline.yaml)):

```yaml
models:
  customer_ltv:
    sql: |
      SELECT customer_id, SUM(amount) AS lifetime_value, COUNT(*) AS order_count
      FROM clean_orders
      GROUP BY customer_id
```

Python ([examples/clickstream_py](examples/clickstream_py/pipeline.py)). Model functions'
parameters name their inputs, and they return SQL or a builder query:

```python
@p.model(expectations=[expect.drop("has_user", "user_id IS NOT NULL")])
def unique_views(page_views):
    return page_views.dedup("view_id", order_by="view_ts", keep="first")

@p.model
def attributed_purchases(unique_views, purchases):
    return (unique_views
            .join(purchases, on=["user_id"], within=("view_ts", "purchase_ts", "30m"))
            .select("unique_views.url", "purchases.amount"))
```

Both produce the same document and IR. [examples/orders_py](examples/orders_py/pipeline.py)
compiles to a manifest identical to the YAML version, and the test suite checks this.

## Tests

```yaml
# examples/orders/tests/orders_test.yaml
- name: enrichment_uses_customer_segment_as_of_order_time
  target: orders_enriched
  given:
    customers:
      - {customer_id: c1, name: Acme, segment: smb, updated_at: "2026-01-01 09:00:00"}
      - {_op: update, customer_id: c1, segment: enterprise, updated_at: "2026-01-01 11:00:00"}
    orders:
      - {order_id: o1, customer_id: c1, amount: 10.00, currency: usd, order_ts: "2026-01-01 10:00:00"}
      - {order_id: o2, customer_id: c1, amount: 20.00, currency: usd, order_ts: "2026-01-01 12:00:00"}
  expect:
    rows:
      - {order_id: o1, segment: smb}
      - {order_id: o2, segment: enterprise}
```

Each test runs the target model in a local Flink on fixture data (CDC sources accept
`_op: insert|update|delete`). The resulting changelog is applied to get the final table, which is
compared by column type. A test can also expect the job to fail, e.g. on a `fail` expectation.
Each test takes about 1 second after JVM start-up.

- **Late data.** Watermarks are deterministic. Each source emits its declared strategy after every
  row, or you place `{_watermark: ...}` rows yourself, so late data can be tested
  ([example](examples/clickstream_py/tests/clickstream_test.yaml)).
- **Sinks.** A test can target a sink. That runs the real INSERT, including upsert materialization
  and `ON CONFLICT`, and compares against the final keyed table.
- **Lookup joins.** Lookup tables are served from fixture rows.

## Shipping changes safely

Flink SQL derives operator UIDs from a counter over the plan, so adding one model renumbers every
operator after it, and the savepoint no longer maps. `cascade plan --previous old/plan.json`
matches the new plan against the old one structurally and pins unchanged operators to their old
UIDs. Operators whose upstream changed but whose own definition didn't keep their state too, and
are reported.

`cascade restore-test old/ new/` proves it on a real job. It runs the old version locally, takes a
savepoint, and restores the new version in strict mode. Adding an aggregate to the orders example:

```
without pinning:  restore FAILED  Cannot map checkpoint/savepoint state for operator … to the new program
with pinning:     operators keeping their UIDs: 25 identical
                  new stateful operator (starts empty): GlobalGroupAggregate #9
                  restore: OK (all savepoint state was mapped to the new job)
```

A deployment runs `plan-setup.sql` (tables and functions; secrets stay out of the plan), then
`EXECUTE PLAN 'plan.json'`.

## Running it on Kubernetes

```bash
python3 -m cascade_dsl plan examples/orders --env prod -o target/plan --previous old/plan.json
```

```bash
python3 -m cascade_dsl deploy examples/orders --env prod --plan-dir target/plan -o deploy/orders
```

This writes a `FlinkDeployment` for the Flink Kubernetes Operator. It runs the pinned plan
through a small runner in the runtime jar, which fills in `${secret:...}` values from a
Kubernetes Secret at startup, so no secret is written to disk. The `scaling:` block sets
`pipeline.max-parallelism` and the operator autoscaler's bounds. `operators.json` maps every
Flink vertex ID to the models it runs, so metrics and per-model autoscaling can speak in models
instead of `Calc[7]`. Stateful jobs get Kubernetes HA, so a JobManager crash resumes the same job
from its latest checkpoint; the kind run kills the JobManager to check it.

## User-defined functions

```yaml
functions:
  mask_email: {language: java, class: com.acme.udf.MaskEmail, jar: udfs/acme-udfs.jar, returns: STRING}
  churn_risk: {language: python, file: udfs/scoring.py, returns: DOUBLE}
```

Python UDFs are plain functions with no PyFlink imports. The compiler generates the PyFlink
wrapper, and the declared return types feed schema inference. See [examples/udfs](examples/udfs)
and [examples/udfs_py](examples/udfs_py).

`kind: aggregate | table | process_table` adds aggregate functions, table functions (`LATERAL
TABLE`) and Flink 2.1's process table functions, which keep keyed state of their own. In Python,
an aggregate function is a plain class and a table function a plain generator. See
[examples/functions](examples/functions) and [examples/functions_py](examples/functions_py).

## Model inference and replication

- **`ml_models:`** declares models for `ML_PREDICT` (OpenAI-compatible endpoints built in). Tests
  pin the model's answers, so they never call it. See [examples/ml_predict](examples/ml_predict).
- **`replicate:`** copies whole databases (Postgres or MySQL, by table pattern) into Paimon or Kafka.
  It creates the sink tables and applies schema changes as they happen, by compiling to a Flink CDC
  pipeline.

## Data quality you can see

Expectations compile into the job. `drop` and `quarantine` go through `CASCADE_EXPECT` and `fail`
through `CASCADE_ASSERT`. Both UDFs report
`cascade_expectation_checked` / `cascade_expectation_violations` counters labeled with model,
expectation and action, so dropped rows show up on dashboards. Quarantined rows also go to a sink,
and tests can target them as `<model>.quarantined`.

## Integration environment

[integration/](integration/) runs a pipeline against real Kafka, Postgres CDC and JDBC on Flink
2.3.0. It runs once as a docker-compose session cluster and once through the Flink Kubernetes
Operator on kind, using exactly what `cascade plan` and `cascade deploy` produce. Each run
upgrades v1 → v2 → v3 through savepoints, including a rescale, a TTL change and backfilled
models. It checks keyed state, Kafka offsets and the CDC position from outside the job. Its README
lists the bugs it found.

**Adding stateful models to a running job.** A new stateful model starts empty while its sources
resume from the savepoint, so it would only ever see new data. `cascade plan` flags this, and
`backfill: true` fixes it by giving the model its own copies of its sources, read from the
beginning.

## Tests of the compiler itself

```bash
PYTHONPATH=.:tests python3 -m unittest discover -s tests
```

There are 118 tests. 100 are pure Python and run in about a second. With `FLINK_HOME` set and the
jars built, 18 more run against real Flink:
- changelog inference for 13 SQL patterns, checked against the planner
- the example test suites
- `infer`
- plan pinning, and the restore test with and without pinning
- the upgrade matrix: 15 kinds of upgrade, where `check-upgrade`, the plan report and real
  savepoint restores must agree
- `backfill: true` models on fixture data
- operator mapping
- lookup joins, sink targets and `ON CONFLICT`
- the deployment runner
- Java and Python UDFs (these need PyFlink: `pip install apache-flink`, or set `CASCADE_PYTHON`)

CI runs everything against Flink 2.3.0.

## Layout

| Path | |
|---|---|
| `cascade_dsl/loader.py` | YAML → document → IR, structure validation, vars/env/secrets |
| `cascade_dsl/api.py`, `builder.py` | Python front end and query builder (emit the same document) |
| `cascade_dsl/sqlscan.py` | SQL tokenizer and static analysis (refs, stateful ops, inference) |
| `cascade_dsl/analyze.py` | semantic checks, relation facts, schema lock |
| `cascade_dsl/connectors.py` | declarative connector catalog |
| `cascade_dsl/lower.py` | structured statements → Flink SQL + manifest |
| `cascade_dsl/localize.py` | swap connectors for local stand-ins / fixtures |
| `cascade_dsl/verify.py` | Flink planner cross-check |
| `cascade_dsl/testing.py` | test discovery, execution, changelog-aware comparison |
| `cascade_dsl/schemas.py` | `cascade infer` / cascade.lock.json |
| `cascade_dsl/plans.py` | compiled plans, UID pinning, restore tests |
| `cascade_dsl/operators.py` | Flink vertices/operators → sources, models, sinks |
| `cascade_dsl/deploy.py` | FlinkDeployment, ConfigMaps, Secret template, Dockerfile |
| `cascade_dsl/udfs.py` | Java/Python UDFs, PyFlink shim generation |
| `cascade_dsl/registry.py` | schema registry / Avro import |
| `cascade_dsl/upgrade.py` | logical upgrade diff |
| `cascade_dsl/jvm.py` | driver for the tools jar |
| `runtime/src` | shipped with jobs: expectation UDFs and metrics, `SqlRunner` (deployed entry point) |
| `runtime/tools` | local tooling jar (describe, run, compile-plan, restore-test, job-graph, `cascade-fixture` connector) |

## Known limits

- **Static analysis is still heuristic.** `verify`, `infer` and `test` use real Flink and are the
  authority. Run them in CI.
- **`plan` with production connectors needs their jars on the tools classpath** (e.g. run it
  inside the runtime image). `--local-connectors` and `restore-test` use stand-ins, which exercise
  operator state but not connector state such as Kafka offsets.
- **UID pinning matches the operators Flink generates.** If a Flink upgrade changes how a query is
  planned, operators won't match, and the restore test reports that before deploy.
- **Schema import handles Avro only** (no JSON Schema or Protobuf).
- **Not every connector runs end to end.** [integration/](integration/) runs a pipeline against real
  Kafka, Postgres CDC and JDBC on Flink 2.3, under docker compose and under the Flink Kubernetes
  Operator on kind (savepoint upgrades, a JobManager kill). `integration/connectors.py` covers:
  - the other CDC sources, the lakes, Elasticsearch, DynamoDB and Kinesis
  - Kafka over mutual TLS
  - replication

  Db2 CDC and schema-registry formats are only planned against their jars.
- **Operator attribution is structural.** It's exact for sources, sinks and stateful operators.
  Stateless operators fused across models are attributed by their position in the graph.
