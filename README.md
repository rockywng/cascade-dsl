# pipedsl

A declarative pipeline DSL for Flink. You write YAML plus plain SQL `SELECT`s; `pipec` checks
the semantics that usually only surface in production (changelog modes, time attributes, unbounded
state, key/upsert mismatches, savepoint compatibility). It then compiles the pipeline to a single
Flink SQL job and a deployment manifest.

```
pipeline.yaml + models/*.sql
        │  load      YAML → typed IR (line-accurate errors, vars/environments, secrets)
        │  analyze   dependency graph · schema & type inference · changelog modes ·
        │            time attributes · stateful operators · connector validation
        │  lower     Flink SQL: SET / CREATE TEMPORARY TABLE / VIEW / EXECUTE STATEMENT SET
        ▼
target/pipeline.sql   target/manifest.json (graph, schemas, state ids, secrets, artifacts)
        │
        ├─ pipec verify         plan with the real Flink planner, cross-check static analysis
        └─ pipec check-upgrade  savepoint compatibility vs. the running version
```

Language reference: [docs/DSL.md](docs/DSL.md).

## Quick start

Requires Python ≥ 3.10 and PyYAML.

```bash
python3 -m pipedsl validate examples/orders
```

```bash
python3 -m pipedsl compile examples/orders --env prod -o target
```

```bash
python3 -m pipedsl graph examples/orders
```

Plan the generated SQL with a local Flink distribution. No cluster or connector jars are
needed, because production connectors are swapped for built-in stand-ins with the same schema and
changelog semantics:

```bash
FLINK_HOME=~/code/flink/build-target runtime/build.sh
```

```bash
python3 -m pipedsl verify examples/orders --flink-home ~/code/flink/build-target
```

Check whether a change can restore from the running version's savepoint:

```bash
python3 -m pipedsl check-upgrade deployed/manifest.json examples/orders
```

## Example

```yaml
models:
  customer_ltv:
    sql: |
      SELECT customer_id, SUM(amount) AS lifetime_value, COUNT(*) AS order_count
      FROM clean_orders
      GROUP BY customer_id

sinks:
  ltv_to_lake:
    from: customer_ltv
    connection: lake
    table: analytics.customer_ltv
    mode: append
```

```
pipeline.yaml:120: error[PD407]: sink 'ltv_to_lake' is append-only but 'customer_ltv' produces
updates (non-windowed GROUP BY keeps one entry per key forever)
    hint: use `mode: upsert` with a primary_key, or make the query append-only (e.g. a window
    aggregation instead of a running GROUP BY)
```

## Tests

```bash
PYTHONPATH=.:tests python3 -m unittest discover -s tests
```

With `FLINK_HOME` set, the suite also plans 13 SQL patterns with the real Flink planner and checks
that the compiler's changelog inference matches it exactly. The patterns are aggregations, windows,
inner/outer/interval/temporal joins, CDC, dedup and OVER.

## Layout

| Path | |
|---|---|
| `pipedsl/loader.py` | YAML → IR, structure validation, vars/env/secrets |
| `pipedsl/sqlscan.py` | SQL tokenizer and static analysis (refs, stateful ops, inference) |
| `pipedsl/analyze.py` | semantic checks and relation facts |
| `pipedsl/connectors.py` | declarative connector catalog |
| `pipedsl/lower.py` | Flink SQL + manifest generation |
| `pipedsl/verify.py` | Flink planner cross-check |
| `pipedsl/upgrade.py` | savepoint compatibility diff |
| `runtime/` | Java UDFs used by generated SQL (`PIPEDSL_ASSERT`) |

## Known limits / next steps

- **Static analysis is heuristic.** It tokenizes the SQL rather than fully resolving it, so
  `verify` is the authority and CI should run both. Its blind spots are UDF return types,
  `UNNEST`/`LATERAL`, set operations beyond the first branch, and complex expression types (these
  ask for a contract). A JVM sidecar that uses Flink's own parser and validator could replace the
  heuristics later.
- **`state_id` is a logical identity.** To actually restore SQL operator state across versions,
  the deployer should compile with `COMPILE PLAN` against the production connector image and
  keep the plan stable for unchanged `state_id`s.
- Expectation violation **metrics** (drop counts) need a metric-emitting UDF instead of a plain filter.
- Python front-end: a decorator API that emits the same IR.
