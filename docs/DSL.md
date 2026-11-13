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
ml_models: {...}                  # models for ML_PREDICT; see "Model inference" below
deploy: {...}                     # Kubernetes deployment; see "Deployment" below
# or, instead of sources/models/sinks:
replicate: {...}                  # whole-database replication; see "Replication" below
```

Durations: `500ms`, `5s`, `10m`, `1h`, `7d`.

### Variables and secrets

- `${var:name}` is resolved at compile time from `vars`, then overridden by `environments.<env>.vars`.
- `${file:name}` names a file the job reads, such as a TLS trust store. Like a secret it stays a
  placeholder. At startup the runner replaces it with `$CASCADE_FILES_DIR/<name>` (default
  `/opt/cascade/files`; `/` and other characters become `_`), where `cascade deploy` mounts the Secret
  `<deployment>-files`.
- `${secret:path}` is **never resolved by the compiler**. It is passed through to the generated SQL as
  a placeholder and listed in `manifest.json → secrets`; the platform agent injects it at deploy
  time. Properties marked as secrets (`password`, `sasl_jaas_config`) must use a `${secret:...}`
  reference or compilation fails (CD201).

### Batch pipelines

`mode: batch` (top level; the default is `streaming`) runs the pipeline once over bounded inputs and
finishes. Use it for nightly recomputes and one-off reprocessing.

- **Sources are read as a bounded snapshot.**
  - **Kafka** reads up to the latest offset at start (`scan.bounded.mode`).
  - **Iceberg and Hudi** do a one-off read.
  - **Filesystem, JDBC and Paimon** are bounded already.
  - **Datagen** needs `number_of_rows`, even with `sequence` fields.
  - **Relational CDC sources** (Postgres, MySQL, SQL Server, Oracle, Db2) are read with a JDBC scan of
    the same table. Every CDC connector declares a changelog even in snapshot mode, and Flink's batch
    planner rejects that; the connector tests found it.
  - **MongoDB CDC and Kinesis** have no bounded read (CD120).
  - **`custom` connectors:** your options decide.
- **Results are final.** Every model emits each result once, so a `GROUP BY` can write to an append
  sink, and no state TTL is needed.
- **Streaming-only features are errors.** Temporal joins (CD121) and `backfill` (CD122): a batch
  run always reads its inputs from the start, and sees one version of each table.
- **Deployment.** It's a FlinkDeployment with `upgradeMode: stateless`; a new spec reruns the job.
  There's no compiled plan (Flink compiles plans for streaming only), so `cascade plan` refuses.
- **Tests.** `cascade test` runs a batch pipeline's tests as a streaming job over the same fixtures,
  which ends with the same final results.

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
| `postgres-cdc`, `mysql-cdc`, `oracle-cdc`, `sqlserver-cdc`, `db2-cdc` | ✓ updating | — | requires `primary_key`; `table: schema.table` |
| `mongodb-cdc` | ✓ updating | — | `database`, `collection`; key `_id` |
| `iceberg` | ✓ (streaming read) | append, upsert | `table: db.table` |
| `jdbc` | ✓ (lookup joins) | append, upsert | |
| `filesystem` | ✓ | append | `path`, many formats |
| `kinesis` | ✓ | append | `stream_arn`; `region` on the connection |
| `dynamodb` | — | append, upsert | upserts put by key and delete on retraction |
| `elasticsearch` | — | append, upsert | Elasticsearch 7 API (also OpenSearch 1.x); key = document id |
| `paimon` | ✓ (changelog when keyed) | append, upsert | `table: db.table` in the connection's `warehouse`; created on first use from the declared columns and key (`table_options` on the sink) |
| `hudi` | ✓ (changelog when keyed) | append, upsert | `path` under the connection's `base_path` |
| `custom` | ✓ | as declared | any Flink SQL connector, see below |
| `datagen` / `print` / `blackhole` | dev | dev | |

**Postgres CDC and lost replication slots.** A source restored from a savepoint resumes from the
position its replication slot holds. If the slot is gone, for example after a database failover
or a cleanup, Flink CDC silently creates a new one at the current position. Every change in
between is lost, with no error; the soak test reproduced this.

The changes in the gap can't be read back: a new slot only streams what happens after it was
created, and Postgres releases the WAL behind a dropped slot. So cascade does two things.

**Making slot loss rare.** At every start, SqlRunner checks each Postgres CDC source's slot:
- **On a fresh start against Postgres 17+,** it creates the slot with `failover = true`. A failover
  slot follows a failover to a standby, provided the standby syncs slots: `sync_replication_slots
  = on` on the standby and `synchronized_standby_slots` on the primary, which is your DBA's
  configuration. On older Postgres the connector creates an ordinary slot.
- **A slot Postgres invalidated** (`wal_status = lost`, past `max_slot_wal_keep_size`) counts as gone.
- **Every start logs each slot's WAL status and lag,** and warns at `unreserved` (about to be
  invalidated).

The runner only runs at start. Between starts, `deploy.monitoring` adds slot alerts for every
Postgres CDC source, read from the standard
[postgres_exporter](https://github.com/prometheus-community/postgres_exporter) (its
`replication_slot` collector) scraping the source database:
- **Lag:** the slot is behind by more than `alerts.max_slot_lag_bytes` (default 1 GiB).
- **Inactive:** nothing has read the slot for 10 minutes.
- **At risk:** its remaining WAL budget is under `alerts.min_slot_safe_wal_bytes` (needs
  `max_slot_wal_keep_size`).
- **Gone:** the slot disappeared.

They match the slot and the generations the runner may move it to. The connector tests check the
metric names against a real exporter.

**Failover is tested.** A Postgres 17 primary with a streaming standby that syncs slots:
1. The runner creates the source's slot as a failover slot, and it's synced to the standby.
2. The primary is stopped while the job is down, with a change the job hasn't read.
3. The standby is promoted.
4. The job is restored against it, and the unread change plus later changes all arrive.

The standby needs `sync_replication_slots = on` and `hot_standby_feedback = on`, and the primary
needs `synchronized_standby_slots`.

**When a slot is gone on restore, `on_slot_loss` on the source decides:**

```yaml
sources:
  customers:
    connection: crm
    table: public.customers
    primary_key: [customer_id]
    on_slot_loss: resnapshot      # default: refuse
```

- **`refuse`** (default): the runner refuses to start and names the slot. Recover by starting the
  source fresh (deploy without the savepoint, or backfill the models that read it), or by setting
  `CASCADE_ALLOW_SLOT_LOSS=1` and replaying the rows that changed in the gap.
- **`resnapshot`**: the source heals itself.
  - **New slot generation.** The runner moves the source to a new slot, named `<slot>_g<time>`
    and created as a failover slot.
  - **Fresh snapshot.** The source sees that its saved position belongs to another generation,
    so it drops that position, re-reads the table, and streams on from the new slot. All
    downstream state is kept.
  - **Reconcile step.** A step between the source and the rest of the job keeps the last row of
    every key. It turns the re-read into exact changes against what downstream already has:
    unchanged rows produce nothing, changed rows become updates, new rows become inserts. Once
    the snapshot is complete, it deletes the keys that vanished during the gap.

  This was tested against Postgres 17 with updates, inserts, deletes and delete-then-reinsert in the
  gap, then a TaskManager failover, a normal restore, and a second loss. The aggregates downstream
  matched Postgres exactly each time. The nightly soak repeats it under load.

  What `resnapshot` can't restore: intermediate versions. A row changed twice in the gap arrives
  as its final version, which only matters to event-time temporal joins over history. The cost is
  state for one copy of the table, as with a keyed upsert source. It compiles to
  `cascade-healing-cdc`, which wraps `postgres-cdc` and needs its incremental snapshot reader (on
  by default).

The slot checks run in SqlRunner, which every deploy target uses. Running `pipeline.sql` through the
SQL client skips them.

Every connector's jar version defaults to the one tested with Flink 2.3 (`TESTED_VERSIONS` in
`cascade_dsl/connectors.py`). `deploy.artifacts` overrides it. `tests/test_connector_smoke.py`
plans a job against each real jar, which checks option names and binary compatibility.

`integration/connectors.py` (workflow `connectors`) runs the rest end to end, against real systems or
their official emulators:
- **CDC sources (Postgres, MySQL, SQL Server, MongoDB):** the snapshot, then live inserts, updates and
  deletes, mirrored exactly into Postgres, plus the batch read.
- **Sinks (Elasticsearch, DynamoDB, Kinesis, Hudi, Paimon, Iceberg):** rows land. Kinesis, Hudi,
  Paimon and Iceberg are also read back through their sources.
- **Lost change history:**
  - **A purged MySQL binlog:** the source fails loudly, and the job restarts until it's fixed.
  - **A cleaned-up SQL Server change table:** the change still arrived.
  - **Postgres:** see the next section.

Kafka, Postgres CDC and JDBC also run in `integration/run.py`.

**Oracle CDC** runs against Oracle XE 21c (multitenant), set up by `integration/oracle/setup.sh`:
ARCHIVELOG mode, supplemental logging, and a common user with LogMiner grants. The setup is part of
the test. A table in a pluggable database needs the connection's `pdb`. The tests found two problems:
- **The connector doesn't bundle the Oracle driver,** so the catalog adds `ojdbc8`.
- **Flink CDC 3.7's incremental reader can't find tables in a PDB.** With `pdb` set, the compiler
  uses the legacy reader and LogMiner's online catalog. Restoring that reader from a checkpoint is
  tested (case `oracle-restore`): changes made while the job was down arrive, and nothing is read
  twice.

**Db2 CDC** is only planned against its jar. It reads changes through IBM's ASN capture, which has to
be installed and administered inside a privileged, licensed Db2 instance. Debezium publishes no image
with it, so there's no trustworthy Db2 to test against in CI. The jar check did find that the connector
doesn't bundle IBM's driver, like MySQL's and Oracle's, so the catalog adds `com.ibm.db2:jcc`.

`cascade fetch-jars <project>` downloads a pipeline's connector jars into `~/.cache/cascade/jars`.
It prints a `CASCADE_EXTRA_JARS=...` line; with that set, `cascade plan`, `test` and `verify` work
locally for any connector.

### Custom connectors

For a Flink SQL connector the catalog doesn't have:

```yaml
connections:
  ch:
    type: custom
    connector: clickhouse                 # the Flink 'connector' option (its factory identifier)
    artifacts: ["com.example:flink-connector-clickhouse:1.2.3"]   # jars for the runtime image
    changelog: append                     # as a source: append | updating | by_format
    sink_modes: [append, upsert]          # default [append]
    requires_primary_key: false
    properties: {url: "clickhouse://ch:8123", password: "${secret:ch/pw}"}   # raw WITH options
```

On sources and sinks, every key besides the common ones (`from`, `mode`, `columns`, ...) is passed
through as a raw `WITH` option. Keys that look like credentials must be `${secret:...}` references
(CD201).

**Not in the catalog, because no Flink 2.x release exists** (checked on Maven Central in November
2026): Pulsar, Google Pub/Sub, HBase, MongoDB (non-CDC), OpenSearch 2. **ClickHouse** publishes a
2.x jar with no SQL factory (DataStream only); use a [process model](#process-models) for it.
**Redis** has no Apache connector at all. **Delta**'s Flink connector (4.x) ships no self-contained
jar: it needs Delta Kernel, Parquet, Hadoop and the Unity Catalog client, about 30 jars by transitive
resolution. The end-to-end test found this, so it's `type: custom` with those artifacts listed. Any of them works as `type: custom` once a 2.x-compatible
build exists.

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
      # instead of max_out_of_orderness, any watermark expression, or `source` for
      # SOURCE_WATERMARK() (connectors that emit their own watermarks; CD111 otherwise):
      # watermark: "order_ts - INTERVAL '1' MINUTE"
      emit: on_periodic         # or on_event (watermark push-down connectors: Kafka)
      alignment:                # watermark alignment across sources of one group (Kafka; CD110)
        group: shop
        max_drift: 30s
        update_interval: 1s     # optional
    processing_time: proc       # optional: adds `proc AS PROCTIME()`
    primary_key: [order_id]     # required for CDC / versioned tables
    options: {...}              # raw Flink WITH options, applied last
```

> In YAML flow style (`{a: DECIMAL(10, 2)}`) the comma splits the mapping; quote such types or
> use block style.

`VARIANT` (semi-structured data, Flink 2.1+) is a column type too. `PARSE_JSON` and `TRY_PARSE_JSON`
produce it, `v['key']` reads a field and `JSON_STRING(v)` turns it back into text. Two limits come from
Flink 2.3:
- **No compiled plan.** Flink 2.3 can't write VARIANT into a compiled plan, so a pipeline that uses it
  deploys as statements. `cascade plan` and `deploy --plan-dir` refuse it, and `restore-test` runs whole
  scripts.
- **No casts out of VARIANT.** `CAST(v['n'] AS DOUBLE)` is rejected; use `JSON_STRING`. Indexing a
  non-object variant (`v['k']` on a number) fails the job, so prefer `TRY_PARSE_JSON` on untrusted input.

Local stand-ins compute a VARIANT source column as NULL. Fixtures take any JSON value for it, and
expectations compare VARIANT values as JSON.

### Metadata and computed columns

A column can come from the connector's metadata instead of the payload, or be computed from other
columns:

```yaml
columns:
  order_id: STRING
  epoch_ms: BIGINT
  kafka_ts: TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'   # Flink's own syntax works
  partition: {type: INT, metadata: partition, virtual: true}
  offset: {type: BIGINT, metadata: true}                  # key = the column name
  ts: {type: TIMESTAMP_LTZ(3), expression: "TO_TIMESTAMP_LTZ(epoch_ms, 3)"}
event_time: {column: kafka_ts, max_out_of_orderness: 5s}  # event time may be either kind
```

- **Catalogued keys.** Kafka (`topic`, `partition`, `headers`, `leader-epoch`, `offset`, `timestamp`,
  `timestamp-type`, and format keys under `value.`/`key.`), Postgres, MySQL, Oracle, SQL Server, Db2 and
  MongoDB CDC (`op_ts`, `table_name`, `database_name`, `schema_name`, `row_kind`, `collection_name` as
  each supports), and filesystem (`file.path`, `file.name`, `file.size`, `file.modification-time`) are
  checked. An unknown key is CD220, a different type CD221. Every key is planned against the real jar
  in the connector smoke test.
- **Other connectors.** Their metadata isn't catalogued; `cascade verify` and the planner check it.
- **Primary keys** must be physical columns (CD106).
- **`on_slot_loss: resnapshot`** can't read metadata (CD222). The re-read describes the snapshot, not
  the change.
- **In tests,** fixtures give metadata columns like any other column, and computed columns are derived
  from the fixture's values.

### Kafka topics and message keys

- `topic: [orders.eu, orders.us]` reads several topics; `topic_pattern: "orders\\..*"` reads every topic
  matching a Java regex. A source names exactly one of the two.
- **Message keys of append topics.** `key_fields: [customer_id]` puts those columns in the message key.
  A sink then partitions by them, which keeps each key's records in order; a source reads them from the
  key. `key_format` defaults to the value's format. `value_includes_key: false` leaves the key columns
  out of the value (Flink's `EXCEPT_KEY`). Upsert topics key by `primary_key` instead (CD224), and every
  key column must be a physical column (CD225).
- **TLS.** `security_protocol: SSL` (or `SASL_SSL`) with `ssl_truststore` and, for mutual TLS,
  `ssl_keystore`, both `${file:...}` references, plus `ssl_*_password` secrets and `ssl_*_type`
  (`JKS`, `PKCS12`, `PEM`). `integration/connectors.py` (case `kafka-tls`) writes and reads over
  mutual TLS against a broker that requires client certificates.

### Reading another pipeline: `from_pipeline`

```yaml
sources:
  ltv:
    from_pipeline: ../shop      # the upstream project directory
    sink: ltv_out               # one of its sinks
    startup: earliest           # optional: any source key overrides what is derived
    event_time: ts              # optional, as on any source
```

The compiler compiles the upstream project (same `--env`) and takes from that sink:
- **The connection.** An identical connection here is reused; otherwise it's added as
  `<connection>__<pipeline>`, along with its secrets.
- **The data's location and schema.** That's the topic, table or path, the columns, primary key
  and format.
- **How to read it.**
  - An **upsert Kafka sink** is read with upsert-kafka as a keyed changelog.
  - An **exactly-once Kafka sink** is read with `isolation.level=read_committed`.

The manifest lists these imports. Upstream changes show up on the next compile, as schema changes
of this source.

Connection, columns and format can't be set on such a source (CD005). Other errors:
- an import cycle (CD130)
- an upstream project that doesn't compile (CD131)
- an unknown sink (CD132)
- a sink that can't be read back, such as `print` (CD133)
- in streaming mode, a sink that can't be read as a stream (JDBC, DynamoDB, or Iceberg upserts)
  (CD134)

This is also how to **split a pipeline into several jobs**: one project per job, joined through
sinks. One pipeline always compiles to one job; splitting it inside a project would compute
shared models twice, or need a hidden topic between the jobs.

Across projects:
- **`cascade lineage <root>`** prints which pipeline reads which sink, for every project under
  `<root>`, as a Mermaid graph.
- **`cascade impact <project> --root <root>`** recompiles every project that reads this project's
  sinks, transitively, and fails if any no longer compiles. Run it in the upstream project's CI to
  catch a breaking sink change before it ships. With `--verify` (needs `FLINK_HOME`), each reader is
  also planned by Flink, which catches what static analysis can't, such as a renamed column used only
  in a `WHERE`.

Kafka sources can also read any compacted topic as a table with `changelog: upsert` (needs a
`primary_key`).

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
  per-source value, so the short job-wide timeout doesn't reach it. A Kafka source that declares
  none gets 60 s. That isn't "never": with more readers than partitions, a reader with no
  partition only stands down through an idle timeout, and would otherwise hold back event time
  forever. `idle_timeout: 0s` opts a Kafka source out of idling entirely.
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

**Metadata.** `metadata: {column: key}` writes a column to the connector's metadata instead of the
payload. Kafka takes `timestamp`, `headers` and `topic`. With a `topic` column, each row goes to the
topic it names, and the sink's `topic` is the list of topics allowed (CD209). Kafka sinks also take
`key_fields` (see "Kafka topics and message keys").

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

## Process models

When SQL can't express it, a model can be **DataStream code**: keyed state and timers, broadcast
state, async I/O, custom windows and triggers, side outputs. The class implements
`dev.cascade.api.Process` (in `cascade-runtime.jar`):

```java
public class Sessionize implements Process {
  public DataStream<Row> apply(ProcessContext ctx, List<DataStream<Row>> inputs) {
    KeyedStream<Row, String> clicks = (KeyedStream<Row, String>) inputs.get(0);   // key_by
    return clicks.process(new Sessions(ctx.config(), ctx.sideOutput("late")))
        .returns(ctx.outputType())
        .uid(ctx.uid("sessionize"));        // stable uid: state survives upgrades
  }
}
```

```yaml
models:
  sessions:
    process:
      class: com.acme.process.Sessionize
      jar: jobs/acme-process.jar      # copied into the image (usrlib)
      inputs: [clicks]                # sources or models, one DataStream each, in order
      key_by: [user_id]               # optional: inputs arrive keyed by these columns
      config: {gap: PT30M}            # ctx.config()
      changelog: append               # or upsert (with primary_key) / retract
      event_time: session_closed      # output time attribute; carries the stream's watermarks
      side_outputs:                   # each becomes a relation: sessions_late
        late: {columns: {user_id: STRING, url: STRING, ts: TIMESTAMP(3)}}
    columns: {...}                    # required: the output schema
    expectations: [...]               # as on any model
```

The model's output is a relation like any other. SQL models, sinks and tests read it, and its
event time works with windows downstream. Inputs carry their event-time timestamps and
watermarks; rows of updating inputs carry their `RowKind`. `cascade test` runs process models
with fixtures. `examples/sessions` sessionizes clicks with state, timers and a late-data side
output, and has a SQL window over the result.

Limits:
- **No compiled plan.** Flink can't compile a plan for DataStream code. A pipeline with a process
  model deploys as statements (`cascade plan` refuses), and its SQL operators get Flink's
  generated UIDs. Upgrades that don't change the pipeline's SQL restore fine. A changed SQL query
  may not, so check it with `restore-test`.
- **The model's own state is yours.** `check-upgrade` sees a process model only as a whole
  (CD140). Use `ctx.uid(...)` on stateful operators, and change state in a compatible way.
- **`cascade restore-test old/ new/` still works.** For these pipelines it runs the whole scripts:
  the old version on stand-in connectors, a savepoint, then the new version restored strictly. That
  checks every operator, including the process model's own state. For example, a changed `config`
  restores, and a removed process model is refused.

### Python process models

```yaml
    process:
      language: python
      file: jobs/sessionize.py
      function: sessionize          # def sessionize(ctx, inputs): -> DataStream
      inputs: [clicks]
      key_by: [user_id]
```

The function gets one PyFlink `DataStream` of `Row`s per input, and `ctx`:
- `ctx.config`
- `ctx.uid(name)`
- `ctx.output_type`: the declared columns as row type information
- `ctx.side_output(name)`: an `OutputTag`; emit with `yield tag, row`

It returns a stream of rows matching the columns. Keyed state, timers and side outputs work as in
PyFlink. `examples/sessions_py` mirrors the Java sessionizer.

PyFlink builds Python operators from Python, so these pipelines are driven by
`cascade_dsl/pyrunner.py`. It applies the job script through SqlRunner's Java helpers and adds the
Python models in between:
- **Kubernetes:** Flink's `PythonDriver` in application mode. The image gets PyFlink, and the
  runner and model files ship in the Python ConfigMap.
- **`flink-run`:** `flink run -py`.
- **`cascade test`:** runs them too (needs PyFlink: `pip install apache-flink`, or `CASCADE_PYTHON`).

The function and the classes in its file are pickled by value, so workers need nothing installed.
Two PyFlink differences:
- **Watermarks per bundle.** PyFlink hands Python functions records in bundles, with the watermark
  per bundle, so lateness checks in `process_element` are only as exact as the bundle.
  `exact_watermarks: true` on the process sets the bundle size to 1, which makes lateness exact at a
  throughput cost. The example's late-data branch uses it.
- **Column types:** the scalar types, `TIMESTAMP_LTZ`, and nested `ARRAY<…>`, `MAP<…, …>` and
  `ROW<…>`.

`cascade restore-test` works for these pipelines too: it runs on the Python runner, with the same
strict savepoint restore as for Java process models.

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

### Aggregate, table and process table functions

`kind:` declares what a function is; the default is `scalar`.

```yaml
functions:
  weighted_avg: {language: java, kind: aggregate, class: com.acme.fn.WeightedAvg, jar: udfs/fn.jar, returns: DOUBLE}
  split_tags:   {language: java, kind: table, class: com.acme.fn.SplitTags, jar: udfs/fn.jar,
                 returns: "ROW<tag STRING, pos INT>"}
  streak:       {language: java, kind: process_table, class: com.acme.fn.Streak, jar: udfs/fn.jar,
                 returns: "ROW<streak INT, ok BOOLEAN>"}
  py_avg:
    language: python
    kind: aggregate
    file: udfs/reviews.py
    class: WeightedAvg                         # create_accumulator, accumulate, get_value (+ retract, merge)
    accumulator: ROW<total DOUBLE, weight DOUBLE>
    returns: DOUBLE
  py_split: {language: python, kind: table, file: udfs/reviews.py, function: split_tags,
             returns: "ROW<tag STRING, pos INT>"}   # a generator yielding tuples
```

- **Aggregate functions** are called like `SUM`. Over updating input they need `retract`.
- **Table functions** are used as `LATERAL TABLE(f(x)) AS t(a, b)`. Their columns are typed from
  `returns`, which must be a `ROW` (CD010).
- **Process table functions** (Flink 2.1+, Java only) are called as
  `FROM f(input => TABLE t PARTITION BY k)`. Their output is the partition keys followed by the `ROW`
  fields. Their `@StateHint` state is keyed state: it needs a state TTL (CD350) and counts toward max
  parallelism. `verify` and `plan` map the planner's operator to the model, so UIDs are pinned as for SQL
  state.

  Build PTF jars with `javac -parameters`, so Flink can name the state.
- **Python** aggregate functions are plain classes and table functions plain generators; the generated
  shim wraps them with PyFlink's `udaf` and `udtf`. A list accumulator becomes a Row when
  `accumulator` is a `ROW`.
- Using a non-table function as a table is CD165.
- **Python API:** `p.java_function(..., kind="table")`, `p.python_function(gen, kind="table", ...)`,
  `p.python_aggregate(Class, returns=..., accumulator=...)` and
  `p.ml_model(name, provider=..., input=..., output=..., properties=...)`. Metadata columns, `key_fields`
  and `topic_pattern` pass through `p.source` and `p.sink` as in YAML.

See [`examples/functions`](../examples/functions) (Java, including a PTF restored from a savepoint) and
[`examples/functions_py`](../examples/functions_py).

## Model inference: `ml_models`

```yaml
ml_models:
  triage:
    provider: openai                 # built in: openai; others: provider id + artifacts
    input: {body: STRING}
    output: {queue: STRING}
    properties:                      # the provider's options; _ becomes -
      endpoint: https://api.openai.com/v1/chat/completions
      model: gpt-4.1-mini
      api_key: ${secret:openai/api_key}
      system_prompt: "Answer with one word: billing, outage or other."
models:
  routed:
    sql: SELECT ticket_id, queue FROM ML_PREDICT(TABLE tickets, MODEL triage, DESCRIPTOR(body))
```

- **Lowering.** Each model becomes `CREATE TEMPORARY MODEL`. `ML_PREDICT`'s output is the input
  table's columns followed by the model's output columns.
- **Checks.** The provider's required options (CD209) and secret options (`api-key`, `auth-token`;
  CD201) are checked.
- **Compiled plans** reference the model by name, so the API key stays out of `plan.json`.
- **Tests never call the model.** Its answers are given like a source's rows, under the model's name,
  with input and output columns:

  ```yaml
  given:
    triage: [{body: "I was charged twice", queue: billing}]
    tickets: [{ticket_id: t1, body: "I was charged twice"}]
  ```

  An input with no pinned answer fails the test. `verify`, `plan` and `restore-test` stand-ins
  answer NULL.
- **Triton isn't built in:** `flink-model-triton` 2.3.0 rejects every input column when the model is
  created ("input column ... should be null"). Use `provider: triton` with `artifacts` once a fixed
  release exists.

See [`examples/ml_predict`](../examples/ml_predict).

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

## Replication: `replicate`

A replication copies every table matching a pattern from a database into a lake or Kafka, and keeps
copying. It creates each sink table from the source schema and applies schema changes as they happen:
added columns, new tables. Flink SQL can't do this, because a SQL job has a fixed set of tables with
fixed columns. Flink CDC's pipeline runtime can, so a replication compiles to a Flink CDC pipeline
definition instead of SQL.

```yaml
pipeline: crm_replica
connections:
  crm: {type: postgres-cdc, properties: {hostname: db, database: app, username: cdc, password: "${secret:crm/pw}"}}
  lake: {type: paimon, properties: {warehouse: s3://lake/wh}}
replicate:
  from: crm                         # postgres-cdc | mysql-cdc
  tables: ["public.*"]              # schema.table (database.table for MySQL), * as a glob
  exclude: [public.audit_log]
  to: lake                          # paimon | kafka (debezium-json, one topic per table)
  schema_changes: evolve            # evolve (default) | try_evolve | lenient | ignore | exception
  route: [{from: "public.orders_*", to: shop.orders}]   # merge or rename tables
  table_options: {bucket: "4"}      # Paimon options of the tables it creates
  source_options: {}                # raw Flink CDC options, applied last
  sink_options: {}
```

- **Its own project.** A replication has no sources, models or sinks (CD601). Read the replicated
  tables from another pipeline.
- **`cascade compile`** writes `cdc-pipeline.yaml`.
- **`cascade deploy`** writes a FlinkDeployment running `dev.cascade.runner.CdcRunner`, and a Dockerfile
  that adds the Flink CDC 3.7 distribution (pinned by sha512) and its pipeline connectors.
- **Postgres tables need `REPLICA IDENTITY FULL`.** Flink CDC's pipeline source needs before-images of
  updates and deletes. The runner checks every matching table at start and refuses, listing the
  `ALTER TABLE ... REPLICA IDENTITY FULL` statements to run. Tables created later need it too, or the
  job fails at their first update.
- **At startup** the runner fills in secrets and runs the Postgres slot guard: a restore whose
  replication slot is gone is refused, because Flink CDC would silently start a new slot. It then builds
  the job with Flink CDC's composer. Monitoring adds the slot alerts.
- **Tested end to end** (`integration/connectors.py`, case `replicate-postgres`):
  - a whole Postgres schema into Paimon: the snapshot, then inserts, updates and deletes
  - an `ALTER TABLE ... ADD COLUMN`, and a table created while running
  - a restore from a checkpoint with a change made while down
  - a refused restore after the slot was dropped
- **No local tools:** `test`, `verify`, `plan` and `restore-test` don't apply; Flink CDC builds the job
  at startup.

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

**High availability.** A stateful job (any `upgrade_mode` but `stateless`, with a checkpoint directory)
gets Kubernetes HA: `high-availability.type: kubernetes`, with its metadata under
`<checkpoints.dir>/ha` (or `deploy.high_availability.storage_dir`). Without HA, a JobManager crash
(OOM, node loss, eviction) brings the application back as a new job with empty state. With it, the
new JobManager resumes the same job from its latest checkpoint.

The kind integration run checks this: it kills the JobManager pod, then checks the job keeps its ID,
restores a checkpoint, and that the upgrade after it continues the totals. Other settings:
- `high_availability: false` turns HA off. It's refused with `upgrade_mode: last-state` or
  `jobmanager.replicas > 1`.
- `jobmanager: {replicas: 2}` adds a standby JobManager.

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

### Monitoring

```yaml
deploy:
  monitoring:
    alerts: {max_backpressure_ms: 500, max_pending_records: 100000, max_violation_ratio: 0.01}
    labels: {team: data}            # added to every alert
```

`monitoring` (or `monitoring: true`) turns on Flink's Prometheus reporter (port 9249, named `metrics`)
and writes `monitoring/alerts.yml` (a Prometheus rule file) and `monitoring/dashboard.json` (Grafana).
For the Kubernetes target it also writes a PodMonitor, a PrometheusRule, and a dashboard ConfigMap
with the `grafana_dashboard` label, all in the kustomization.

The alerts follow the pipeline:
- **Always:** the job down or restarting; checkpoints failing, or none completed for five intervals;
  sustained backpressure.
- **With Kafka sources:** records pending.
- **With event time:** event time not advancing for 15 minutes, so windows stop closing.
- **With `drop` or `quarantine` expectations:** the violation ratio per model and expectation (from
  cascade's expectation counters).
- **With window aggregations:** rows dropped because they arrived after their window closed
  (`numLateRecordsDropped`, over `alerts.max_late_records`, default 0). Interval joins and OVER
  aggregations drop late rows too, but Flink reports no metric for them.

The integration run scrapes the running job and fails if any metric an alert or panel uses isn't
reported. An alert on a misspelled metric would never fire.

`tests/test_alert_rules.py` runs `promtool test rules` in CI. Each alert gets synthetic series for
its condition and must fire with the expected labels and annotations. A healthy series set must
fire nothing.

### Other targets

`deploy.target` picks what `cascade deploy` generates. The default, `kubernetes`, is the
FlinkDeployment above: application mode, one job per cluster.

**`session`: a FlinkSessionJob on a shared, operator-managed session cluster.**

```yaml
deploy:
  target: session
  session_cluster: shared-flink                  # the FlinkDeployment running the session cluster
  jar_uri: s3://artifacts/cascade-runtime.jar    # where the operator fetches the runner
```

Session jobs can't mount files, so the job's scripts travel base64-encoded in its arguments.
The cluster supplies everything else, and `REQUIREMENTS` lists it:
- the connector jars
- process and UDF jars in `usrlib`
- the secrets as environment variables

Python UDFs and process models need files on the cluster, so they aren't supported here. The operator
fetches `jar_uri` over https only, unless `kubernetes.operator.user.artifacts.allowed-schemes` allows
more; `cascade deploy` warns otherwise. The kind integration run deploys a session job this way. On a shared cluster, one
JDBC sink's database outage takes down a TaskManager that other jobs run on (CD215).

**`flink-run`: a `run.sh` around `flink run`, for clusters the operator doesn't manage.**

```yaml
deploy:
  target: flink-run
  run:
    mode: remote                  # or yarn-application
    jobmanager: flink-jm:8081     # remote: the cluster's REST address
  flink_config: {state.checkpoints.dir: "hdfs:///flink/checkpoints"}   # -D options
```

- **`remote`** works with standalone and session clusters. The `flink` client runs SqlRunner,
  which builds the job graph and submits it. The job files sit next to `run.sh` in `job/`, and
  secrets come from the client's environment.
- **`yarn-application`** ships `job/` with the application. Its jars and Python files are copied
  into `job/` and referenced relatively.

**Not generated:** managed services.
- **Amazon Managed Service for Apache Flink** runs your own application jar. Use the
  `flink-run` files with a jar that bundles `cascade-runtime.jar` and `job/`.
- **Confluent Cloud for Apache Flink** accepts only SQL statements: no custom jars, no plans,
  its own catalog of tables. `pipeline.sql` is plain Flink SQL that you can adapt.
- **Ververica Platform** deployments are, like the operator's, a jar plus arguments. That's the
  session target's shape, but there's no generator for Ververica's API.

## Schema import

`cascade import-schema --registry URL --subject SUBJECT [--version N] [--source NAME]` (or
`--avsc FILE`) prints a `columns:` block converted from Avro: logical types, nullable unions,
records → `ROW`, arrays, maps, enums. Basic auth comes from `CASCADE_REGISTRY_USER_INFO=user:pass`.
It's a reviewable, explicit step, so compilation never depends on the registry.

## What the DSL can't do, and why

These were reviewed one by one in November 2026. Each is a limit of Flink, of the connectors, or of
the data itself, not something missing from the compiler.

- **Bootstrapping or migrating SQL operator state** (State Processor API). Flink SQL operators keep
  internal, unversioned state layouts that change between Flink versions and planner rules.
  Writing them would tie the DSL to planner internals. Instead:
  - **New state:** build it from history with `backfill`.
  - **A process model's own state:** it's yours. Use the State Processor API against its
    `ctx.uid(...)` operators.
- **History a source doesn't have.** A backfill reaches as far as Kafka retention goes. A CDC
  snapshot holds only each row's current version, so event-time temporal joins over history find
  no older versions. That's a property of the data, not the compiler.
- **DataStream iterations, Gelly, Flink ML.** Flink 2.0 removed iterations and Gelly, and Flink ML
  has no 2.x release. Iterative algorithms belong in a process model or outside Flink.
- **Per-source idle timeouts on CDC sources.** Flink CDC's table sources don't implement watermark
  push-down, so only the job-wide timeout reaches them (CD109). Fixing that needs a change in
  Flink CDC itself.
- **A JDBC sink's outage killing its TaskManager** (CD215). This is a bug in
  flink-connector-jdbc 4.1: `close()` rethrows during failover. Application mode, which is the
  default target, contains it: Kubernetes restarts the pod and only this job is affected.
- **Exactly-once into sinks without transactions or idempotence.** Elasticsearch, DynamoDB and
  filesystem-append deliveries are what each connector guarantees. Upsert modes make replays
  idempotent wherever the sink has a key.
- **Connectors with no Flink 2.x release:** Pulsar, Pub/Sub, HBase, MongoDB (non-CDC),
  OpenSearch 2. ClickHouse's 2.x jar has no SQL factory, and Redis has no Apache connector. Use
  `type: custom` once a build exists, or a process model for DataStream-only connectors.
- **Flink 1.x.** Compiled-plan UID pinning, the 2.x connector line, the planner behaviour the
  upgrade matrix verifies and the process-model API are all checked on 2.x only. Supporting 1.20
  would mean a second connector catalog and a second verified matrix. It's possible, but not
  supported.
- **Managed Flink services.** Confluent Cloud takes only SQL statements in its own catalog (no
  jars, no plans). AWS Managed Flink and Ververica run jars through their own APIs. The
  `flink-run` files and `pipeline.sql` are the starting points.
- **Splitting one project into several jobs.** One pipeline is one job. Splitting inside a
  project would compute shared models twice, or need hidden topics between jobs. Split across
  projects and connect them with `from_pipeline`.
- **Compiled plans with VARIANT.** Flink 2.3 can't serialize the type into a plan, so such pipelines
  deploy as statements (see Sources).
- **Python process table functions.** PyFlink has no ProcessTableFunction; use a Python process model.
- **Materialized tables** (`CREATE MATERIALIZED TABLE`). Flink runs them through the SQL Gateway with a
  persistent catalog and a workflow scheduler for refreshes, outside any one job. A continuously
  refreshed materialized table is a model with an upsert sink, which is what cascade compiles.
- **Triton models** (see "Model inference"), until flink-model-triton fixes its input check.
- **Replication sources and sinks beyond Postgres/MySQL and Paimon/Kafka.** Flink CDC has pipeline
  connectors for Oracle, SQL Server, Iceberg, Doris, StarRocks and others. `source_options` and
  `sink_options` pass raw options, but only the four pairs are tested.
- **Planned UID pinning for pipelines with process models or `mode: batch`.** Flink compiles plans
  only for pure streaming SQL. Such pipelines deploy as statements. Process-model state is pinned
  by `ctx.uid`.
- **Proof at production scale.** The soak (`integration/soak.py`, nightly) covers:
  - failures, rescaling, CDC schema changes and slot loss
  - thousands of keys and multiple partitions

  The slot-loss case found silent data loss in Flink CDC, which the runner now refuses (see
  "Postgres CDC and lost replication slots"). It runs on a CI runner. State in the tens of GBs, high parallelism and sustained high
  throughput need a real cluster; run `soak.py` there with `--rate`, `--customers` and
  `--partitions` scaled up.

## Editors

`cascade lsp` is a language server (LSP over stdio) for `pipeline.yaml`:
- **Diagnostics:** the compiler's own, as you type. Unsaved buffers are checked from a temporary copy
  of the project.
- **Completion:** connector types after `type:`; the connector's properties inside a connection,
  source or sink; and section keys.
- **Hover:** a diagnostic code's meaning.
- **Go to definition and find references:** for any source, model, sink, connection or function
  name, whether used in SQL, `from:`, `connection:` or `inputs:`.

`editors/vscode` is a small VS Code extension that starts it. CI runs it in a real headless VS Code
and checks that the diagnostics arrive and that go-to-definition works. Any other LSP client can run
`cascade lsp`.

## Diagnostic codes

| Range | Area |
|---|---|
| CD0xx | YAML structure, unknown keys (with suggestions), vars, types |
| CD1xx | names, sources, event time, primary keys (CD108 job-wide idle timeout, CD109 conflicting idle timeouts, CD110 alignment/emit without push-down, CD111 SOURCE_WATERMARK unsupported); batch (CD120 unbounded source, CD121 temporal join, CD122 backfill); `from_pipeline` (CD130 cycle, CD131 upstream doesn't compile, CD132 unknown sink, CD133 sink not readable, CD134 not streamable); process models (CD140 state not checked by upgrades, CD141 key_by column, CD142 event_time / primary_key column); functions (CD160 missing module, CD161 function not defined, CD162 shadows a built-in, CD163 module name clash, CD164 jar not built, CD165 not a table function) |
| CD2xx | connectors, properties, formats, secrets (CD215: a JDBC sink restarts the TaskManager during a database outage); metadata columns (CD220 unknown or read-only key, CD221 type differs, CD222 metadata with `on_slot_loss: resnapshot`, CD223 sink metadata column missing); Kafka keys (CD224 key_fields on an upsert topic, CD225 key column not physical) |
| CD30x–CD32x | model SQL, references, cycles, Python model build errors (CD304), contracts, expectations |
| CD33x–CD34x | windows and time attributes, temporal joins |
| CD35x | state / TTL |
| CD36x | schema lock (CD360 used, CD361 stale, CD362 Flink could not resolve a model) |
| CD4xx | sinks: changelog compatibility, keys, types, ON CONFLICT (CD412: DO ERROR / DO NOTHING need event time on every scanned source) |
| CD50x | pipeline-level (unused nodes, no sinks) |
| CD52x | scaling (CD520 no max parallelism with keyed state, CD521 invalid bounds, CD522 uneven key groups) |
| CD53x | deployment (CD530 secret in deploy settings) |
| CD60x | replication (CD601 sources/models/sinks or batch mode in a replication, CD602 unsupported from/to) |

`cascade validate --json` emits machine-readable diagnostics for editors and CI.
