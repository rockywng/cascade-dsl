# Integration environment

Runs a pipeline against real systems, using exactly what the tools generate:

| | `run.py` (docker compose) | `k8s.py` (kind + Flink Kubernetes Operator 1.16.1) |
|---|---|---|
| Kafka, Postgres | containers | pods |
| Flink 2.3.0 | session cluster | application mode, managed by the operator |
| Runtime image | built from the Dockerfile `cascade deploy` writes | the same image, loaded into kind |
| Job | `cascade plan` + the ConfigMap from `cascade deploy --plan-dir`, submitted through `SqlRunner` | `kubectl apply -k` of the `cascade deploy` directory |
| Upgrade to v2 | `flink stop --savepointPath`, then submit v2 with `-s` (strict) | `kubectl apply -k` of v2: the operator stops with a savepoint and restores |

[`pipeline/pipeline.yaml`](pipeline/pipeline.yaml) reads orders from Kafka and customers from
Postgres CDC. It drops bad orders with an expectation, enriches orders with an event-time temporal
join, and computes a windowed revenue and a running LTV. Its sinks are append Kafka, upsert Kafka
and a JDBC upsert into Postgres. v2 adds a model and a sink.

```bash
python3 integration/run.py
```

```bash
python3 integration/k8s.py
```

Both need Docker with compose and Java 17. `k8s.py` also needs kind, kubectl and helm. Use
`--keep` to leave the stack running, and `--skip-build` to reuse the image. Each run takes a few
minutes.

## What a run checks

**v1**
- **Secrets:** without the secret in the environment, the runner refuses to start and names the
  missing secret. With it, the job runs and the secret is never written to disk.
- **Sinks**, checked from the outside: the JDBC table in Postgres, the upsert topic (latest value
  per key) and the append topic.
- **Expectations:** the bad order is dropped.
- **Enrichment:** orders get the customer's segment through the CDC temporal join.

**The upgrade to v2**
- **Pinning:** `cascade plan --previous` keeps 19 operator UIDs, and the restore is strict.
- **Keyed state:** LTV totals continue from v1's values.
- **Kafka offsets:** no order is counted twice.
- **CDC position:** a customer change made before the upgrade is applied by the restored join, and
  the table is not re-read.
- **New-operator hazard:** the plan report warns that the new operators start empty. The test
  asserts both the warning and the effect.

## What it found

These are all fixed in the compiler, and the runs guard against them:

1. **JDBC on Flink 2.x is several jars.** `flink-connector-jdbc-core`, a per-database module, the
   driver, and the OpenLineage clients (`openlineage-java`, `openlineage-sql-java`). The modules
   are thin jars and fail at job-graph time without OpenLineage. The connector catalog listed the
   1.x single jar.
2. **The Postgres CDC source re-snapshotted the whole table after every restore.** This was in its
   default (non-incremental) mode, with Flink CDC 3.7.0-2.2: v2 started from the savepoint and
   exported every row again. A model aggregating a CDC source would double-count after each
   upgrade. The catalog now sets `scan.incremental.snapshot.enabled = true`; that reader resumes
   from the saved LSN.
3. **New operators can need history they will never get.** Adding a model that reads
   `orders_enriched` made the planner build a *second* TemporalJoin for the new branch. It starts
   with an empty versioned table, and the CDC source does not resend old rows, so every
   unchanged customer looks up as `unknown`. `cascade plan` now names such operators and says
   why. Temporal joins weren't even counted as stateful before, because their plan nodes carry
   no `state` entry.
4. **The operator never upgraded the job when only the ConfigMap changed.** It acts on spec
   changes, and Kubernetes updates mounted ConfigMaps in place, so a running v1 could read v2's
   plan on failover. Job ConfigMaps are now content-addressed and referenced from the pod
   template, so applying a new version triggers a savepoint upgrade.
5. **`kind load docker-image` fails on Docker Desktop's containerd image store**, with "content
   digest not found". `k8s.py` loads the runtime image as a single-platform archive.

Connector versions used: Kafka `5.0.0-2.2`, Postgres CDC `3.7.0-2.2`, JDBC `4.1.0-2.2`. None of
them is built for 2.3 yet, and all of them run on Flink 2.3.0.
