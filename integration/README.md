# Integration environment

Runs a pipeline against real systems, using exactly what the tools generate:

| | `run.py` (docker compose) | `k8s.py` (kind + Flink Kubernetes Operator 1.16.1) |
|---|---|---|
| Kafka, Postgres | containers | pods |
| Flink 2.3.0 | session cluster | application mode, managed by the operator |
| Runtime image | built from the Dockerfile `cascade deploy` writes | the same image, loaded into kind |
| Job | `cascade plan` + the ConfigMap from `cascade deploy --plan-dir`, submitted through `SqlRunner` | `kubectl apply -k` of the `cascade deploy` directory |
| Upgrades v1 → v2 → v3 | `flink stop --savepointPath`, then submit with `-s` (strict) | `kubectl apply -k` of each version: the operator stops with a savepoint and restores |

[`pipeline/pipeline.yaml`](pipeline/pipeline.yaml) reads orders from Kafka and customers from
Postgres CDC. It drops bad orders with an expectation, enriches orders with an event-time temporal
join, and computes a windowed revenue and a running LTV. Its sinks are append Kafka, upsert Kafka
and a JDBC upsert into Postgres.

- **v2** adds a model *without* backfill. That's the hazard case: the planner duplicates a temporal
  join for it, and the copy starts empty.
- **v3** adds two models *with* `backfill: true`, rescales from parallelism 1 to 2, and lengthens
  the state TTL from 1d to 2d.

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
- **New-operator hazard:** the plan report warns that the new operators start empty, and suggests
  `backfill: true` on the new model. The test asserts both the warning and the effect.

**The upgrade to v3**
- **Pinning:** every v2 operator keeps its UID, the restore is strict, and the report flags no
  missing history.
- **Rescale:** LTV state is redistributed from parallelism 1 to 2 and continues correctly.
- **Backfill from Kafka:** the per-customer count includes every order since the beginning
  (3, 3, 1), not just the ones after the upgrade.
- **Backfill through CDC:** the backfilled temporal join counts every order exactly once. Which
  segment each lands in depends on timing: with the pipeline's idle timeout, replayed orders can be
  joined before the CDC snapshot arrives. Compose saw 2 unknown / 4 enterprise; kind saw 5 / 1.
  `cascade validate` warns about this (CD372), and the run asserts the warning.

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
   unchanged customer looks up as `unknown`. Temporal joins weren't even counted as stateful
   before, because their plan nodes carry no `state` entry. Now:
   - **The plan report names such operators** whenever one of their sources is being restored.
   - **The fix is `backfill: true`** ([docs](../docs/DSL.md#adding-a-stateful-model-to-a-running-pipeline-backfill)).
     It builds the model's state from dedicated copies of its sources, read from the beginning.
4. **The operator never upgraded the job when only the ConfigMap changed.** It acts on spec
   changes, and Kubernetes updates mounted ConfigMaps in place, so a running v1 could read v2's
   plan on failover. Job ConfigMaps are now content-addressed and referenced from the pod
   template, so applying a new version triggers a savepoint upgrade.
5. **`kind load docker-image` fails on Docker Desktop's containerd image store**, with "content
   digest not found". `k8s.py` loads the runtime image as a single-platform archive.

The upgrade matrix (`tests/test_upgrade_matrix.py`, stand-in connectors, 15 kinds of upgrade)
found more. These are disagreements between `check-upgrade`, the pin report and real restores, all
fixed:
- **Renames were reported as state loss.** State identities no longer depend on names, and the
  diff reports `RENAMED`.
- **A TTL change silently dropped aggregate state** through pinning. The TTL duration is no longer
  part of operator identity.
- **Toggling mini-batch was reported as ok** but re-plans the aggregates. It's now
  `PLANNER_CONFIG_CHANGED`.
- **Replaced stateless sinks** no longer count as state loss.
- **Renaming a model moves its quarantine topic.** That's now reported.

Connector versions used: Kafka `5.0.0-2.2`, Postgres CDC `3.7.0-2.2`, JDBC `4.1.0-2.2`. None of
them is built for 2.3 yet, and all of them run on Flink 2.3.0.
