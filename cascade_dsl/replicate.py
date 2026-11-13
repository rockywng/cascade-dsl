"""Whole-database replication: `replicate:` compiles to a Flink CDC pipeline definition.

A replication copies many tables (selected by pattern) from a CDC source into a lake or Kafka, creating
each sink table from the source schema and applying schema changes (added columns, new tables) as they
happen. Flink SQL can't do that: a SQL job has a fixed set of tables with fixed columns. Flink CDC's
pipeline runtime can, so the compiler writes its YAML (`cdc-pipeline.yaml`) and the runtime jar's
dev.cascade.runner.CdcRunner runs it: secrets substituted, the Postgres slot guard, then Flink CDC's
composer on the job's own execution environment.

    replicate:
      from: crm                       # postgres-cdc | mysql-cdc connection
      tables: [public.orders, "public.customer_*"]
      exclude: [public.audit_log]
      to: lake                        # paimon | kafka connection
      schema_changes: evolve          # evolve | try_evolve | lenient | ignore | exception
      route: [{from: "public.orders_*", to: shop.orders}]   # merge or rename tables
      table_options: {bucket: "4"}    # paimon: options of the tables it creates
"""
from __future__ import annotations

import json
import re

from .connectors import SECRET_RE, optstr

CDC_VERSION = "3.7.0"
CDC_FLINK = "2.2"  # Flink CDC builds for Flink 2.2; its jars run on 2.3 (integration/connectors.py)
CDC_DIST_URL = (f"https://archive.apache.org/dist/flink/flink-cdc-{CDC_VERSION}/"
                f"flink-cdc-{CDC_VERSION}-{CDC_FLINK}-bin.tar.gz")
CDC_DIST_SHA512 = "112683a74bb0395f162efc2c79986d6c951812731b5f7f4669f3025b0e8f812ace592e262379a5bea1f49d28c71e0604102ec02d7bcf7f281515c5da4e1248ab"
SOURCES = {"postgres-cdc": "postgres", "mysql-cdc": "mysql"}
SINKS = {"paimon": "paimon", "kafka": "kafka"}
SCHEMA_CHANGES = ("evolve", "try_evolve", "lenient", "ignore", "exception")
KEYS = {"from", "to", "tables", "exclude", "schema_changes", "route", "table_options", "parallelism",
        "slot_name", "source_options", "sink_options"}


def artifact(kind: str) -> str:
    return f"org.apache.flink:flink-cdc-pipeline-connector-{kind}"


def pattern(table: str, prefix: str | None) -> str:
    """`schema.table` with `*` globs -> Flink CDC's pattern (dot-delimited regex), with the Postgres
    database prepended."""
    parts = [re.escape(x).replace(r"\*", r"\.*") for x in table.split(".")]
    return ".".join(([prefix] if prefix else []) + parts)


def slot_name(p) -> str:
    return re.sub(r"[^a-z0-9_]", "_", f"cascade_{p.name}".lower())[:63]


def check(c) -> None:
    p, d = c.pipeline, c.diagnostics
    r = p.replicate
    loc = r.get("__loc")
    if p.sources or p.models or p.sinks:
        d.error("CD601", "a replication pipeline has no sources, models or sinks: it copies whole tables", loc,
                hint="put SQL pipelines in their own project; read the replicated tables from there")
    src, dst = p.connections.get(str(r.get("from"))), p.connections.get(str(r.get("to")))
    if src is None or src.type not in SOURCES:
        d.error("CD602", f"replicate.from must be a {' or '.join(SOURCES)} connection (got '{r.get('from')}')", loc)
    if dst is None or dst.type not in SINKS:
        d.error("CD602", f"replicate.to must be a {' or '.join(SINKS)} connection (got '{r.get('to')}')", loc)
    tables = r.get("tables")
    if not tables or not isinstance(tables, (list, str)):
        d.error("CD005", "replicate.tables is required: table names or patterns (schema.table, * as a glob)", loc)
    if str(r.get("schema_changes", "evolve")) not in SCHEMA_CHANGES:
        d.error("CD014", f"replicate.schema_changes must be one of {', '.join(SCHEMA_CHANGES)}", loc)
    for k in (src.properties if src else {}).keys() | (dst.properties if dst else {}).keys():
        v = ((src.properties if src else {}) | (dst.properties if dst else {})).get(k)
        if "password" in str(k) and not SECRET_RE.match(str(v)):
            d.error("CD201", f"'{k}' is a secret and must be a ${{secret:...}} reference", loc)
    if p.mode == "batch":
        d.error("CD601", "replication is streaming only", loc)
    for kind in ([SOURCES.get(src.type)] if src else []) + ([SINKS.get(dst.type)] if dst else []):
        if kind:
            c.extra_artifacts.add(artifact(kind))
    if src is not None and src.type == "mysql-cdc":
        c.extra_artifacts.add("com.mysql:mysql-connector-j")
    if dst is not None and dst.type == "paimon":
        c.extra_artifacts |= {"org.apache.hadoop:hadoop-client-api", "org.apache.hadoop:hadoop-client-runtime",
                              "commons-logging:commons-logging"}


def definition(p) -> dict:
    """The Flink CDC pipeline definition (YAML document) for `replicate:`."""
    r = p.replicate
    src, dst = p.connections[str(r["from"])], p.connections[str(r["to"])]
    sp, dp = src.properties, dst.properties
    kind = SOURCES[src.type]
    db = sp.get("database") if kind == "postgres" else None
    tables = [r["tables"]] if isinstance(r["tables"], str) else list(r["tables"])
    source = {"type": kind, "hostname": sp["hostname"], "port": int(sp.get("port", 5432 if kind == "postgres" else 3306)),
              "username": sp["username"], "password": sp["password"],
              "tables": ",".join(pattern(t, db) for t in tables),
              "schema-change.enabled": True}
    if r.get("exclude"):
        ex = [r["exclude"]] if isinstance(r["exclude"], str) else list(r["exclude"])
        source["tables.exclude"] = ",".join(pattern(t, db) for t in ex)
    if kind == "postgres":
        source["slot.name"] = r.get("slot_name") or slot_name(p)
        source["decoding.plugin.name"] = "pgoutput"
    source.update(r.get("source_options") or {})
    if dst.type == "paimon":
        sink = {"type": "paimon", "catalog.properties.metastore": "filesystem",
                "catalog.properties.warehouse": dp["warehouse"]}
        for k, v in (r.get("table_options") or {}).items():
            sink[f"table.properties.{k}"] = optstr(v)
    else:
        sink = {"type": "kafka", "properties.bootstrap.servers": dp["bootstrap_servers"],
                "value.format": "debezium-json"}
        for k in ("security_protocol", "sasl_mechanism", "sasl_jaas_config"):
            if dp.get(k):
                sink["properties." + k.replace("_", ".")] = dp[k]
    sink.update(r.get("sink_options") or {})
    doc = {"source": source, "sink": sink}
    routes = []
    for rt in r.get("route") or []:
        routes.append({"source-table": pattern(str(rt["from"]), db), "sink-table": str(rt["to"])})
    if routes:
        doc["route"] = routes
    doc["pipeline"] = {"name": p.name, "parallelism": int(r.get("parallelism", 1)),
                       "schema.change.behavior": str(r.get("schema_changes", "evolve")).upper()}
    return doc


def guard(p) -> str | None:
    """The Postgres slot guard (dev.cascade.runner.Guards), as for SQL CDC sources: on restore, refuse to
    start when the replication slot is gone; Flink CDC would silently take a new one and skip the gap."""
    r = p.replicate
    src = p.connections[str(r["from"])]
    if src.type != "postgres-cdc":
        return None
    sp = src.properties
    spec = {"source": "replicate", "slot": r.get("slot_name") or slot_name(p), "hostname": sp["hostname"],
            "port": str(sp.get("port", 5432)), "database": sp["database"], "username": sp["username"],
            "plugin": "pgoutput", "on_slot_loss": "refuse"}
    pw = str(sp["password"])
    if SECRET_RE.match(pw):
        from .deploy import env_name
        spec["password_env"] = env_name(pw[len("${secret:"):-1])
    else:
        spec["password"] = pw
    return "-- @@require-slot " + json.dumps(spec, sort_keys=True)


def render(p) -> str:
    import yaml

    class Dumper(yaml.SafeDumper):
        pass

    def string(dumper, value):
        # secrets in single quotes: the runner substitutes values with ' doubled, as in SQL literals
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="'" if "${secret:" in value else None)
    Dumper.add_representer(str, string)
    head = (f"# Flink CDC pipeline generated by cascade for {p.name}. Do not edit.\n"
            "# ${secret:...} placeholders are filled in by dev.cascade.runner.CdcRunner at startup.\n")
    return head + yaml.dump(definition(p), Dumper=Dumper, sort_keys=False, default_flow_style=False, width=120)
