"""Connector catalog.

Every connector is a declarative descriptor: which DSL keys it accepts, how they map
to Flink WITH options, which formats and sink modes it supports, whether it emits a
changelog, and which artifact the runtime image must contain. Validation, lowering
and docs are all driven from these descriptors, so adding a connector is data, not code.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

SECRET_RE = re.compile(r"^\$\{secret:[^}]+\}$")
CDC_FORMATS = {"debezium-json", "canal-json", "maxwell-json", "debezium-avro-confluent", "ogg-json"}
STREAM_FORMATS = {"json", "avro", "avro-confluent", "csv", "raw", "protobuf"} | CDC_FORMATS
FILE_FORMATS = {"json", "csv", "parquet", "avro", "orc", "raw", "debezium-json", "canal-json"}


@dataclass
class Prop:
    flink_key: str | None  # None = handled specially by the connector's `finish` hook
    required: bool = False
    default: object = None
    secret: bool = False
    values: dict | None = None  # DSL value -> Flink value (also restricts allowed values)
    doc: str = ""


@dataclass
class ConnectorSpec:
    type: str
    roles: set
    connection: dict[str, Prop] = field(default_factory=dict)
    source: dict[str, Prop] = field(default_factory=dict)
    sink: dict[str, Prop] = field(default_factory=dict)
    formats: set | None = None  # None = connector has no 'format' option
    default_format: str | None = None
    sink_modes: set = field(default_factory=lambda: {"append"})
    source_changelog: str = "append"  # append | updating | by_format
    source_requires_pk: bool = False
    artifact: str | None = None
    doc: str = ""
    # (role, mode, fmt, options, ctx) -> mutate options; return list of (code, message) problems
    finish: Callable | None = None

    def source_is_changelog(self, fmt: str | None) -> bool:
        if self.source_changelog == "by_format":
            return fmt in CDC_FORMATS
        return self.source_changelog == "updating"


@dataclass
class LowerCtx:
    pipeline: str
    node: str
    connection: str
    primary_key: list[str]


def _kafka_finish(role, mode, fmt, opts, ctx: LowerCtx):
    problems = []
    if role == "sink" and mode == "upsert":
        opts["connector"] = "upsert-kafka"
        opts.pop("format", None)
        opts["key.format"] = fmt or "json"
        opts["value.format"] = fmt or "json"
        opts.pop("sink.delivery-guarantee", None)
        if fmt in CDC_FORMATS:
            problems.append(("PD214", f"format '{fmt}' cannot be used with upsert mode"))
    if role == "source":
        opts.setdefault("properties.group.id", f"{ctx.pipeline}.{ctx.node}")
    registry = opts.pop("__schema_registry_url", None)
    if fmt in ("avro-confluent", "debezium-avro-confluent"):
        if not registry:
            problems.append(("PD213", f"format '{fmt}' needs connection property 'schema_registry_url'"))
        elif role == "sink" and mode == "upsert":
            opts[f"key.{fmt}.url"] = registry
            opts[f"value.{fmt}.url"] = registry
        else:
            opts[f"{fmt}.url"] = registry
    return problems


def _db_table_finish(db_key, table_key):
    def finish(role, mode, fmt, opts, ctx: LowerCtx):
        full = opts.pop("__table", None)
        if full is None:
            return []
        parts = str(full).split(".")
        if len(parts) != 2:
            return [("PD212", f"table must be '<{db_key.split('-')[0]}>.<table>' (got '{full}')")]
        opts[db_key], opts[table_key] = parts
        return []
    return finish


def _pg_finish(role, mode, fmt, opts, ctx: LowerCtx):
    problems = _db_table_finish("schema-name", "table-name")(role, mode, fmt, opts, ctx)
    # replication slots are per-consumer; derive a stable unique name
    opts.setdefault("slot.name", re.sub(r"[^a-z0-9_]", "_", f"{ctx.pipeline}_{ctx.node}".lower())[:63])
    return problems


def _iceberg_finish(role, mode, fmt, opts, ctx: LowerCtx):
    problems = _db_table_finish("catalog-database", "catalog-table")(role, mode, fmt, opts, ctx)
    if role == "sink" and mode == "upsert":
        opts["write.upsert.enabled"] = "true"
        opts.setdefault("format-version", "2")
    if role == "source":
        opts.setdefault("streaming", "true")
        opts.setdefault("monitor-interval", "30s")
    opts.setdefault("catalog-name", ctx.connection)
    return problems


def _fs_finish(role, mode, fmt, opts, ctx: LowerCtx):
    base = opts.pop("__base_path", None)
    if base and "path" in opts and "://" not in str(opts["path"]) and not str(opts["path"]).startswith("/"):
        opts["path"] = str(base).rstrip("/") + "/" + str(opts["path"]).lstrip("/")
    return []


CATALOG: dict[str, ConnectorSpec] = {}


def register(spec: ConnectorSpec):
    CATALOG[spec.type] = spec


register(ConnectorSpec(
    type="kafka", roles={"source", "sink"},
    doc="Apache Kafka (also MSK, Confluent Cloud, Redpanda). Upsert sinks use upsert-kafka.",
    connection={
        "bootstrap_servers": Prop("properties.bootstrap.servers", required=True),
        "security_protocol": Prop("properties.security.protocol"),
        "sasl_mechanism": Prop("properties.sasl.mechanism"),
        "sasl_jaas_config": Prop("properties.sasl.jaas.config", secret=True),
        "schema_registry_url": Prop("__schema_registry_url"),
    },
    source={
        "topic": Prop("topic", required=True),
        "group_id": Prop("properties.group.id"),
        "startup": Prop("scan.startup.mode", default="group", values={
            "earliest": "earliest-offset", "latest": "latest-offset", "group": "group-offsets"}),
    },
    sink={
        "topic": Prop("topic", required=True),
        "delivery": Prop("sink.delivery-guarantee", default="at-least-once",
                         values={"at-least-once": "at-least-once", "exactly-once": "exactly-once"}),
        "transactional_id_prefix": Prop("sink.transactional-id-prefix"),
    },
    formats=STREAM_FORMATS, default_format="json", sink_modes={"append", "upsert"},
    source_changelog="by_format", artifact="org.apache.flink:flink-sql-connector-kafka",
    finish=_kafka_finish,
))

for _db, _port in (("postgres", 5432), ("mysql", 3306)):
    register(ConnectorSpec(
        type=f"{_db}-cdc", roles={"source"},
        doc=f"{_db.title()} change data capture via Flink CDC. Emits an updating changelog keyed by primary key.",
        connection={
            "hostname": Prop("hostname", required=True),
            "port": Prop("port", default=_port),
            "username": Prop("username", required=True),
            "password": Prop("password", required=True, secret=True),
            **({"database": Prop("database-name", required=True)} if _db == "postgres" else {}),
        },
        source={
            "table": Prop("__table", required=True,
                          doc="schema.table" if _db == "postgres" else "database.table"),
            "startup": Prop("scan.startup.mode", default="initial",
                            values={"initial": "initial", "latest": "latest-offset"}),
            **({"slot_name": Prop("slot.name"),
                "decoding_plugin": Prop("decoding.plugin.name", default="pgoutput")}
               if _db == "postgres" else {"server_id": Prop("server-id")}),
        },
        source_changelog="updating", source_requires_pk=True,
        artifact=f"org.apache.flink:flink-sql-connector-{_db}-cdc",
        finish=_pg_finish if _db == "postgres" else _db_table_finish("database-name", "table-name"),
    ))

register(ConnectorSpec(
    type="iceberg", roles={"source", "sink"},
    doc="Apache Iceberg tables. Upsert sinks write equality deletes (format v2).",
    connection={
        "catalog_type": Prop("catalog-type", required=True,
                             values={"rest": "rest", "hive": "hive", "hadoop": "hadoop", "glue": "glue"}),
        "catalog_name": Prop("catalog-name"),
        "uri": Prop("uri"),
        "warehouse": Prop("warehouse"),
    },
    source={"table": Prop("__table", required=True, doc="database.table")},
    sink={"table": Prop("__table", required=True, doc="database.table")},
    sink_modes={"append", "upsert"}, artifact="org.apache.iceberg:iceberg-flink-runtime",
    finish=_iceberg_finish,
))

register(ConnectorSpec(
    type="jdbc", roles={"source", "sink"},
    doc="JDBC databases. Upsert mode uses the primary key for INSERT .. ON CONFLICT/MERGE.",
    connection={
        "url": Prop("url", required=True),
        "username": Prop("username"),
        "password": Prop("password", secret=True),
    },
    source={"table": Prop("table-name", required=True)},
    sink={"table": Prop("table-name", required=True),
          "buffer_rows": Prop("sink.buffer-flush.max-rows")},
    sink_modes={"append", "upsert"}, artifact="org.apache.flink:flink-connector-jdbc",
))

register(ConnectorSpec(
    type="filesystem", roles={"source", "sink"},
    doc="Files on local disk or object storage (s3://, gs://, abfs://).",
    connection={"base_path": Prop("__base_path")},
    source={"path": Prop("path", required=True)},
    sink={"path": Prop("path", required=True)},
    formats=FILE_FORMATS, default_format="json", source_changelog="by_format",
    finish=_fs_finish,
))

register(ConnectorSpec(
    type="datagen", roles={"source"},
    doc="Random data generator for development and load tests.",
    source={"rows_per_second": Prop("rows-per-second"), "number_of_rows": Prop("number-of-rows")},
))

register(ConnectorSpec(type="print", roles={"sink"}, sink_modes={"append", "upsert"},
                       doc="Writes rows to TaskManager stdout (development only)."))
register(ConnectorSpec(type="blackhole", roles={"sink"}, sink_modes={"append", "upsert"},
                       doc="Discards all rows (benchmarks)."))


def build_options(spec: ConnectorSpec, role: str, connection_props: dict, endpoint: dict,
                  fmt: str | None, mode: str, ctx: LowerCtx, passthrough: dict) -> tuple[dict, list]:
    """Map DSL properties to Flink WITH options. Returns (options, [(code, message, key)])."""
    problems: list[tuple[str, str, str | None]] = []
    opts: dict = {"connector": spec.type}

    def apply(schema: dict[str, Prop], values: dict, where: str):
        for key, value in values.items():
            if key not in schema:
                import difflib
                close = difflib.get_close_matches(key, list(schema), n=1)
                problems.append(("PD210", f"unknown {where} property '{key}' for connector '{spec.type}'"
                                 + (f" (did you mean '{close[0]}'?)" if close else
                                    f"; supported: {', '.join(schema) or '(none)'}"), key))
                continue
            p = schema[key]
            if p.secret and not SECRET_RE.match(str(value)):
                problems.append(("PD201", f"'{key}' is a secret and must be a ${{secret:...}} reference, "
                                 "not a literal value", key))
            if p.values is not None:
                if str(value) not in p.values:
                    problems.append(("PD211", f"invalid value '{value}' for '{key}'; expected one of "
                                     + ", ".join(p.values), key))
                    continue
                value = p.values[str(value)]
            if p.flink_key:
                opts[p.flink_key] = value
        for key, p in schema.items():
            if key in values:
                continue
            if p.required:
                problems.append(("PD209", f"missing required {where} property '{key}' for connector "
                                 f"'{spec.type}'" + (f" ({p.doc})" if p.doc else ""), None))
            elif p.default is not None and p.flink_key:
                opts[p.flink_key] = p.values[p.default] if p.values else p.default

    apply(spec.connection, connection_props, "connection")
    apply(spec.source if role == "source" else spec.sink, endpoint, role)

    if spec.formats is not None:
        f = fmt or spec.default_format
        if f not in spec.formats:
            problems.append(("PD212", f"format '{f}' is not supported by connector '{spec.type}'; "
                             f"supported: {', '.join(sorted(spec.formats))}", "format"))
        opts["format"] = f
    elif fmt:
        problems.append(("PD212", f"connector '{spec.type}' does not take a 'format'", "format"))

    if spec.finish:
        for code, msg in spec.finish(role, mode, opts.get("format", fmt), opts, ctx):
            problems.append((code, msg, None))
    opts = {k: v for k, v in opts.items() if not k.startswith("__")}
    for k, v in passthrough.items():
        opts[k] = v
    return {k: str(v) for k, v in opts.items()}, problems
