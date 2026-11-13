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


def optstr(v) -> str:
    """A WITH option value. Lists are Flink's ';'-separated lists (topics, hosts): rendering a
    Python list as text gave '['a', 'b']', a single topic of that name, so the job read nothing."""
    if isinstance(v, (list, tuple)):
        return ";".join(optstr(x) for x in v)
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


KEY_PROPS = {}  # filled below Prop: the Kafka message key of append sources and sinks


@dataclass
class Prop:
    flink_key: str | None  # None = handled specially by the connector's `finish` hook
    required: bool = False
    default: object = None
    secret: bool = False
    values: dict | None = None  # DSL value -> Flink value (also restricts allowed values)
    doc: str = ""


KEY_PROPS.update({
    "key_fields": Prop("key.fields", doc="columns carried in the message key (append topics; upsert uses primary_key)"),
    "key_format": Prop("key.format", doc="format of the key; default: the value's format"),
    "value_includes_key": Prop("value.fields-include", values={"True": "ALL", "False": "EXCEPT_KEY"},
                               doc="true (default): the value also carries the key fields"),
})


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
    source_changelog: str = "append"  # append | updating | by_format | by_primary_key
    source_requires_pk: bool = False
    artifact: str | None = None
    # connection properties -> extra Maven coordinates (e.g. JDBC dialect module and driver)
    extra_artifacts: Callable | None = None
    doc: str = ""
    # (role, mode, fmt, options, ctx) -> mutate options; return list of (code, message) problems
    finish: Callable | None = None
    # metadata key -> (Flink type, writable by sinks); None = not checked (the planner decides)
    metadata: dict | None = None
    # metadata key prefixes the format contributes (Kafka: 'value.' for debezium-json's fields)
    metadata_prefixes: tuple = ()

    def source_is_changelog(self, fmt: str | None, has_pk: bool = False) -> bool:
        if self.source_changelog == "by_format":
            return fmt in CDC_FORMATS
        if self.source_changelog == "by_primary_key":  # lakehouse tables: keyed tables are upsert tables
            return has_pk
        return self.source_changelog == "updating"


@dataclass
class LowerCtx:
    pipeline: str
    node: str
    connection: str
    primary_key: list[str]


def _kafka_finish(role, mode, fmt, opts, ctx: LowerCtx):
    problems = []
    keyed = "key.fields" in opts
    if (("key.format" in opts or "value.fields-include" in opts) and not keyed):
        problems.append(("CD209", "key_format and value_includes_key need key_fields"))
    if keyed and (mode == "upsert" or opts.get("__changelog") == "upsert"):
        problems.append(("CD224", "key_fields is for append topics; an upsert topic's key is the primary_key"))
    elif keyed:
        # with a key format, Flink names the value format 'value.format'
        opts.setdefault("key.format", opts.get("format") or "json")
        if "format" in opts:
            opts["value.format"] = opts.pop("format")
    if role == "source" and ("topic" in opts) == ("topic-pattern" in opts):
        problems.append(("CD209", "a Kafka source needs exactly one of 'topic' or 'topic_pattern'"))
    if role == "sink" and mode == "upsert":
        opts["connector"] = "upsert-kafka"
        opts.pop("format", None)
        opts["key.format"] = fmt or "json"
        opts["value.format"] = fmt or "json"
        opts.pop("sink.delivery-guarantee", None)
        if fmt in CDC_FORMATS:
            problems.append(("CD214", f"format '{fmt}' cannot be used with upsert mode"))
    if role == "source":
        opts.setdefault("properties.group.id", f"{ctx.pipeline}.{ctx.node}")
        if opts.pop("__changelog", "append") == "upsert":
            # upsert-kafka reads the whole compacted topic as a table; it takes no startup mode
            opts["connector"] = "upsert-kafka"
            f = opts.pop("format", None) or "json"
            opts["key.format"] = opts["value.format"] = f
            opts.pop("scan.startup.mode", None)
    if role == "sink" and opts.get("sink.delivery-guarantee") == "exactly-once":
        # Flink's default transaction timeout (1 h) exceeds the broker's default maximum
        # (transaction.max.timeout.ms = 15 min): the producer is rejected and the job crash-loops
        # (found by integration/soak.py). Transactions must outlive a checkpoint plus a restart.
        opts.setdefault("properties.transaction.timeout.ms", str(15 * 60 * 1000))
        # one stable prefix per sink: transactional ids must not collide across jobs and must stay
        # the same across restarts so pending transactions can be aborted or committed
        opts.setdefault("sink.transactional-id-prefix", f"{ctx.pipeline}.{ctx.node}")
    registry = opts.pop("__schema_registry_url", None)
    if fmt in ("avro-confluent", "debezium-avro-confluent"):
        if not registry:
            problems.append(("CD213", f"format '{fmt}' needs connection property 'schema_registry_url'"))
        elif "key.format" in opts:  # upsert topics and keyed append topics: one URL per part
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
            return [("CD212", f"table must be '<{db_key.split('-')[0]}>.<table>' (got '{full}')")]
        opts[db_key], opts[table_key] = parts
        return []
    return finish


def _pg_finish(role, mode, fmt, opts, ctx: LowerCtx):
    problems = _db_table_finish("schema-name", "table-name")(role, mode, fmt, opts, ctx)
    # replication slots are per-consumer; derive a stable unique name
    opts.setdefault("slot.name", re.sub(r"[^a-z0-9_]", "_", f"{ctx.pipeline}_{ctx.node}".lower())[:63])
    # The legacy (non-incremental) reader re-snapshots the whole table when a job is restored from a
    # savepoint (seen in integration/run.py with Flink CDC 3.7): every upgrade would re-emit every row.
    # The incremental reader resumes from the saved LSN.
    opts.setdefault("scan.incremental.snapshot.enabled", "true")
    if opts.pop("__on_slot_loss", "refuse") == "resnapshot":
        # dev.cascade.cdc.HealingCdcFactory: the CDC source behind a slot-generation tag and a reconcile step
        opts["delegate"] = opts["connector"]
        opts["connector"] = "cascade-healing-cdc"
    return problems


def _jdbc_finish(role, mode, fmt, opts, ctx: LowerCtx):
    # The Postgres driver leaves TCP keepalive off. Cloud NATs and AWS PrivateLink drop a flow idle
    # for 350 s without telling either end, so a sink whose input went quiet that long waited
    # forever on its next write: the job stayed RUNNING, checkpoints stopped, no data moved (found
    # by cascade-dataplane's EKS soak). With keepalive on, the pod's probe timing (Cascade sets it
    # well under 350 s) keeps the flow alive and finds a dead one. MySQL's driver has it on already.
    url = str(opts.get("url") or "")
    if url.startswith("jdbc:postgresql:") and "tcpkeepalive=" not in url.lower():
        opts["url"] = url + ("&" if "?" in url else "?") + "tcpKeepAlive=true"
    return []


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

# Published connector jars that don't work as released, and the fix applied to them (by `cascade
# fetch-jars` locally, and in the Dockerfile `cascade deploy` writes): resource files whose comment
# lines must go. flink-sql-connector-dynamodb 6.0.x-2.0 shades the AWS SDK with an Apache licence header
# added to its execution.interceptors file, which the SDK reads line by line as class names: every
# DynamoDB client creation fails with ClassNotFoundException "# Licensed to the Apache Software
# Foundation..." (found by integration/connectors.py).
PATCHES = {
    "org.apache.flink:flink-sql-connector-dynamodb": [
        "org/apache/flink/connector/dynamodb/shaded/software/amazon/awssdk/services/dynamodb/execution.interceptors",
    ],
}

# Connector versions tested with the Flink version the project targets (2.3). `cascade deploy` uses them
# when the deploy block's `artifacts:` doesn't pin one. Ones marked "smoke" are checked by planning a job
# against the real jar (tests/test_connector_smoke.py); the rest also run end to end in integration/.
TESTED_VERSIONS = {
    "org.apache.flink:flink-sql-connector-kafka": "5.0.0-2.2",
    "org.apache.flink:flink-sql-connector-postgres-cdc": "3.7.0-2.2",
    "org.apache.flink:flink-sql-connector-mysql-cdc": "3.7.0-2.2",           # smoke
    "org.apache.flink:flink-sql-connector-oracle-cdc": "3.7.0-2.2",          # smoke
    "org.apache.flink:flink-sql-connector-sqlserver-cdc": "3.7.0-2.2",       # smoke
    "org.apache.flink:flink-sql-connector-db2-cdc": "3.7.0-2.2",             # smoke
    "org.apache.flink:flink-sql-connector-mongodb-cdc": "3.7.0-2.2",         # smoke
    "org.apache.flink:flink-connector-jdbc-core": "4.1.0-2.2",
    "org.apache.flink:flink-connector-jdbc-postgres": "4.1.0-2.2",
    "org.apache.flink:flink-connector-jdbc-mysql": "4.1.0-2.2",
    "org.postgresql:postgresql": "42.7.14",
    "com.mysql:mysql-connector-j": "9.5.0",
    "org.apache.flink:flink-connector-jdbc-sqlserver": "4.1.0-2.2",
    "org.apache.flink:flink-connector-jdbc-oracle": "4.1.0-2.2",
    "org.apache.flink:flink-connector-jdbc-db2": "4.1.0-2.2",
    "com.microsoft.sqlserver:mssql-jdbc": "13.6.2.jre11",
    "com.oracle.database.jdbc:ojdbc11": "23.26.4.0.0",
    "com.oracle.database.jdbc:ojdbc8": "19.33.0.0",  # the oracle-cdc driver (Debezium 1.9: the 19c line)
    "com.ibm.db2:jcc": "12.1.5.0",
    "org.apache.hadoop:hadoop-client-api": "3.4.2",
    "org.apache.hadoop:hadoop-client-runtime": "3.4.2",
    "io.openlineage:openlineage-java": "1.32.0",
    "io.openlineage:openlineage-sql-java": "1.32.0",
    "org.apache.flink:flink-sql-connector-aws-kinesis-streams": "6.0.1-2.0",  # smoke
    "org.apache.flink:flink-sql-connector-dynamodb": "6.0.1-2.0",             # smoke
    "org.apache.flink:flink-sql-connector-elasticsearch7": "4.0.0-2.0",       # smoke
    "org.apache.paimon:paimon-flink-2.2": "2.1.0",                            # smoke
    "org.apache.hudi:hudi-flink2.0-bundle": "1.2.1",                          # smoke
    "commons-logging:commons-logging": "1.4.0",
    "org.apache.iceberg:iceberg-flink-runtime-2.3": "1.12.0",  # needs Hadoop and a catalog: not smoked
    "org.apache.flink:flink-model-openai": "2.3.0",                           # ML_PREDICT provider, smoke
}


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
        "topic": Prop("topic", doc="a topic, or a list of topics"),
        "topic_pattern": Prop("topic-pattern", doc="a Java regex of topics to read (instead of topic)"),
        "group_id": Prop("properties.group.id"),
        "changelog": Prop("__changelog", values={"append": "append", "upsert": "upsert"},
                          doc="upsert: a compacted topic of keyed rows, read with upsert-kafka"),
        "startup": Prop("scan.startup.mode", default="group", values={
            "earliest": "earliest-offset", "latest": "latest-offset", "group": "group-offsets"}),
        **KEY_PROPS,
    },
    sink={
        "topic": Prop("topic", doc="a topic; with a 'topic' metadata column, the list of topics it may name"),
        **KEY_PROPS,
        "delivery": Prop("sink.delivery-guarantee", default="at-least-once",
                         values={"at-least-once": "at-least-once", "exactly-once": "exactly-once"}),
        "transactional_id_prefix": Prop("sink.transactional-id-prefix"),
    },
    formats=STREAM_FORMATS, default_format="json", sink_modes={"append", "upsert"},
    source_changelog="by_format", artifact="org.apache.flink:flink-sql-connector-kafka",
    finish=_kafka_finish,
))

def _eventhubs_finish(role, mode, fmt, opts, ctx: LowerCtx):
    """Azure Event Hubs speaks the Kafka protocol on port 9093: lower to the Kafka connector."""
    ns = opts.pop("__namespace")
    opts["connector"] = "kafka"
    opts["properties.bootstrap.servers"] = f"{ns}.servicebus.windows.net:9093"
    opts["properties.security.protocol"] = "SASL_SSL"
    opts["properties.sasl.mechanism"] = "PLAIN"
    cs = opts.pop("__connection_string")
    # the secret placeholder is substituted at runtime inside the JAAS string
    opts["properties.sasl.jaas.config"] = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{cs}";')
    # Event Hubs has no Kafka transactions and no log compaction on its Kafka endpoint
    return _kafka_finish(role, mode, fmt, opts, ctx)


register(ConnectorSpec(
    type="eventhubs", roles={"source", "sink"},
    doc="Azure Event Hubs through its Kafka endpoint (at-least-once; no transactions, no compacted topics).",
    connection={
        "namespace": Prop("__namespace", required=True, doc="the Event Hubs namespace, without .servicebus.windows.net"),
        "connection_string": Prop("__connection_string", required=True, secret=True),
        "schema_registry_url": Prop("__schema_registry_url"),
    },
    source={
        "event_hub": Prop("topic", required=True, doc="the event hub (a Kafka topic)"),
        "consumer_group": Prop("properties.group.id", doc="defaults to <pipeline>.<node>"),
        "startup": Prop("scan.startup.mode", default="group", values={
            "earliest": "earliest-offset", "latest": "latest-offset", "group": "group-offsets"}),
        **KEY_PROPS,
    },
    sink={
        "event_hub": Prop("topic", required=True),
        **KEY_PROPS,
        "delivery": Prop("sink.delivery-guarantee", default="at-least-once",
                         values={"at-least-once": "at-least-once", "none": "none"}),
    },
    formats=STREAM_FORMATS, default_format="json", sink_modes={"append", "upsert"},
    source_changelog="by_format", artifact="org.apache.flink:flink-sql-connector-kafka",
    finish=_eventhubs_finish,
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
                "decoding_plugin": Prop("decoding.plugin.name", default="pgoutput"),
                "on_slot_loss": Prop("__on_slot_loss", values={"refuse": "refuse", "resnapshot": "resnapshot"},
                                     doc="when the replication slot is gone on restore: refuse to start, or "
                                         "re-snapshot and reconcile downstream state")}
               if _db == "postgres" else {"server_id": Prop("server-id")}),
        },
        source_changelog="updating", source_requires_pk=True,
        artifact=f"org.apache.flink:flink-sql-connector-{_db}-cdc",
        # Flink CDC's MySQL jar doesn't bundle the driver (GPL): without it the source can't be created
        extra_artifacts=(lambda props: ["com.mysql:mysql-connector-j"]) if _db == "mysql" else None,
        finish=_pg_finish if _db == "postgres" else _db_table_finish("database-name", "table-name"),
    ))

_CDC_COMMON = {
    "hostname": Prop("hostname", required=True),
    "username": Prop("username", required=True),
    "password": Prop("password", required=True, secret=True),
}
_CDC_STARTUP = Prop("scan.startup.mode", default="initial", values={"initial": "initial", "latest": "latest-offset"})


def _incremental(finish=None):
    def f(role, mode, fmt, opts, ctx):
        problems = finish(role, mode, fmt, opts, ctx) if finish else []
        opts.setdefault("scan.incremental.snapshot.enabled", "true")  # resumes from its offset on restore
        return problems
    return f


def _passthrough_table(role, mode, fmt, opts, ctx):
    full = opts.pop("__table", None)
    if full is not None:
        opts["table-name"] = full
    return []


def _oracle_finish(role, mode, fmt, opts, ctx):
    problems = _db_table_finish("schema-name", "table-name")(role, mode, fmt, opts, ctx)
    if opts.get("debezium.database.pdb.name"):
        # Flink CDC 3.7's incremental reader discovers tables over a connection to the container root and
        # finds none in a pluggable database ("db history ... missing"; integration/connectors.py): the
        # legacy reader reads them. LogMiner's online catalog avoids mining the dictionary from redo.
        opts.setdefault("scan.incremental.snapshot.enabled", "false")
        opts.setdefault("debezium.log.mining.strategy", "online_catalog")
    else:
        opts.setdefault("scan.incremental.snapshot.enabled", "true")
    return problems


register(ConnectorSpec(
    type="oracle-cdc", roles={"source"},
    doc="Oracle change data capture via Flink CDC (LogMiner). Emits an updating changelog.",
    connection={**_CDC_COMMON, "port": Prop("port", default=1521), "database": Prop("database-name", required=True),
                "pdb": Prop("debezium.database.pdb.name", doc="the pluggable database, in a multitenant (CDB) setup")},
    source={"table": Prop("__table", required=True, doc="schema.table"), "startup": _CDC_STARTUP},
    source_changelog="updating", source_requires_pk=True,
    artifact="org.apache.flink:flink-sql-connector-oracle-cdc",
    # like MySQL's, the jar doesn't bundle the driver (found by integration/connectors.py: "No suitable driver")
    extra_artifacts=lambda props: ["com.oracle.database.jdbc:ojdbc8"],
    finish=_oracle_finish,
))
for _db, _port, _doc in (("sqlserver", 1433, "SQL Server"), ("db2", 50000, "Db2")):
    register(ConnectorSpec(
        type=f"{_db}-cdc", roles={"source"},
        doc=f"{_doc} change data capture via Flink CDC. Emits an updating changelog.",
        connection={**_CDC_COMMON, "port": Prop("port", default=_port),
                    "database": Prop("database-name", required=True)},
        source={"table": Prop("__table", required=True, doc="schema.table"), "startup": _CDC_STARTUP},
        source_changelog="updating", source_requires_pk=True,
        artifact=f"org.apache.flink:flink-sql-connector-{_db}-cdc",
        # the Db2 jar doesn't bundle IBM's driver (licensing; like MySQL's and Oracle's)
        extra_artifacts=(lambda props: ["com.ibm.db2:jcc"]) if _db == "db2" else None,
        finish=_incremental(_passthrough_table),
    ))
register(ConnectorSpec(
    type="mongodb-cdc", roles={"source"},
    doc="MongoDB change streams via Flink CDC. Emits an updating changelog keyed by _id.",
    connection={"hosts": Prop("hosts", required=True), "username": Prop("username"),
                "password": Prop("password", secret=True), "scheme": Prop("scheme"),
                "connection_options": Prop("connection.options")},
    source={"database": Prop("database", required=True), "collection": Prop("collection", required=True),
            "startup": _CDC_STARTUP},
    source_changelog="updating", source_requires_pk=True,
    artifact="org.apache.flink:flink-sql-connector-mongodb-cdc",
    finish=_incremental(),
))


_AWS_CONNECTION = {
    "region": Prop("aws.region", required=True),
    "endpoint": Prop("aws.endpoint", doc="override, e.g. a VPC endpoint or a local emulator"),
    "access_key_id": Prop("__aws_key", secret=True, doc="static credentials (default: the AWS default chain)"),
    "secret_access_key": Prop("__aws_secret", secret=True),
}


def _aws_finish(role, mode, fmt, opts, ctx):
    key, secret = opts.pop("__aws_key", None), opts.pop("__aws_secret", None)
    if key or secret:
        if not (key and secret):
            return [("CD209", "access_key_id and secret_access_key go together")]
        opts["aws.credentials.provider"] = "BASIC"
        # the SQL option names (the connector maps them to aws.credentials.provider.basic.*)
        opts["aws.credentials.basic.accesskeyid"] = key
        opts["aws.credentials.basic.secretkey"] = secret
    return []


def _kinesis_finish(role, mode, fmt, opts, ctx):
    return _aws_finish(role, mode, fmt, opts, ctx)


register(ConnectorSpec(
    type="kinesis", roles={"source", "sink"},
    doc="Amazon Kinesis Data Streams (connector 6.x, enhanced fan-out off by default).",
    connection=_AWS_CONNECTION,
    source={"stream_arn": Prop("stream.arn", required=True),
            "startup": Prop("source.init.position", default="latest",
                            values={"earliest": "TRIM_HORIZON", "latest": "LATEST"})},
    sink={"stream_arn": Prop("stream.arn", required=True),
          "partition_key": Prop("sink.partitioner", doc="'random' or a column-based partitioner")},
    formats=STREAM_FORMATS - CDC_FORMATS | {"debezium-json"}, default_format="json",
    source_changelog="by_format",
    artifact="org.apache.flink:flink-sql-connector-aws-kinesis-streams",
    finish=_kinesis_finish,
))
register(ConnectorSpec(
    type="dynamodb", roles={"sink"},
    doc="Amazon DynamoDB. Upsert sinks put by primary key and delete on retraction.",
    connection=_AWS_CONNECTION,
    sink={"table": Prop("table-name", required=True)},
    sink_modes={"append", "upsert"},
    artifact="org.apache.flink:flink-sql-connector-dynamodb",
    finish=_aws_finish,
))


def _es_finish(role, mode, fmt, opts, ctx):
    opts["connector"] = "elasticsearch-7"
    return []


register(ConnectorSpec(
    type="elasticsearch", roles={"sink"},
    doc="Elasticsearch 7 (and OpenSearch 1.x, which speaks its API). Upsert sinks use the primary key "
        "as document id.",
    connection={"hosts": Prop("hosts", required=True), "username": Prop("username"),
                "password": Prop("password", secret=True), "path_prefix": Prop("connection.path-prefix")},
    sink={"index": Prop("index", required=True), "delivery": Prop("sink.delivery-guarantee", default="at-least-once",
          values={"at-least-once": "at-least-once", "none": "none"})},
    formats={"json"}, default_format="json", sink_modes={"append", "upsert"},
    artifact="org.apache.flink:flink-sql-connector-elasticsearch7",
    finish=_es_finish,
))


def _lake_finish(streaming: dict):
    def f(role, mode, fmt, opts, ctx):
        base = opts.pop("__base_path", None)
        if base and "path" in opts and "://" not in str(opts["path"]) and not str(opts["path"]).startswith("/"):
            opts["path"] = str(base).rstrip("/") + "/" + str(opts["path"]).lstrip("/")
        if role == "source":
            for k, v in streaming.items():
                opts.setdefault(k, v)
        return []
    return f


def _paimon_finish(role, mode, fmt, opts, ctx):
    """`table: db.table` in a filesystem warehouse -> the table's path; the runner creates the table
    (with the declared columns and key) if it doesn't exist (`-- @@ensure-table`, lower.py)."""
    warehouse = str(opts.pop("__warehouse", "")).rstrip("/")
    full = opts.pop("__table", None)
    opts.pop("__table_options", None)
    if full is not None:
        parts = str(full).split(".")
        if len(parts) != 2:
            return [("CD212", f"table must be '<database>.<table>' (got '{full}')")]
        opts["path"] = f"{warehouse}/{parts[0]}.db/{parts[1]}"
    return []


register(ConnectorSpec(
    type="paimon", roles={"source", "sink"},
    doc="Apache Paimon tables in a filesystem warehouse, created on first use with the declared columns and "
        "key. Keyed tables (primary_key) are upsert tables and read as changelogs.",
    connection={"warehouse": Prop("__warehouse", required=True, doc="file://, s3://, hdfs:// ...")},
    source={"table": Prop("__table", required=True, doc="database.table")},
    sink={"table": Prop("__table", required=True, doc="database.table"),
          "table_options": Prop("__table_options", doc="Paimon table options when it is created, e.g. bucket")},
    sink_modes={"append", "upsert"}, source_changelog="by_primary_key",
    # Paimon publishes one jar per Flink minor; 2.1.0 has none for 2.3 yet, and the 2.2 build runs on 2.3
    artifact="org.apache.paimon:paimon-flink-2.2",
    # Paimon's file IO goes through Hadoop's Configuration even for local and s3:// warehouses
    extra_artifacts=lambda props: ["org.apache.hadoop:hadoop-client-api", "org.apache.hadoop:hadoop-client-runtime",
                                   "commons-logging:commons-logging"],
    finish=_paimon_finish,
))
register(ConnectorSpec(
    type="hudi", roles={"source", "sink"},
    doc="Apache Hudi tables. Sources read the table as a stream of commits.",
    connection={"base_path": Prop("__base_path")},
    source={"path": Prop("path", required=True)},
    sink={"path": Prop("path", required=True),
          "table_type": Prop("table.type", default="cow", values={"cow": "COPY_ON_WRITE", "mor": "MERGE_ON_READ"})},
    sink_modes={"append", "upsert"}, source_changelog="by_primary_key",
    artifact="org.apache.hudi:hudi-flink2.0-bundle",
    # the bundle expects Hadoop (Configuration, FileSystem) and commons-logging on the classpath
    extra_artifacts=lambda props: ["org.apache.hadoop:hadoop-client-api", "org.apache.hadoop:hadoop-client-runtime",
                                   "commons-logging:commons-logging"],
    finish=_lake_finish({"read.streaming.enabled": "true"}),
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
    sink_modes={"append", "upsert"}, # Iceberg publishes one runtime jar per Flink minor version, e.g. iceberg-flink-runtime-2.0
    artifact="org.apache.iceberg:iceberg-flink-runtime-{flink_minor}",
    # the runtime jar expects Hadoop on the classpath (catalogs, FileIO); Flink's image has none
    extra_artifacts=lambda props: ["org.apache.hadoop:hadoop-client-api", "org.apache.hadoop:hadoop-client-runtime"],
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
    # Flink 2.x splits JDBC into a core module plus one module per database; the driver is separate
    sink_modes={"append", "upsert"}, artifact="org.apache.flink:flink-connector-jdbc-core",
    extra_artifacts=lambda props: _jdbc_artifacts(str(props.get("url", ""))),
    finish=_jdbc_finish,
))

JDBC_DIALECTS = {  # url prefix -> (Flink dialect module, driver coordinate)
    "jdbc:postgresql:": ("postgres", "org.postgresql:postgresql"),
    "jdbc:mysql:": ("mysql", "com.mysql:mysql-connector-j"),
    "jdbc:oracle:": ("oracle", "com.oracle.database.jdbc:ojdbc11"),
    "jdbc:sqlserver:": ("sqlserver", "com.microsoft.sqlserver:mssql-jdbc"),
    "jdbc:db2:": ("db2", "com.ibm.db2:jcc"),
}


def _jdbc_artifacts(url: str) -> list[str]:
    for prefix, (dialect, driver) in JDBC_DIALECTS.items():
        if url.startswith(prefix):
            # the 4.x modules are thin jars: their lineage support needs the OpenLineage clients at runtime
            return [f"org.apache.flink:flink-connector-jdbc-{dialect}", driver,
                    "io.openlineage:openlineage-java", "io.openlineage:openlineage-sql-java"]
    return []


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


register(ConnectorSpec(
    type="custom", roles={"source", "sink"},
    doc="Any Flink SQL connector the catalog doesn't know: `connector` names its factory, `artifacts` "
        "lists its jars (group:artifact:version), `properties` and source/sink keys are raw WITH options.",
))

# --- metadata columns (METADATA FROM '...'), from each connector's documentation ----------

KAFKA_METADATA = {
    "topic": ("STRING NOT NULL", True), "partition": ("INT NOT NULL", False),
    "headers": ("MAP<STRING, BYTES> NOT NULL", True), "leader-epoch": ("INT", False),
    "offset": ("BIGINT NOT NULL", False), "timestamp": ("TIMESTAMP_LTZ(3) NOT NULL", True),
    "timestamp-type": ("STRING NOT NULL", False),
}
for _t in ("kafka", "eventhubs"):
    CATALOG[_t].metadata = KAFKA_METADATA
    CATALOG[_t].metadata_prefixes = ("value.", "key.")
_CDC_COMMON = {"table_name": ("STRING NOT NULL", False), "database_name": ("STRING NOT NULL", False),
               "op_ts": ("TIMESTAMP_LTZ(3) NOT NULL", False)}
CATALOG["postgres-cdc"].metadata = {**_CDC_COMMON, "schema_name": ("STRING NOT NULL", False),
                                    "row_kind": ("STRING NOT NULL", False)}
CATALOG["mysql-cdc"].metadata = {**_CDC_COMMON, "row_kind": ("STRING NOT NULL", False)}
CATALOG["oracle-cdc"].metadata = {**_CDC_COMMON, "schema_name": ("STRING NOT NULL", False)}
CATALOG["sqlserver-cdc"].metadata = {**_CDC_COMMON, "schema_name": ("STRING NOT NULL", False)}
CATALOG["db2-cdc"].metadata = {**_CDC_COMMON, "schema_name": ("STRING NOT NULL", False)}
CATALOG["mongodb-cdc"].metadata = {"database_name": ("STRING NOT NULL", False),
                                   "collection_name": ("STRING NOT NULL", False),
                                   "op_ts": ("TIMESTAMP_LTZ(3) NOT NULL", False),
                                   "row_kind": ("STRING NOT NULL", False)}
CATALOG["filesystem"].metadata = {"file.path": ("STRING NOT NULL", False), "file.name": ("STRING NOT NULL", False),
                                  "file.size": ("BIGINT NOT NULL", False),
                                  "file.modification-time": ("TIMESTAMP_LTZ(3) NOT NULL", False)}
for _t in ("jdbc", "datagen", "print", "blackhole", "elasticsearch", "dynamodb"):
    CATALOG[_t].metadata = {}


SECRET_KEY_RE = re.compile(r"(password|secret|token|credential|private[._-]?key|jaas)", re.I)


def spec_for(conn_type: str, conn=None) -> ConnectorSpec | None:
    """The catalog entry for a connection; a `custom` connection gets a spec built from its settings."""
    base = CATALOG.get(conn_type)
    if conn_type != "custom" or conn is None:
        return base
    cu = conn.custom
    arts = [str(a) for a in cu.get("artifacts") or []]
    return ConnectorSpec(
        type="custom", roles={"source", "sink"}, formats=None,
        sink_modes=set(cu.get("sink_modes") or ["append"]),
        source_changelog=str(cu.get("changelog", "append")),
        source_requires_pk=bool(cu.get("requires_primary_key", False)),
        extra_artifacts=lambda props, arts=arts: arts,
        doc=base.doc if base else "",
    )


def _custom_options(conn, role, connection_props, endpoint, fmt, passthrough):
    problems = []
    opts = {"connector": str(conn.custom.get("connector", ""))}
    for k, v in {**connection_props, **endpoint}.items():
        if SECRET_KEY_RE.search(str(k)) and not SECRET_RE.match(str(v)):
            problems.append(("CD201", f"'{k}' looks like a secret and must be a ${{secret:...}} reference, "
                             "not a literal value", k))
        opts[str(k)] = v
    if fmt:
        opts["format"] = fmt
    opts.update(passthrough)
    return {k: optstr(v) for k, v in opts.items()}, problems


def build_options(spec: ConnectorSpec, role: str, connection_props: dict, endpoint: dict,
                  fmt: str | None, mode: str, ctx: LowerCtx, passthrough: dict,
                  conn=None) -> tuple[dict, list]:
    """Map DSL properties to Flink WITH options. Returns (options, [(code, message, key)])."""
    if spec.type == "custom":
        return _custom_options(conn, role, connection_props, endpoint, fmt, passthrough)
    problems: list[tuple[str, str, str | None]] = []
    opts: dict = {"connector": spec.type}

    def apply(schema: dict[str, Prop], values: dict, where: str):
        for key, value in values.items():
            if key not in schema:
                import difflib
                close = difflib.get_close_matches(key, list(schema), n=1)
                problems.append(("CD210", f"unknown {where} property '{key}' for connector '{spec.type}'"
                                 + (f" (did you mean '{close[0]}'?)" if close else
                                    f"; supported: {', '.join(schema) or '(none)'}"), key))
                continue
            p = schema[key]
            if p.secret and not SECRET_RE.match(str(value)):
                problems.append(("CD201", f"'{key}' is a secret and must be a ${{secret:...}} reference, "
                                 "not a literal value", key))
            if p.values is not None:
                if str(value) not in p.values:
                    problems.append(("CD211", f"invalid value '{value}' for '{key}'; expected one of "
                                     + ", ".join(p.values), key))
                    continue
                value = p.values[str(value)]
            if p.flink_key:
                opts[p.flink_key] = value
        for key, p in schema.items():
            if key in values:
                continue
            if p.required:
                problems.append(("CD209", f"missing required {where} property '{key}' for connector "
                                 f"'{spec.type}'" + (f" ({p.doc})" if p.doc else ""), None))
            elif p.default is not None and p.flink_key:
                opts[p.flink_key] = p.values[p.default] if p.values else p.default

    apply(spec.connection, connection_props, "connection")
    apply(spec.source if role == "source" else spec.sink, endpoint, role)

    if spec.formats is not None:
        f = fmt or spec.default_format
        if f not in spec.formats:
            problems.append(("CD212", f"format '{f}' is not supported by connector '{spec.type}'; "
                             f"supported: {', '.join(sorted(spec.formats))}", "format"))
        opts["format"] = f
    elif fmt:
        problems.append(("CD212", f"connector '{spec.type}' does not take a 'format'", "format"))

    if spec.finish:
        for code, msg in spec.finish(role, mode, opts.get("format", fmt), opts, ctx):
            problems.append((code, msg, None))
    opts = {k: v for k, v in opts.items() if not k.startswith("__")}
    for k, v in passthrough.items():
        opts[k] = v
    return {k: optstr(v) for k, v in opts.items()}, problems
