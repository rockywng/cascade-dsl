"""Typed intermediate representation of a pipeline.

The loader produces these nodes from YAML; the analyzer fills in `RelationInfo`
for every source and model; validation and lowering only ever look at the IR.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .diagnostics import Location
from .types import SqlType

Changelog = Literal["append", "updating"]


@dataclass
class Column:
    name: str
    type: SqlType
    loc: Location | None = None


@dataclass
class EventTime:
    column: str
    max_out_of_orderness_ms: int
    loc: Location | None = None
    idle_timeout_ms: int | None = None  # None: never idle
    expression: str | None = None  # custom WATERMARK expression ('source' = SOURCE_WATERMARK())
    alignment: dict | None = None  # {group, max_drift_ms, update_interval_ms}
    emit: str | None = None  # on_event | on_periodic


@dataclass
class Connection:
    name: str
    type: str
    properties: dict
    loc: Location | None = None
    # type: custom only: connector, artifacts, changelog, sink_modes, requires_primary_key
    custom: dict = field(default_factory=dict)


@dataclass
class Source:
    name: str
    connection: str
    endpoint: dict  # connector-specific keys (topic, table, path, startup, ...)
    format: str | None
    columns: list[Column]
    primary_key: list[str]
    event_time: EventTime | None
    processing_time: str | None
    options: dict  # raw Flink WITH-option passthrough
    description: str | None = None
    loc: Location | None = None
    copy_of: str | None = None  # generated backfill copy of this source
    from_pipeline: dict | None = None  # {path, sink}: read another pipeline's sink (imports.py)


@dataclass
class Expectation:
    name: str
    check: str
    on_violation: Literal["drop", "fail", "quarantine"]
    loc: Location | None = None


@dataclass
class Function:
    """A user-defined scalar function (Java class in a jar, or a plain Python function)."""

    name: str
    language: Literal["java", "python"]
    class_name: str | None = None  # java
    jar: str | None = None  # java: path relative to the project
    file: str | None = None  # python: module file relative to the project
    function: str | None = None  # python: function name in the module
    returns: SqlType | None = None
    deterministic: bool = True
    description: str | None = None
    loc: Location | None = None

    @property
    def module(self) -> str | None:
        import os
        return os.path.splitext(os.path.basename(self.file))[0] if self.file else None


@dataclass
class Scaling:
    """Parallelism settings. The platform autoscaler reads them from the manifest."""

    max_parallelism: int | None = None  # pipeline.max-parallelism: number of key groups, fixed for the state's life
    parallelism: int | None = None  # initial parallelism
    min: int | None = None
    max: int | None = None
    target_utilization: float | None = None  # busy-time fraction the autoscaler aims for
    max_lag_ms: int | None = None  # SLO: source lag the autoscaler must keep below
    autoscale: bool = True
    loc: Location | None = None


@dataclass
class Model:
    name: str
    sql: str
    sql_loc: Location | None  # where the SQL text starts (file + line)
    columns: list[Column] | None  # optional output contract
    expectations: list[Expectation]
    allow_unbounded_state: bool = False
    description: str | None = None
    scaling: Scaling | None = None  # per-model parallelism bounds (min/max only)
    backfill: bool = False  # read own copies of the sources from the beginning (see backfill.py)
    loc: Location | None = None
    copy_of: str | None = None  # generated backfill copy of this model


@dataclass
class Sink:
    name: str
    from_: str
    connection: str
    endpoint: dict
    format: str | None
    mode: Literal["append", "upsert"]
    primary_key: list[str]
    options: dict
    on_conflict: str | None = None  # error | nothing | deduplicate (Flink >= 2.3 upsert sinks)
    description: str | None = None
    loc: Location | None = None


@dataclass
class Quarantine:
    connection: str
    endpoint: dict
    format: str | None
    loc: Location | None = None


@dataclass
class StatefulOp:
    kind: str  # group_agg, window_agg, regular_join, interval_join, temporal_join, dedup, distinct, over_agg
    bounded: bool
    detail: str = ""


@dataclass
class OutputColumn:
    name: str
    type: SqlType | None  # None = not statically inferable
    expr: str = ""


@dataclass
class RelationInfo:
    """Static facts about a source or model, derived by the analyzer."""

    columns: list[OutputColumn] | None  # None = output shape unknown
    rowtime: set[str] = field(default_factory=set)
    changelog: Changelog = "append"
    inputs: list[str] = field(default_factory=list)
    stateful_ops: list[StatefulOp] = field(default_factory=list)
    has_ttl_hint: bool = False
    upsert_key: list[str] | None = None  # statically known unique key of an updating relation

    def column(self, name: str) -> OutputColumn | None:
        for c in self.columns or []:
            if c.name.lower() == name.lower():
                return c
        return None


@dataclass
class Pipeline:
    name: str
    file: str
    root_dir: str
    env: str | None
    description: str | None
    owner: str | None
    state_ttl_ms: int | None
    flink_config: dict
    connections: dict[str, Connection]
    sources: dict[str, Source]
    models: dict[str, Model]
    sinks: dict[str, Sink]
    quarantine: Quarantine | None
    secrets: set[str] = field(default_factory=set)
    scaling: Scaling | None = None
    functions: dict[str, Function] = field(default_factory=dict)
    deploy: dict | None = None  # deployment settings (image, resources, checkpoints, ...), vars resolved
    schema_lock: dict | None = None  # cascade.lock.json "models" section, if present
    mode: str = "streaming"  # streaming | batch
    imports: list = field(default_factory=list)  # [{source, pipeline, sink, path}]
    relations: dict[str, RelationInfo] = field(default_factory=dict)
    loc: Location | None = None

    def relation_names(self) -> set[str]:
        return set(self.sources) | set(self.models)
