"""Semantic analysis: resolve the dependency graph, infer schemas / changelog modes /
time attributes for every relation, and run the validation rules.

Diagnostic code ranges:
  CD0xx structure (loader)      CD1xx names & sources       CD2xx connectors & secrets
  CD3xx models & expectations   CD35x state                 CD4xx sinks
  CD5xx pipeline-level
"""
from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field

from .connectors import CATALOG, ConnectorSpec, LowerCtx, build_options, spec_for
from .diagnostics import Diagnostics, Location
from .ir import OutputColumn, Pipeline, RelationInfo, StatefulOp
from .sqlscan import (Analysis, Ref, SqlSyntaxError, analyze_sql, infer_relation, line_of,
                      referenced_columns)
from .types import TypeParseError, parse_type
from .util import h, normalize_sql

UNBOUNDED_KINDS = {"group_agg", "regular_join", "dedup", "distinct", "over_agg"}


@dataclass
class Checked:
    """Everything later stages need: the pipeline with relations filled in, plus per-model facts."""

    pipeline: Pipeline
    diagnostics: Diagnostics
    order: list[str] = field(default_factory=list)  # models in topological order
    analyses: dict[str, Analysis] = field(default_factory=dict)
    options: dict[str, dict] = field(default_factory=dict)  # node -> Flink WITH options
    implicit_connections: set[str] = field(default_factory=set)
    inferred: dict[str, list | None] = field(default_factory=dict)  # model -> columns before contract
    settings: dict[str, str] = field(default_factory=dict)  # job-wide SETs derived by the compiler
    extra_artifacts: set = field(default_factory=set)  # jars needed beyond the connections' own


def _suggest(name, candidates) -> str | None:
    m = difflib.get_close_matches(name, sorted(candidates), n=1)
    return f"did you mean '{m[0]}'?" if m else None


def check(p: Pipeline, diags: Diagnostics | None = None) -> Checked:
    d = diags or Diagnostics()
    c = Checked(p, d)
    from . import backfill
    backfill.apply(p, d)
    _check_namespace(c)
    _check_functions(c)
    _check_connections(c)
    _check_sources(c)
    _check_idle_timeouts(c)
    _check_watermark_options(c)
    _check_models(c)
    _check_sinks(c)
    _check_quarantine(c)
    _check_pipeline(c)
    _check_scaling(c)
    _check_batch(c)
    return c


# --- names / connections ---------------------------------------------------------------

def _check_namespace(c: Checked):
    p, d = c.pipeline, c.diagnostics
    seen: dict[str, str] = {}
    for kind, nodes in (("source", p.sources), ("model", p.models), ("sink", p.sinks)):
        for name, node in nodes.items():
            if name in seen:
                d.error("CD101", f"name '{name}' is used by both a {seen[name]} and a {kind}", node.loc,
                        hint="sources, models and sinks share one namespace", node=name)
            seen[name] = kind
            if "__" in name and getattr(node, "copy_of", None) is None:
                d.error("CD101", f"name '{name}' must not contain '__' (reserved for generated objects)",
                        node.loc, node=name)


def _connector(c: Checked, conn_name: str, loc, node: str) -> tuple[ConnectorSpec | None, dict]:
    p, d = c.pipeline, c.diagnostics
    if conn_name in p.connections:
        conn = p.connections[conn_name]
        return spec_for(conn.type, conn), conn.properties
    # built-in zero-config connectors may be referenced directly (connection: datagen)
    spec = CATALOG.get(conn_name)
    if spec is not None and not spec.connection:
        c.implicit_connections.add(conn_name)
        return spec, {}
    d.error("CD202", f"unknown connection '{conn_name}'", loc, node=node,
            hint=_suggest(conn_name, p.connections) or "declare it under `connections:`")
    return None, {}


def _check_connections(c: Checked):
    for conn in c.pipeline.connections.values():
        if conn.type not in CATALOG:
            c.diagnostics.error("CD200", f"unknown connector type '{conn.type}' for connection '{conn.name}'",
                                conn.loc, hint=_suggest(conn.type, CATALOG) or
                                f"available: {', '.join(sorted(CATALOG))}")


def _report_option_problems(c: Checked, problems, loc, node):
    for code, msg, _key in problems:
        c.diagnostics.error(code, f"{node}: {msg}", loc, node=node)


# --- sources ---------------------------------------------------------------------------

def _check_sources(c: Checked):
    p, d = c.pipeline, c.diagnostics
    for s in p.sources.values():
        spec, props = _connector(c, s.connection, s.loc, s.name)
        cols = {col.name: col for col in s.columns}
        if len(cols) != len(s.columns):
            d.error("CD105", f"source '{s.name}' declares a column twice", s.loc, node=s.name)
        rowtime: set[str] = set()
        out_cols = [OutputColumn(col.name, col.type) for col in s.columns]
        if s.event_time:
            et = cols.get(s.event_time.column)
            if et is None:
                d.error("CD102", f"event_time column '{s.event_time.column}' is not a column of source '{s.name}'",
                        s.event_time.loc, hint=_suggest(s.event_time.column, cols), node=s.name)
            elif not et.type.is_temporal:
                d.error("CD103", f"event_time column '{et.name}' must be TIMESTAMP(p) or TIMESTAMP_LTZ(p), "
                        f"got {et.type}", s.event_time.loc,
                        hint="convert it with a computed column in a model, or change the declared type",
                        node=s.name)
            else:
                rowtime.add(et.name)
        if s.processing_time:
            if s.processing_time in cols:
                d.error("CD105", f"processing_time column '{s.processing_time}' collides with a declared column",
                        s.loc, node=s.name)
            out_cols.append(OutputColumn(s.processing_time, parse_type("TIMESTAMP_LTZ(3) NOT NULL")))
            rowtime.add(s.processing_time)
        for k in s.primary_key:
            if k not in cols:
                d.error("CD106", f"primary_key column '{k}' is not a column of source '{s.name}'", s.loc,
                        hint=_suggest(k, cols), node=s.name)
        changelog = "append"
        if spec is not None:
            if "source" not in spec.roles:
                d.error("CD203", f"connector '{spec.type}' cannot be used as a source", s.loc, node=s.name)
            else:
                if spec.source_requires_pk and not s.primary_key:
                    d.error("CD104", f"source '{s.name}' ({spec.type}) requires a primary_key", s.loc,
                            hint="CDC sources are keyed changelogs; declare the table's primary key", node=s.name)
                changelog = "updating" if spec.source_is_changelog(s.format, bool(s.primary_key)) else "append"
                if s.endpoint.get("changelog") == "upsert":
                    changelog = "updating"
                    if not s.primary_key:
                        d.error("CD104", f"source '{s.name}' reads an upsert topic and needs a primary_key", s.loc,
                                node=s.name)
                if p.mode == "batch":
                    changelog = "append"  # a bounded read (snapshot) is insert-only
                if changelog == "updating" and not s.primary_key and spec.type == "kafka":
                    d.warning("CD107", f"changelog source '{s.name}' has no primary_key; Flink cannot "
                              "normalize duplicate change events", s.loc, node=s.name)
                opts, problems = build_options(
                    spec, "source", props, s.endpoint, s.format, "append",
                    LowerCtx(p.name, s.name, s.connection, s.primary_key), s.options,
                    conn=p.connections.get(s.connection))
                _report_option_problems(c, problems, s.loc, s.name)
                c.options[s.name] = opts
                _check_source_metadata(c, s, spec, opts)
                _check_key_fields(c, s.name, s.endpoint, [col.name for col in s.columns
                                                          if col.metadata is None and not col.expression], s.loc)
        p.relations[s.name] = RelationInfo(columns=out_cols, rowtime=rowtime, changelog=changelog,
                                           upsert_key=list(s.primary_key) or None)


def _metadata_problem(spec: ConnectorSpec, key: str, declared, writing: bool) -> tuple[str, str] | None:
    """(code, message) when a metadata key is unknown, unwritable, or declared with another type."""
    if spec.metadata is None or any(key.startswith(pre) for pre in spec.metadata_prefixes):
        return None  # not catalogued (custom connectors, lakes): the planner checks it in `cascade verify`
    if key not in spec.metadata:
        known = ", ".join(k for k, (_, w) in spec.metadata.items() if w or not writing) or "(none)"
        return ("CD220", f"connector '{spec.type}' has no {'writable ' if writing else ''}metadata '{key}'; "
                         f"available: {known}")
    typ, writable = spec.metadata[key]
    if writing and not writable:
        return ("CD220", f"connector '{spec.type}' metadata '{key}' is read-only")
    if declared is not None and not declared.same_base(parse_type(typ)):
        return ("CD221", f"metadata '{key}' of connector '{spec.type}' is {typ}, declared as {declared}")
    return None


def _check_key_fields(c: Checked, node: str, endpoint: dict, columns: list[str] | None, loc):
    keys = endpoint.get("key_fields")
    if keys is None or columns is None:
        return
    for k in ([keys] if isinstance(keys, str) else keys):
        if k not in columns:
            c.diagnostics.error("CD225", f"{node}: key_fields column '{k}' is not a physical column", loc, node=node,
                                hint=_suggest(str(k), columns) or "the key carries table columns, not metadata")


def _check_source_metadata(c: Checked, s, spec: ConnectorSpec, opts: dict):
    d = c.diagnostics
    for col in s.columns:
        if col.metadata is not None:
            problem = _metadata_problem(spec, col.metadata, col.type, writing=False)
            if problem and problem[0] == "CD221":
                d.warning(problem[0], f"source '{s.name}' column '{col.name}': {problem[1]}", col.loc, node=s.name,
                          hint="Flink casts it if it can; declare the connector's type to be exact")
            elif problem:
                d.error(problem[0], f"source '{s.name}' column '{col.name}': {problem[1]}", col.loc, node=s.name)
            if opts.get("connector") == "cascade-healing-cdc":
                d.error("CD222", f"source '{s.name}' column '{col.name}': metadata columns can't be read with "
                        "on_slot_loss: resnapshot", col.loc, node=s.name,
                        hint="the healing source re-reads the table and reconciles rows by key, so per-change "
                             "metadata (op_ts, row_kind) would describe the re-read, not the change")
        if (col.metadata is not None or col.expression) and col.name in s.primary_key:
            d.error("CD106", f"source '{s.name}': primary_key column '{col.name}' must be a physical column, "
                    "not metadata or computed", col.loc, node=s.name)


IDLE_GLOBAL = "table.exec.source.idle-timeout"
IDLE_SOURCE = "scan.watermark.idle-timeout"
WATERMARK_PUSHDOWN = {"kafka", "upsert-kafka"}  # connectors that take a per-source idle timeout
# Kafka sources that declare no idle_timeout, when a job-wide one is set for other sources. Not
# "never": a reader subtask with no partition (parallelism above the partition count) only stands
# down through an idle timeout, and would hold event time back forever. Long enough that a broker
# restart or a restore doesn't let other sources overtake the stream.
KAFKA_DEFAULT_IDLE_MS = 60_000
SOURCE_WATERMARK = {"cascade-fixture"}  # catalog connectors implementing SupportsSourceWatermark


def _is_zero_duration(v) -> bool:
    return str(v).strip().replace(" ", "") in ("0", "0ms", "0s")


def _check_watermark_options(c: Checked):
    """Alignment and emit strategy are per-source WITH options that only watermark push-down sources take."""
    p, d = c.pipeline, c.diagnostics
    for s in p.sources.values():
        et = s.event_time
        if not et or s.name not in c.options:
            continue
        opts = c.options[s.name]
        connector = opts.get("connector")
        custom = p.connections.get(s.connection) is not None and p.connections[s.connection].type == "custom"
        if (et.alignment or et.emit) and connector not in WATERMARK_PUSHDOWN and not custom:
            d.error("CD110", f"source '{s.name}': watermark alignment and emit strategy need a connector with "
                    f"watermark push-down ({', '.join(sorted(WATERMARK_PUSHDOWN))}); '{connector}' has none",
                    et.loc, node=s.name)
            continue
        if et.alignment:
            opts.setdefault("scan.watermark.alignment.group", et.alignment["group"])
            opts.setdefault("scan.watermark.alignment.max-drift", f"{et.alignment['max_drift_ms']} ms")
            if et.alignment.get("update_interval_ms"):
                opts.setdefault("scan.watermark.alignment.update-interval", f"{et.alignment['update_interval_ms']} ms")
        if et.emit:
            opts.setdefault("scan.watermark.emit.strategy", et.emit.replace("_", "-"))
        if et.expression == "source" and connector not in SOURCE_WATERMARK and not custom:
            d.error("CD111", f"source '{s.name}': watermark: source (SOURCE_WATERMARK()) needs a connector that "
                    f"emits its own watermarks; '{connector}' doesn't", et.loc, node=s.name,
                    hint="use max_out_of_orderness or a watermark expression")


def _check_idle_timeouts(c: Checked):
    """Idle timeouts are per source. A job-wide one marks *every* source idle when it pauses, so a
    stalled fact stream (broker restart, slow restore) lets another source's watermark move event
    time past rows that are still on their way, and windows then drop them as late."""
    p, d = c.pipeline, c.diagnostics
    timed = [s for s in p.sources.values() if s.event_time]
    declared = [s for s in timed if s.event_time.idle_timeout_ms is not None]
    raw = p.flink_config.get(IDLE_GLOBAL)
    if raw is not None and not _is_zero_duration(raw):
        if declared:
            d.error("CD109", f"flink.config sets {IDLE_GLOBAL} and source(s) "
                    f"{', '.join(s.name for s in declared)} declare event_time.idle_timeout; use one or the other",
                    declared[0].event_time.loc, node=declared[0].name,
                    hint=f"remove {IDLE_GLOBAL}; idle_timeout on the sources that legitimately go quiet")
        elif len(timed) > 1:
            d.warning("CD108", f"{IDLE_GLOBAL} ({raw}) applies to every source, including "
                      f"{', '.join(s.name for s in timed)}: if one stalls (broker restart, slow restore), the "
                      "others' watermarks move event time past its pending rows and windows drop them as late",
                      p.loc, hint="set event_time.idle_timeout only on sources that legitimately go quiet "
                                  "(a CDC dimension table, a low-traffic partition)")
        return
    if not declared:
        return

    def pushdown(s):
        return (c.options.get(s.name) or {}).get("connector") in WATERMARK_PUSHDOWN
    shared = [s for s in timed if not pushdown(s)]
    values = {s.event_time.idle_timeout_ms or 0 for s in shared}
    if len(values) > 1:
        d.error("CD109", f"sources {', '.join(s.name for s in shared)} can only take the job-wide idle timeout "
                "(their connectors don't support a per-source one), so they must all declare the same "
                "event_time.idle_timeout (or none)", shared[0].event_time.loc, node=shared[0].name)
        return
    global_ms = next(iter(values), 0)
    if global_ms:
        c.settings[IDLE_GLOBAL] = f"{global_ms} ms"
    for s in timed:
        if pushdown(s) and s.name in c.options:
            own = s.event_time.idle_timeout_ms
            if own is None and global_ms:
                own = KAFKA_DEFAULT_IDLE_MS  # explicit, so the short job-wide one doesn't apply here
            if own is not None:
                c.options[s.name].setdefault(IDLE_SOURCE, f"{own} ms")


# --- models ----------------------------------------------------------------------------

def _sql_loc(model, pos) -> Location | None:
    if model.sql_loc is None:
        return model.loc
    if model.sql_loc.file.endswith(".py"):
        return model.loc  # SQL built in Python: point at the model definition
    return Location(model.sql_loc.file, (model.sql_loc.line or 1) + line_of(model.sql, pos))


def _check_models(c: Checked):
    p, d = c.pipeline, c.diagnostics
    deps: dict[str, list[str]] = {}
    for m in p.models.values():
        if m.process is not None:
            ins = [m.process["side_of"]] if "side_of" in m.process else list(m.process.get("inputs") or [])
            c.analyses[m.name] = Analysis(refs=[Ref(n, None, 0) for n in ins])
            inputs = []
            for n in ins:
                if n in p.sinks:
                    d.error("CD302", f"model '{m.name}' reads from sink '{n}'; sinks are write-only", m.loc, node=m.name)
                elif n not in p.relation_names() or n == m.name:
                    d.error("CD302", f"process model '{m.name}': unknown input '{n}'", m.loc, node=m.name,
                            hint=_suggest(n, p.relation_names()))
                elif n not in inputs:
                    inputs.append(n)
            deps[m.name] = inputs
            continue
        if not m.sql.strip():
            continue
        try:
            _toks, a = analyze_sql(m.sql)
        except SqlSyntaxError as e:
            d.error("CD300", f"SQL syntax error in model '{m.name}': {e}", _sql_loc(m, e.pos), node=m.name)
            continue
        c.analyses[m.name] = a
        for code, msg, pos, hint in a.problems:
            d.error(code, f"model '{m.name}': {msg}", _sql_loc(m, pos), hint=hint, node=m.name)
        inputs = []
        for r in a.refs:
            if r.name == m.name:
                d.error("CD303", f"model '{m.name}' references itself", _sql_loc(m, r.pos), node=m.name)
            elif r.name in p.sinks:
                d.error("CD302", f"model '{m.name}' reads from sink '{r.name}'; sinks are write-only",
                        _sql_loc(m, r.pos), hint=f"read from the sink's input '{p.sinks[r.name].from_}' instead",
                        node=m.name)
            elif r.name not in p.relation_names():
                d.error("CD302", f"model '{m.name}' references unknown table '{r.name}'", _sql_loc(m, r.pos),
                        hint=_suggest(r.name, p.relation_names()) or "declare it as a source or model",
                        node=m.name)
            elif r.name not in inputs:
                inputs.append(r.name)
        deps[m.name] = inputs

    # topological order (Kahn) + cycle detection
    order, state = [], {}

    def visit(n, stack):
        if state.get(n) == "done":
            return
        if state.get(n) == "active":
            cyc = stack[stack.index(n):] + [n]
            d.error("CD303", "cycle between models: " + " -> ".join(cyc), p.models[n].loc, node=n)
            return
        state[n] = "active"
        for dep in deps.get(n, []):
            if dep in p.models:
                visit(dep, stack + [n])
        state[n] = "done"
        order.append(n)

    for n in deps:
        visit(n, [])
    c.order = order if not any(x.code == "CD303" for x in d.items) else []

    for name in c.order:
        _analyze_model(c, name, deps[name])


def _analyze_model(c: Checked, name: str, inputs: list[str]):
    p, d = c.pipeline, c.diagnostics
    m, a = p.models[name], c.analyses[name]
    rels = p.relations
    if m.process is not None:
        _analyze_process(c, name, inputs)
        return

    kinds = {op.kind for op in a.ops}
    for fn, table, col, pos in a.windows:
        base = rels.get(table)
        if base is None:
            continue
        if col not in base.rowtime:
            if not base.rowtime:
                d.error("CD331", f"model '{name}': {fn} window over '{table}', which has no time attribute",
                        _sql_loc(m, pos), node=name,
                        hint=f"declare `event_time` on the source feeding '{table}' and keep that column "
                             "unmodified in intermediate models")
            else:
                d.error("CD330", f"model '{name}': DESCRIPTOR({col}) is not a time attribute of '{table}'",
                        _sql_loc(m, pos), node=name,
                        hint=f"time attributes of '{table}': {', '.join(sorted(base.rowtime))}")
    columns, rowtime = infer_relation(m.sql, rels, {f.name.upper(): f.returns for f in p.functions.values()
                                                    if f.returns is not None})
    columns, rowtime = _apply_lock(c, name, inputs, columns, rowtime, kinds)
    c.inferred[name] = columns

    # --- temporal joins need a versioned table
    for r in a.refs:
        if r.temporal and r.name in p.sources:
            src = p.sources[r.name]
            spec = spec_for(p.connections[src.connection].type, p.connections[src.connection]) \
                if src.connection in p.connections else None
            if spec is not None and spec.type == "jdbc":
                continue  # lookup join
            if not src.primary_key:
                d.error("CD340", f"model '{name}': temporal join on '{r.name}' requires it to have a primary_key",
                        _sql_loc(m, r.pos), node=name, hint="a versioned table is keyed by its primary key")
            if src.event_time is None:
                d.error("CD341", f"model '{name}': event-time temporal join on '{r.name}' requires `event_time` "
                        f"on source '{r.name}'", _sql_loc(m, r.pos), node=name)

    # --- changelog mode
    temporal_sides = {r.name for r in a.refs if r.temporal}
    updating_inputs = [i for i in inputs if rels.get(i) and rels[i].changelog == "updating"
                       and i not in temporal_sides]
    changelog = "updating" if (a.updating or updating_inputs) else "append"
    if p.mode == "batch":
        changelog = "append"  # batch operators emit final results once
    if updating_inputs and a.windows:
        d.warning("CD332", f"model '{name}': window aggregation over updating input(s) "
                  f"{', '.join(updating_inputs)}", m.loc, node=name,
                  hint="window TVF aggregations expect append-only input; check `cascade verify`")

    # --- output contract
    if m.columns is not None:
        declared = [col.name for col in m.columns]
        if columns is not None:
            inferred = [col.name for col in columns]
            if [x.lower() for x in inferred] != [x.lower() for x in declared]:
                d.error("CD310", f"model '{name}': declared columns {declared} do not match the query's "
                        f"output columns {inferred}", m.loc, node=name,
                        hint="the `columns` contract must list every output column, in SELECT order")
            else:
                for got, want in zip(columns, m.columns):
                    if got.type is not None and not got.type.same_base(want.type):
                        sev = d.error if got.name in rowtime else d.warning
                        sev("CD311", f"model '{name}': column '{got.name}' is {got.type} but the contract "
                            f"declares {want.type}" + ("; time attributes cannot be cast" if got.name in rowtime
                                                       else "; it will be CAST"), want.loc or m.loc, node=name)
        columns = [OutputColumn(col.name, col.type, (columns[i].expr if columns and i < len(columns) else ""))
                   for i, col in enumerate(m.columns)]

    # --- expectations
    _check_expectations(c, m, columns, changelog)

    # --- state
    unbounded = [op for op in a.ops if not op.bounded and op.kind in UNBOUNDED_KINDS]
    if p.mode == "batch":
        unbounded = []  # a batch job's state lives for one run
    if unbounded and p.state_ttl_ms is None and not a.has_ttl_hint and not m.allow_unbounded_state:
        ops = "; ".join(op.detail for op in unbounded)
        d.error("CD350", f"model '{name}' keeps unbounded state ({ops}) and no state TTL is configured",
                m.loc, node=name,
                hint="set `defaults.state_ttl`, add a /*+ STATE_TTL(...) */ hint, rewrite with a window/interval "
                     "join, or set `allow_unbounded_state: true` if the key space is known to be bounded")
    elif unbounded and p.state_ttl_ms is not None and not a.has_ttl_hint:
        d.info("CD351", f"model '{name}': state for idle keys expires after the pipeline state_ttl; results "
               "for keys inactive longer than that restart from empty", m.loc, node=name)

    rels[name] = RelationInfo(columns=columns, rowtime=rowtime, changelog=changelog, inputs=inputs,
                              stateful_ops=list(a.ops), has_ttl_hint=a.has_ttl_hint,
                              upsert_key=_upsert_key(p, a, inputs))


def _analyze_process(c: Checked, name: str, inputs: list[str]):
    """A process model's output is what it declares: columns, changelog, key and time attribute."""
    p, d = c.pipeline, c.diagnostics
    m, pr = p.models[name], p.models[name].process
    columns = [OutputColumn(col.name, col.type) for col in (m.columns or [])]
    names = {col.name for col in columns}
    side = "side_of" in pr
    et = pr.get("event_time")
    if et and et not in names:
        d.error("CD142", f"process model '{name}': event_time '{et}' is not a declared column", m.loc, node=name)
    elif et and not next(col for col in columns if col.name == et).type.is_temporal:
        d.error("CD142", f"process model '{name}': event_time '{et}' must be a TIMESTAMP column", m.loc, node=name)
    pk = [] if side else list(pr.get("primary_key") or [])
    for k in pk:
        if k not in names:
            d.error("CD142", f"process model '{name}': primary_key column '{k}' is not declared", m.loc, node=name)
    if not side:
        for n in inputs:
            cols = {col.name for col in (p.relations[n].columns or [])} if n in p.relations else set()
            for k in pr.get("key_by") or []:
                if cols and k not in cols:
                    d.error("CD141", f"process model '{name}': key_by column '{k}' is not a column of input '{n}'",
                            m.loc, node=name)
        if p.mode == "streaming":
            d.info("CD140", f"process model '{name}': Flink can't compile a plan for DataStream code, so the "
                   "pipeline deploys as statements and upgrade checks don't cover this model's state; give its "
                   "stateful operators .uid(ctx.uid(...))", m.loc, node=name)
    changelog = "updating" if pr.get("changelog", "append") != "append" and not side else "append"
    if p.mode == "batch":
        changelog = "append"
    _check_expectations(c, m, columns, changelog)
    p.relations[name] = RelationInfo(
        columns=columns, rowtime={et} if et else set(), changelog=changelog, inputs=inputs,
        stateful_ops=[] if side else [StatefulOp("process", True, "process " + (
            pr["class"] if pr.get("language", "java") == "java" else f"{pr.get('file')}:{pr.get('function')}"))],
        upsert_key=pk or None)


def _upsert_key(p: Pipeline, a: Analysis, inputs: list[str]) -> list[str] | None:
    """Output names of a statically provable unique key: GROUP BY keys, or a CDC source key passed through."""
    renames: dict[str, str] = {}
    for item in a.select_items:
        if len(item) in (1, 3, 4, 5) and item[-1].kind in ("ident", "qident"):
            if len(item) == 1:
                renames[item[0].name.lower()] = item[0].name
            elif len(item) == 3 and item[1].text == ".":
                renames[item[2].name.lower()] = item[2].name
            elif len(item) == 3 and item[1].is_kw("AS"):
                renames[item[0].name.lower()] = item[2].name
            elif len(item) == 5 and item[1].text == "." and item[3].is_kw("AS"):
                renames[item[2].name.lower()] = item[4].name
    if any(len(i) == 1 and i[0].text == "*" for i in a.select_items):
        for i in inputs:
            for col in (p.relations[i].columns or []):
                renames.setdefault(col.name.lower(), col.name)
    if a.group_keys is not None:
        keys = a.group_keys
    elif len(inputs) == 1 and not a.ops and p.relations[inputs[0]].upsert_key:
        keys = p.relations[inputs[0]].upsert_key
    else:
        return None
    out = [renames.get(k.lower()) for k in keys]
    return None if any(x is None for x in out) else out


def process_identity(m, inputs: list[str] | None = None) -> list:
    """The parts of a process model that define what it computes (inputs by position if given)."""
    pr = m.process
    if "side_of" in pr:
        return ["side", pr["side"], pr.get("event_time")]
    ins = [f"_input{i}" for i in range(len(inputs))] if inputs is not None else pr["inputs"]
    code = pr["class"] if pr.get("language", "java") == "java" else f"{pr.get('file')}:{pr.get('function')}"
    return [code, ins, pr.get("key_by"), sorted(pr.get("config", {}).items()), pr.get("changelog"),
            pr.get("primary_key"), pr.get("event_time"),
            sorted((k, [(c.name, c.type.text) for c in v["columns"]], v.get("event_time"))
                   for k, v in pr.get("side_outputs", {}).items())]


def model_fingerprint(p: Pipeline, name: str, inputs: list[str],
                      input_columns: dict[str, list[OutputColumn] | None]) -> str:
    """Identity of a model's output schema: its SQL, contract and input schemas (base types)."""
    m = p.models[name]
    ins = []
    for i in inputs:
        cols = input_columns.get(i)
        ins.append((i, [(col.name, col.type.with_nullable(True).text if col.type else None) for col in cols or []]))
    contract = [(col.name, col.type.text) for col in m.columns] if m.columns else None
    if m.process is not None:
        return h("process", process_identity(m), contract, ins)
    sql_up = m.sql.upper()
    fns = sorted((f.name, f.language, f.class_name, f.file, f.function, f.returns.text if f.returns else None)
                 for f in p.functions.values() if f.name.upper() in sql_up)
    if fns:
        return h(normalize_sql(m.sql), contract, ins, fns)
    return h(normalize_sql(m.sql), contract, ins)


def _apply_lock(c: Checked, name, inputs, columns, rowtime, kinds):
    """Use exact Flink-derived types from cascade.lock.json when its fingerprint is current."""
    p, d = c.pipeline, c.diagnostics
    entry = (p.schema_lock or {}).get(name)
    if entry is None:
        return columns, rowtime
    fp = model_fingerprint(p, name, inputs, {i: p.relations[i].columns for i in inputs if i in p.relations})
    if entry.get("fingerprint") != fp:
        d.warning("CD361", f"model '{name}': cascade.lock.json is stale (the model or its inputs changed)",
                  p.models[name].loc, hint="re-run `cascade infer`", node=name)
        return columns, rowtime
    exact = []
    for col in entry["columns"]:
        try:
            exact.append(OutputColumn(col["name"], parse_type(col["type"])))
        except TypeParseError:
            exact.append(OutputColumn(col["name"], None))
    if columns is None:
        # the validator reports pre-optimization time attributes; trust them only where no
        # aggregation/regular join can have materialized them
        trusted = not (kinds & {"group_agg", "global_agg", "window_agg", "regular_join"})
        rowtime = {col["name"] for col in entry["columns"] if col.get("time_attribute") == "rowtime"} if trusted else set()
    d.info("CD360", f"model '{name}': using exact schema from cascade.lock.json", p.models[name].loc, node=name)
    return exact, rowtime


def _check_expectations(c: Checked, m, columns, changelog):
    d = c.diagnostics
    known = {col.name.lower() for col in columns} if columns is not None else None
    for e in m.expectations:
        try:
            refs = referenced_columns(e.check)
        except SqlSyntaxError as err:
            d.error("CD321", f"model '{m.name}' expectation '{e.name}': {err}", e.loc, node=m.name)
            continue
        if known is not None:
            for col, _ in refs:
                if col.lower() not in known:
                    d.error("CD320", f"model '{m.name}' expectation '{e.name}' references unknown column '{col}'",
                            e.loc, hint=_suggest(col, [x.name for x in columns]), node=m.name)
        if e.on_violation == "quarantine":
            if c.pipeline.quarantine is None:
                d.error("CD322", f"model '{m.name}' expectation '{e.name}' quarantines rows but the pipeline "
                        "has no `quarantine:` destination", e.loc, node=m.name)
            if changelog != "append":
                d.error("CD323", f"model '{m.name}' is updating; quarantine expectations need append-only "
                        "models", e.loc, hint="move the expectation to an upstream append-only model",
                        node=m.name)
            if columns is None or any(col.type is None for col in columns):
                d.error("CD324", f"model '{m.name}': quarantine needs every output column type to be known",
                        e.loc, hint="declare a `columns:` contract on the model", node=m.name)


# --- functions --------------------------------------------------------------------------

BUILTIN_FUNCTIONS = {"UPPER", "LOWER", "TRIM", "CONCAT", "SUBSTRING", "COALESCE", "ABS", "ROUND", "FLOOR", "CEIL",
                     "COUNT", "SUM", "MIN", "MAX", "AVG", "CAST", "IF", "NULLIF", "REPLACE", "REGEXP_REPLACE",
                     "REGEXP_EXTRACT", "DATE_FORMAT", "TO_TIMESTAMP", "NOW", "MD5", "SHA256", "SPLIT_INDEX",
                     "JSON_VALUE", "JSON_QUERY", "HASH_CODE", "LENGTH", "POSITION", "LPAD", "RPAD", "INITCAP"}


def _check_functions(c: Checked):
    import os
    from .udfs import SHIM_MODULE, check_python_module
    p, d = c.pipeline, c.diagnostics
    modules: dict[str, str] = {}
    for f in p.functions.values():
        if f.name.upper() in BUILTIN_FUNCTIONS or f.name.upper() in {"CASCADE_ASSERT", "CASCADE_EXPECT"}:
            d.warning("CD162", f"function '{f.name}' shadows the built-in function {f.name.upper()}", f.loc,
                      hint="rename it; temporary system functions take precedence over built-ins everywhere")
        if f.language == "java":
            if f.jar and not os.path.exists(os.path.join(p.root_dir, f.jar)):
                d.warning("CD164", f"function '{f.name}': jar '{f.jar}' does not exist (yet)", f.loc,
                          hint="build it before running tests, verify or deploy")
            continue
        path = os.path.join(p.root_dir, f.file or "")
        if not f.file or not f.file.endswith(".py") or not os.path.isfile(path):
            d.error("CD160", f"function '{f.name}': Python module '{f.file}' not found in the project", f.loc,
                    hint="`file` is a .py module relative to the project directory")
            continue
        problem = check_python_module(path, f.function)
        if problem:
            d.error("CD161", f"function '{f.name}': {f.file} {problem}", f.loc)
        if f.module == SHIM_MODULE:
            d.error("CD163", f"function '{f.name}': module name '{SHIM_MODULE}' is reserved", f.loc)
        elif modules.get(f.module, f.file) != f.file:
            d.error("CD163", f"function '{f.name}': modules {modules[f.module]} and {f.file} have the same name "
                    f"'{f.module}'; Python workers can import only one", f.loc)
        modules[f.module] = f.file


# --- sinks / quarantine / pipeline -----------------------------------------------------

def _check_sinks(c: Checked):
    p, d = c.pipeline, c.diagnostics
    for k in p.sinks.values():
        rel = p.relations.get(k.from_)
        if k.from_ not in p.relation_names():
            d.error("CD401", f"sink '{k.name}' reads from unknown table '{k.from_}'", k.loc,
                    hint=_suggest(k.from_, p.relation_names()), node=k.name)
        spec, props = _connector(c, k.connection, k.loc, k.name)
        if spec is not None:
            if "sink" not in spec.roles:
                d.error("CD403", f"connector '{spec.type}' cannot be used as a sink", k.loc, node=k.name)
                continue
            if k.mode not in spec.sink_modes:
                d.error("CD404", f"connector '{spec.type}' does not support mode '{k.mode}'", k.loc, node=k.name,
                        hint=f"supported modes: {', '.join(sorted(spec.sink_modes))}")
            opts, problems = build_options(spec, "sink", props, k.endpoint, k.format, k.mode,
                                           LowerCtx(p.name, k.name, k.connection, k.primary_key), k.options,
                                           conn=p.connections.get(k.connection))
            _report_option_problems(c, problems, k.loc, k.name)
            c.options[k.name] = opts
            _check_key_fields(c, k.name, k.endpoint, [x.name for x in rel.columns if x.name not in k.metadata]
                              if rel and rel.columns is not None else None, k.loc)
            for col, key in k.metadata.items():
                rcol = next((x for x in (rel.columns or []) if x.name == col), None) if rel else None
                if rel is not None and rel.columns is not None and rcol is None:
                    d.error("CD223", f"sink '{k.name}': metadata column '{col}' is not a column of '{k.from_}'",
                            k.loc, node=k.name)
                    continue
                problem = _metadata_problem(spec, key, rcol.type if rcol else None, writing=True)
                if problem and problem[0] == "CD221":
                    d.warning(problem[0], f"sink '{k.name}' column '{col}': {problem[1]}", k.loc, node=k.name,
                              hint="cast the column in the model")
                elif problem:
                    d.error(problem[0], f"sink '{k.name}' column '{col}': {problem[1]}", k.loc, node=k.name)
            if spec.type == "kafka":
                if "topic" not in k.endpoint:
                    d.error("CD209", f"sink '{k.name}': a Kafka sink needs 'topic'", k.loc, node=k.name,
                            hint="with a column written to the 'topic' metadata, 'topic' is the list of topics "
                                 "it may name")
                elif "topic" in k.metadata.values() and not isinstance(k.endpoint["topic"], list):
                    d.error("CD209", f"sink '{k.name}' routes rows by its 'topic' metadata column, so 'topic' "
                            "must be the list of topics it may name", k.loc, node=k.name,
                            hint="e.g. topic: [orders.eu, orders.us]; rows naming another topic fail the job")
        if spec is not None and spec.type == "jdbc":
            d.info("CD215", f"sink '{k.name}': while the database is unreachable, the JDBC sink fails the job, "
                   "and its close() rethrows the write error during failover, which Flink treats as fatal: "
                   "the TaskManager shuts down (flink-connector-jdbc 4.1; seen in integration/soak.py)",
                   k.loc, node=k.name,
                   hint="run TaskManagers under a supervisor that restarts them (Kubernetes does), and "
                        "don't share a session cluster with other jobs")
        if k.mode == "upsert" and not k.primary_key:
            d.error("CD405", f"upsert sink '{k.name}' needs a primary_key", k.loc, node=k.name)
        if rel is None:
            continue
        if k.mode == "append" and rel.changelog == "updating":
            why = _updating_reason(p, k.from_)
            d.error("CD407", f"sink '{k.name}' is append-only but '{k.from_}' produces updates ({why})", k.loc,
                    node=k.name,
                    hint="use `mode: upsert` with a primary_key, or make the query append-only "
                         "(e.g. a window aggregation instead of a running GROUP BY)")
        if k.on_conflict and k.mode != "upsert":
            d.error("CD411", f"sink '{k.name}': on_conflict only applies to upsert sinks", k.loc, node=k.name)
        if k.on_conflict in ("error", "nothing") and k.from_ in p.relation_names():
            missing = [s for s in sorted(_upstream_sources(p, k.from_))
                       if p.sources[s].event_time is None and not _lookup_only(c, s)]
            if missing:
                d.error("CD412", f"sink '{k.name}': ON CONFLICT DO {k.on_conflict.upper()} needs every source it "
                        f"reads to define event time, but {', '.join(missing)} do(es) not", k.loc, node=k.name,
                        hint="Flink resolves conflicts when the watermark advances; declare `event_time` on those "
                             "sources or use `on_conflict: deduplicate`")
        if k.mode == "upsert" and rel.changelog == "updating" and not k.on_conflict and k.primary_key:
            key = rel.upsert_key
            if key is None or sorted(x.lower() for x in key) != sorted(x.lower() for x in k.primary_key):
                d.warning("CD410", f"sink '{k.name}': cannot prove that the upsert key of '{k.from_}' "
                          f"({', '.join(key) if key else 'unknown'}) equals the sink primary key "
                          f"({', '.join(k.primary_key)}); Flink >= 2.3 will reject the INSERT", k.loc, node=k.name,
                          hint="set `on_conflict: error | nothing | deduplicate`, or key the sink by the query's "
                               "GROUP BY / source primary key (`cascade verify` gives the planner's answer)")
        if rel.columns is None:
            d.error("CD408", f"sink '{k.name}': cannot determine the output columns of '{k.from_}'", k.loc,
                    node=k.name, hint=f"give every SELECT item a name, or declare `columns:` on '{k.from_}'")
            continue
        unknown = [col.name for col in rel.columns if col.type is None]
        if unknown:
            d.error("CD408", f"sink '{k.name}': cannot infer the type of {', '.join(unknown)} in '{k.from_}'",
                    k.loc, node=k.name,
                    hint=f"declare a `columns:` contract on '{k.from_}' or CAST the expressions")
        names = {col.name.lower() for col in rel.columns}
        for key in k.primary_key:
            if key.lower() not in names:
                d.error("CD406", f"sink '{k.name}' primary_key column '{key}' is not an output column of "
                        f"'{k.from_}'", k.loc, hint=_suggest(key, [col.name for col in rel.columns]), node=k.name)


def _upstream_sources(p: Pipeline, name: str) -> set[str]:
    out, stack, seen = set(), [name], set()
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        if n in p.sources:
            out.add(n)
        elif p.relations.get(n):
            stack.extend(p.relations[n].inputs)
    return out


def _lookup_only(c: Checked, source: str) -> bool:
    """A source used only as the dimension side of FOR SYSTEM_TIME AS OF (a lookup join)."""
    pat = re.compile(r"\b" + re.escape(source) + r"\b\s+FOR\s+SYSTEM_TIME\s+AS\s+OF", re.IGNORECASE)
    uses = [m for m in c.pipeline.models.values() if re.search(r"\b" + re.escape(source) + r"\b", m.sql)]
    return bool(uses) and all(pat.search(m.sql) for m in uses) and \
        c.pipeline.sources[source].event_time is None


def _updating_reason(p: Pipeline, name: str) -> str:
    rel = p.relations[name]
    ops = [op.detail for op in rel.stateful_ops if op.kind in ("group_agg", "regular_join", "dedup", "global_agg")]
    if ops:
        return ops[0]
    for i in rel.inputs:
        if p.relations.get(i) and p.relations[i].changelog == "updating":
            return f"its input '{i}' is updating"
    if name in p.sources:
        return "it is a changelog source"
    return "updating input"


def _check_quarantine(c: Checked):
    p = c.pipeline
    q = p.quarantine
    if q is None:
        return
    spec, props = _connector(c, q.connection, q.loc, "quarantine")
    if spec is None:
        return
    if "sink" not in spec.roles:
        c.diagnostics.error("CD403", f"connector '{spec.type}' cannot be used for quarantine", q.loc)
        return
    for m in p.models.values():
        if any(e.on_violation == "quarantine" for e in m.expectations):
            endpoint = {k: (v.format(pipeline=p.name, model=m.name) if isinstance(v, str) else v)
                        for k, v in q.endpoint.items()}
            node = f"{m.name}__quarantine"
            opts, problems = build_options(spec, "sink", props, endpoint, q.format, "append",
                                           LowerCtx(p.name, node, q.connection, []), {},
                                           conn=p.connections.get(q.connection))
            _report_option_problems(c, problems, q.loc, "quarantine")
            c.options[node] = opts


def _check_pipeline(c: Checked):
    p, d = c.pipeline, c.diagnostics
    if not p.sinks:
        d.error("CD502", "pipeline has no sinks; nothing would run", p.loc)
        return
    used = set()
    stack = [k.from_ for k in p.sinks.values()]
    while stack:
        n = stack.pop()
        if n in used:
            continue
        used.add(n)
        rel = p.relations.get(n)
        if rel:
            stack.extend(rel.inputs)
    for n in p.relation_names() - used:
        node = p.sources.get(n) or p.models.get(n)
        d.warning("CD501", f"{'source' if n in p.sources else 'model'} '{n}' does not feed any sink", node.loc,
                  hint="it will not be part of the deployed job", node=n)


# --- batch ------------------------------------------------------------------------------

# how each catalog connector reads a bounded snapshot; None = can't be bounded
BATCH_SOURCE = {
    "kafka": {"scan.bounded.mode": "latest-offset"},   # everything in the topic when the job starts
    "eventhubs": {"scan.bounded.mode": "latest-offset"},   # lowers to the Kafka connector
    # CDC sources declare a changelog even when reading a snapshot, which Flink's batch planner rejects
    # (integration/connectors.py, every CDC connector). Relational ones are read with a JDBC scan of the
    # same table instead (_cdc_as_jdbc); MongoDB has no SQL batch read.
    "mongodb-cdc": None,
    "iceberg": {"streaming": "false"}, "hudi": {"read.streaming.enabled": "false"},
    "paimon": {}, "filesystem": {}, "jdbc": {}, "datagen": {},
    "kinesis": None,
}


def _cdc_as_jdbc(connector: str, opts: dict) -> dict | None:
    """The bounded read of a relational CDC source: a JDBC scan of the same table."""
    host, port = opts.get("hostname"), opts.get("port")
    db, table = opts.get("database-name"), opts.get("table-name")
    if connector == "postgres-cdc":
        url, table = f"jdbc:postgresql://{host}:{port or 5432}/{db}", f"{opts.get('schema-name')}.{table}"
    elif connector == "mysql-cdc":
        url = f"jdbc:mysql://{host}:{port or 3306}/{db}"
    elif connector == "sqlserver-cdc":
        url = f"jdbc:sqlserver://{host}:{port or 1433};databaseName={db};encrypt=false"
    elif connector == "oracle-cdc":  # a table in a pluggable database is reached through its service
        service = opts.get("debezium.database.pdb.name") or db
        url, table = f"jdbc:oracle:thin:@{host}:{port or 1521}/{service}", f"{opts.get('schema-name')}.{table}"
    elif connector == "db2-cdc":
        url = f"jdbc:db2://{host}:{port or 50000}/{db}"
    else:
        return None
    out = {"connector": "jdbc", "url": url, "table-name": table, "username": opts.get("username", "")}
    if opts.get("password") is not None:
        out["password"] = opts["password"]
    return out


def _check_batch(c: Checked):
    """mode: batch runs the pipeline once over bounded inputs, then finishes."""
    p, d = c.pipeline, c.diagnostics
    if p.mode != "batch":
        return
    c.settings["execution.runtime-mode"] = "batch"
    for s in p.sources.values():
        opts = c.options.get(s.name)
        if opts is None:
            continue
        connector = opts.get("connector")
        custom = s.connection in p.connections and p.connections[s.connection].type == "custom"
        if custom:
            continue  # the user's options decide boundedness
        if connector == "cascade-healing-cdc":  # a batch run has no slot to lose
            connector = opts["connector"] = opts.pop("delegate")
        jdbc = _cdc_as_jdbc(connector, opts)
        if jdbc is not None:
            opts.clear()
            opts.update(jdbc)
            from .connectors import _jdbc_artifacts
            c.extra_artifacts |= {"org.apache.flink:flink-connector-jdbc-core", *_jdbc_artifacts(jdbc["url"])}
            continue
        bounded = BATCH_SOURCE.get(connector, {})
        if bounded is None:
            d.error("CD120", f"source '{s.name}': connector '{connector}' has no bounded read Flink's batch mode "
                    "accepts, so it can't be used in a batch pipeline", s.loc, node=s.name)
            continue
        for k, v in bounded.items():
            opts[k] = v
        if connector == "datagen" and "number-of-rows" not in opts:  # even with sequence fields (Flink 2.3)
            d.error("CD120", f"source '{s.name}': datagen in a batch pipeline needs number_of_rows", s.loc,
                    node=s.name)
        if connector == "upsert-kafka" and "scan.bounded.mode" in opts:
            pass
    for m in p.models.values():
        if m.backfill:
            d.error("CD122", f"model '{m.name}': backfill only applies to streaming pipelines (a batch run "
                    "always reads its inputs from the start)", m.loc, node=m.name)
    for name, a in c.analyses.items():
        if any(r.temporal for r in a.refs):
            d.error("CD121", f"model '{name}': temporal joins (FOR SYSTEM_TIME AS OF) are streaming-only",
                    p.models[name].loc, node=name, hint="join on the key instead; a batch run sees one version "
                                                        "of each table")


# --- scaling ----------------------------------------------------------------------------

FLINK_MAX_PARALLELISM = 32768
SCALING_OWNED = {"pipeline.max-parallelism": "max_parallelism", "parallelism.default": "parallelism"}


def aligned_parallelisms(max_parallelism: int, lo: int, hi: int) -> list[int]:
    """Parallelisms in [lo, hi] that split the key groups evenly (no subtask gets an extra group)."""
    return [x for x in range(max(lo, 1), min(hi, max_parallelism) + 1) if max_parallelism % x == 0]


def _check_scaling(c: Checked):
    p, d = c.pipeline, c.diagnostics
    sc = p.scaling
    for key, field_name in SCALING_OWNED.items():
        if key in p.flink_config:
            d.error("CD521", f"set '{key}' through `scaling.{field_name}`, not flink.config", p.loc,
                    hint="the scaling block is what the autoscaler and upgrade checks read")
    keyed = [n for n in c.order if p.relations.get(n) and p.relations[n].stateful_ops]
    if p.mode == "batch":
        keyed = []  # no state survives a batch run, so max parallelism can change freely
    if sc is None or sc.max_parallelism is None:
        if keyed:
            d.warning("CD520", f"pipeline keeps keyed state ({', '.join(keyed[:3])}"
                      f"{', ...' if len(keyed) > 3 else ''}) but does not set `scaling.max_parallelism`",
                      sc.loc if sc else p.loc,
                      hint="max parallelism is fixed when the job first starts and caps how far it can ever "
                           "scale; changing it later discards keyed state. Pick a number with many divisors "
                           "that leaves headroom, e.g. 360 or 720")
        if sc is None:
            return
    if sc.max_parallelism is not None and not 1 <= sc.max_parallelism <= FLINK_MAX_PARALLELISM:
        d.error("CD521", f"scaling.max_parallelism must be between 1 and {FLINK_MAX_PARALLELISM}", sc.loc)
        return
    for key in ("parallelism", "min", "max"):
        v = getattr(sc, key)
        if v is not None and v < 1:
            d.error("CD521", f"scaling.{key} must be at least 1", sc.loc)
            return
    lo = sc.min or 1
    hi = sc.max or sc.max_parallelism or FLINK_MAX_PARALLELISM
    if lo > hi:
        d.error("CD521", f"scaling.min ({lo}) is greater than scaling.max ({hi})", sc.loc)
    if sc.max_parallelism and sc.max and sc.max > sc.max_parallelism:
        d.error("CD521", f"scaling.max ({sc.max}) exceeds scaling.max_parallelism ({sc.max_parallelism})", sc.loc,
                hint="an operator can never run with more subtasks than key groups")
    if sc.parallelism is not None and not lo <= sc.parallelism <= hi:
        d.error("CD521", f"scaling.parallelism ({sc.parallelism}) is outside [{lo}, {hi}]", sc.loc)
    if sc.target_utilization is not None and not 0 < sc.target_utilization <= 1:
        d.error("CD521", "scaling.target_utilization must be in (0, 1]", sc.loc)
    if sc.max_parallelism and sc.max and sc.max - lo >= 4 and not d.has_errors:
        aligned = aligned_parallelisms(sc.max_parallelism, lo, sc.max)
        if len(aligned) < 3:
            d.warning("CD522", f"scaling.max_parallelism {sc.max_parallelism} divides evenly by only "
                      f"{len(aligned)} parallelism(s) in [{lo}, {sc.max}] ({', '.join(map(str, aligned)) or 'none'}); "
                      "other parallelisms give some subtasks one more key group than others", sc.loc,
                      hint="highly composite values such as 120, 360, 720 or 2520 scale evenly")
    for m in p.models.values():
        ms = m.scaling
        if ms is None:
            continue
        if ms.min is not None and ms.max is not None and ms.min > ms.max:
            d.error("CD521", f"model '{m.name}': scaling.min is greater than scaling.max", ms.loc, node=m.name)
        for key in ("min", "max"):
            v = getattr(ms, key)
            if v is not None and not lo <= v <= hi:
                d.error("CD521", f"model '{m.name}': scaling.{key} ({v}) is outside the pipeline's range "
                        f"[{lo}, {hi}]", ms.loc, node=m.name)
