"""Semantic analysis: resolve the dependency graph, infer schemas / changelog modes /
time attributes for every relation, and run the validation rules.

Diagnostic code ranges:
  PD0xx structure (loader)      PD1xx names & sources       PD2xx connectors & secrets
  PD3xx models & expectations   PD35x state                 PD4xx sinks
  PD5xx pipeline-level
"""
from __future__ import annotations

import difflib
from dataclasses import dataclass, field

from .connectors import CATALOG, ConnectorSpec, LowerCtx, build_options
from .diagnostics import Diagnostics, Location
from .ir import OutputColumn, Pipeline, RelationInfo
from .sqlscan import (Analysis, SqlSyntaxError, analyze_sql, infer_relation, line_of,
                      referenced_columns)
from .types import parse_type

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


def _suggest(name, candidates) -> str | None:
    m = difflib.get_close_matches(name, sorted(candidates), n=1)
    return f"did you mean '{m[0]}'?" if m else None


def check(p: Pipeline, diags: Diagnostics | None = None) -> Checked:
    d = diags or Diagnostics()
    c = Checked(p, d)
    _check_namespace(c)
    _check_connections(c)
    _check_sources(c)
    _check_models(c)
    _check_sinks(c)
    _check_quarantine(c)
    _check_pipeline(c)
    return c


# --- names / connections ---------------------------------------------------------------

def _check_namespace(c: Checked):
    p, d = c.pipeline, c.diagnostics
    seen: dict[str, str] = {}
    for kind, nodes in (("source", p.sources), ("model", p.models), ("sink", p.sinks)):
        for name, node in nodes.items():
            if name in seen:
                d.error("PD101", f"name '{name}' is used by both a {seen[name]} and a {kind}", node.loc,
                        hint="sources, models and sinks share one namespace", node=name)
            seen[name] = kind
            if "__" in name:
                d.error("PD101", f"name '{name}' must not contain '__' (reserved for generated objects)",
                        node.loc, node=name)


def _connector(c: Checked, conn_name: str, loc, node: str) -> tuple[ConnectorSpec | None, dict]:
    p, d = c.pipeline, c.diagnostics
    if conn_name in p.connections:
        conn = p.connections[conn_name]
        return CATALOG.get(conn.type), conn.properties
    # built-in zero-config connectors may be referenced directly (connection: datagen)
    spec = CATALOG.get(conn_name)
    if spec is not None and not spec.connection:
        c.implicit_connections.add(conn_name)
        return spec, {}
    d.error("PD202", f"unknown connection '{conn_name}'", loc, node=node,
            hint=_suggest(conn_name, p.connections) or "declare it under `connections:`")
    return None, {}


def _check_connections(c: Checked):
    for conn in c.pipeline.connections.values():
        if conn.type not in CATALOG:
            c.diagnostics.error("PD200", f"unknown connector type '{conn.type}' for connection '{conn.name}'",
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
            d.error("PD105", f"source '{s.name}' declares a column twice", s.loc, node=s.name)
        rowtime: set[str] = set()
        out_cols = [OutputColumn(col.name, col.type) for col in s.columns]
        if s.event_time:
            et = cols.get(s.event_time.column)
            if et is None:
                d.error("PD102", f"event_time column '{s.event_time.column}' is not a column of source '{s.name}'",
                        s.event_time.loc, hint=_suggest(s.event_time.column, cols), node=s.name)
            elif not et.type.is_temporal:
                d.error("PD103", f"event_time column '{et.name}' must be TIMESTAMP(p) or TIMESTAMP_LTZ(p), "
                        f"got {et.type}", s.event_time.loc,
                        hint="convert it with a computed column in a model, or change the declared type",
                        node=s.name)
            else:
                rowtime.add(et.name)
        if s.processing_time:
            if s.processing_time in cols:
                d.error("PD105", f"processing_time column '{s.processing_time}' collides with a declared column",
                        s.loc, node=s.name)
            out_cols.append(OutputColumn(s.processing_time, parse_type("TIMESTAMP_LTZ(3) NOT NULL")))
            rowtime.add(s.processing_time)
        for k in s.primary_key:
            if k not in cols:
                d.error("PD106", f"primary_key column '{k}' is not a column of source '{s.name}'", s.loc,
                        hint=_suggest(k, cols), node=s.name)
        changelog = "append"
        if spec is not None:
            if "source" not in spec.roles:
                d.error("PD203", f"connector '{spec.type}' cannot be used as a source", s.loc, node=s.name)
            else:
                if spec.source_requires_pk and not s.primary_key:
                    d.error("PD104", f"source '{s.name}' ({spec.type}) requires a primary_key", s.loc,
                            hint="CDC sources are keyed changelogs; declare the table's primary key", node=s.name)
                changelog = "updating" if spec.source_is_changelog(s.format) else "append"
                if changelog == "updating" and not s.primary_key and spec.type == "kafka":
                    d.warning("PD107", f"changelog source '{s.name}' has no primary_key; Flink cannot "
                              "normalize duplicate change events", s.loc, node=s.name)
                opts, problems = build_options(
                    spec, "source", props, s.endpoint, s.format, "append",
                    LowerCtx(p.name, s.name, s.connection, s.primary_key), s.options)
                _report_option_problems(c, problems, s.loc, s.name)
                c.options[s.name] = opts
        p.relations[s.name] = RelationInfo(columns=out_cols, rowtime=rowtime, changelog=changelog,
                                           upsert_key=list(s.primary_key) or None)


# --- models ----------------------------------------------------------------------------

def _sql_loc(model, pos) -> Location | None:
    if model.sql_loc is None:
        return model.loc
    return Location(model.sql_loc.file, (model.sql_loc.line or 1) + line_of(model.sql, pos))


def _check_models(c: Checked):
    p, d = c.pipeline, c.diagnostics
    deps: dict[str, list[str]] = {}
    for m in p.models.values():
        if not m.sql.strip():
            continue
        try:
            _toks, a = analyze_sql(m.sql)
        except SqlSyntaxError as e:
            d.error("PD300", f"SQL syntax error in model '{m.name}': {e}", _sql_loc(m, e.pos), node=m.name)
            continue
        c.analyses[m.name] = a
        for code, msg, pos, hint in a.problems:
            d.error(code, f"model '{m.name}': {msg}", _sql_loc(m, pos), hint=hint, node=m.name)
        inputs = []
        for r in a.refs:
            if r.name == m.name:
                d.error("PD303", f"model '{m.name}' references itself", _sql_loc(m, r.pos), node=m.name)
            elif r.name in p.sinks:
                d.error("PD302", f"model '{m.name}' reads from sink '{r.name}'; sinks are write-only",
                        _sql_loc(m, r.pos), hint=f"read from the sink's input '{p.sinks[r.name].from_}' instead",
                        node=m.name)
            elif r.name not in p.relation_names():
                d.error("PD302", f"model '{m.name}' references unknown table '{r.name}'", _sql_loc(m, r.pos),
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
            d.error("PD303", "cycle between models: " + " -> ".join(cyc), p.models[n].loc, node=n)
            return
        state[n] = "active"
        for dep in deps.get(n, []):
            if dep in p.models:
                visit(dep, stack + [n])
        state[n] = "done"
        order.append(n)

    for n in deps:
        visit(n, [])
    c.order = order if not any(x.code == "PD303" for x in d.items) else []

    for name in c.order:
        _analyze_model(c, name, deps[name])


def _analyze_model(c: Checked, name: str, inputs: list[str]):
    p, d = c.pipeline, c.diagnostics
    m, a = p.models[name], c.analyses[name]
    rels = p.relations

    kinds = {op.kind for op in a.ops}
    for fn, table, col, pos in a.windows:
        base = rels.get(table)
        if base is None:
            continue
        if col not in base.rowtime:
            if not base.rowtime:
                d.error("PD331", f"model '{name}': {fn} window over '{table}', which has no time attribute",
                        _sql_loc(m, pos), node=name,
                        hint=f"declare `event_time` on the source feeding '{table}' and keep that column "
                             "unmodified in intermediate models")
            else:
                d.error("PD330", f"model '{name}': DESCRIPTOR({col}) is not a time attribute of '{table}'",
                        _sql_loc(m, pos), node=name,
                        hint=f"time attributes of '{table}': {', '.join(sorted(base.rowtime))}")
    columns, rowtime = infer_relation(m.sql, rels)
    c.inferred[name] = columns

    # --- temporal joins need a versioned table
    for r in a.refs:
        if r.temporal and r.name in p.sources:
            src = p.sources[r.name]
            spec = CATALOG.get(p.connections[src.connection].type) if src.connection in p.connections else None
            if spec is not None and spec.type == "jdbc":
                continue  # lookup join
            if not src.primary_key:
                d.error("PD340", f"model '{name}': temporal join on '{r.name}' requires it to have a primary_key",
                        _sql_loc(m, r.pos), node=name, hint="a versioned table is keyed by its primary key")
            if src.event_time is None:
                d.error("PD341", f"model '{name}': event-time temporal join on '{r.name}' requires `event_time` "
                        f"on source '{r.name}'", _sql_loc(m, r.pos), node=name)

    # --- changelog mode
    temporal_sides = {r.name for r in a.refs if r.temporal}
    updating_inputs = [i for i in inputs if rels.get(i) and rels[i].changelog == "updating"
                       and i not in temporal_sides]
    changelog = "updating" if (a.updating or updating_inputs) else "append"
    if updating_inputs and a.windows:
        d.warning("PD332", f"model '{name}': window aggregation over updating input(s) "
                  f"{', '.join(updating_inputs)}", m.loc, node=name,
                  hint="window TVF aggregations expect append-only input; check `pipec verify`")

    # --- output contract
    if m.columns is not None:
        declared = [col.name for col in m.columns]
        if columns is not None:
            inferred = [col.name for col in columns]
            if [x.lower() for x in inferred] != [x.lower() for x in declared]:
                d.error("PD310", f"model '{name}': declared columns {declared} do not match the query's "
                        f"output columns {inferred}", m.loc, node=name,
                        hint="the `columns` contract must list every output column, in SELECT order")
            else:
                for got, want in zip(columns, m.columns):
                    if got.type is not None and not got.type.same_base(want.type):
                        sev = d.error if got.name in rowtime else d.warning
                        sev("PD311", f"model '{name}': column '{got.name}' is {got.type} but the contract "
                            f"declares {want.type}" + ("; time attributes cannot be cast" if got.name in rowtime
                                                       else "; it will be CAST"), want.loc or m.loc, node=name)
        columns = [OutputColumn(col.name, col.type, (columns[i].expr if columns and i < len(columns) else ""))
                   for i, col in enumerate(m.columns)]

    # --- expectations
    _check_expectations(c, m, columns, changelog)

    # --- state
    unbounded = [op for op in a.ops if not op.bounded and op.kind in UNBOUNDED_KINDS]
    if unbounded and p.state_ttl_ms is None and not a.has_ttl_hint and not m.allow_unbounded_state:
        ops = "; ".join(op.detail for op in unbounded)
        d.error("PD350", f"model '{name}' keeps unbounded state ({ops}) and no state TTL is configured",
                m.loc, node=name,
                hint="set `defaults.state_ttl`, add a /*+ STATE_TTL(...) */ hint, rewrite with a window/interval "
                     "join, or set `allow_unbounded_state: true` if the key space is known to be bounded")
    elif unbounded and p.state_ttl_ms is not None and not a.has_ttl_hint:
        d.info("PD351", f"model '{name}': state for idle keys expires after the pipeline state_ttl; results "
               "for keys inactive longer than that restart from empty", m.loc, node=name)

    rels[name] = RelationInfo(columns=columns, rowtime=rowtime, changelog=changelog, inputs=inputs,
                              stateful_ops=list(a.ops), has_ttl_hint=a.has_ttl_hint,
                              upsert_key=_upsert_key(p, a, inputs))


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


def _check_expectations(c: Checked, m, columns, changelog):
    d = c.diagnostics
    known = {col.name.lower() for col in columns} if columns is not None else None
    for e in m.expectations:
        try:
            refs = referenced_columns(e.check)
        except SqlSyntaxError as err:
            d.error("PD321", f"model '{m.name}' expectation '{e.name}': {err}", e.loc, node=m.name)
            continue
        if known is not None:
            for col, _ in refs:
                if col.lower() not in known:
                    d.error("PD320", f"model '{m.name}' expectation '{e.name}' references unknown column '{col}'",
                            e.loc, hint=_suggest(col, [x.name for x in columns]), node=m.name)
        if e.on_violation == "quarantine":
            if c.pipeline.quarantine is None:
                d.error("PD322", f"model '{m.name}' expectation '{e.name}' quarantines rows but the pipeline "
                        "has no `quarantine:` destination", e.loc, node=m.name)
            if changelog != "append":
                d.error("PD323", f"model '{m.name}' is updating; quarantine expectations need append-only "
                        "models", e.loc, hint="move the expectation to an upstream append-only model",
                        node=m.name)
            if columns is None or any(col.type is None for col in columns):
                d.error("PD324", f"model '{m.name}': quarantine needs every output column type to be known",
                        e.loc, hint="declare a `columns:` contract on the model", node=m.name)


# --- sinks / quarantine / pipeline -----------------------------------------------------

def _check_sinks(c: Checked):
    p, d = c.pipeline, c.diagnostics
    for k in p.sinks.values():
        rel = p.relations.get(k.from_)
        if k.from_ not in p.relation_names():
            d.error("PD401", f"sink '{k.name}' reads from unknown table '{k.from_}'", k.loc,
                    hint=_suggest(k.from_, p.relation_names()), node=k.name)
        spec, props = _connector(c, k.connection, k.loc, k.name)
        if spec is not None:
            if "sink" not in spec.roles:
                d.error("PD403", f"connector '{spec.type}' cannot be used as a sink", k.loc, node=k.name)
                continue
            if k.mode not in spec.sink_modes:
                d.error("PD404", f"connector '{spec.type}' does not support mode '{k.mode}'", k.loc, node=k.name,
                        hint=f"supported modes: {', '.join(sorted(spec.sink_modes))}")
            opts, problems = build_options(spec, "sink", props, k.endpoint, k.format, k.mode,
                                           LowerCtx(p.name, k.name, k.connection, k.primary_key), k.options)
            _report_option_problems(c, problems, k.loc, k.name)
            c.options[k.name] = opts
        if k.mode == "upsert" and not k.primary_key:
            d.error("PD405", f"upsert sink '{k.name}' needs a primary_key", k.loc, node=k.name)
        if rel is None:
            continue
        if k.mode == "append" and rel.changelog == "updating":
            why = _updating_reason(p, k.from_)
            d.error("PD407", f"sink '{k.name}' is append-only but '{k.from_}' produces updates ({why})", k.loc,
                    node=k.name,
                    hint="use `mode: upsert` with a primary_key, or make the query append-only "
                         "(e.g. a window aggregation instead of a running GROUP BY)")
        if k.on_conflict and k.mode != "upsert":
            d.error("PD411", f"sink '{k.name}': on_conflict only applies to upsert sinks", k.loc, node=k.name)
        if k.mode == "upsert" and rel.changelog == "updating" and not k.on_conflict and k.primary_key:
            key = rel.upsert_key
            if key is None or sorted(x.lower() for x in key) != sorted(x.lower() for x in k.primary_key):
                d.warning("PD410", f"sink '{k.name}': cannot prove that the upsert key of '{k.from_}' "
                          f"({', '.join(key) if key else 'unknown'}) equals the sink primary key "
                          f"({', '.join(k.primary_key)}); Flink >= 2.3 will reject the INSERT", k.loc, node=k.name,
                          hint="set `on_conflict: error | nothing | deduplicate`, or key the sink by the query's "
                               "GROUP BY / source primary key (`pipec verify` gives the planner's answer)")
        if rel.columns is None:
            d.error("PD408", f"sink '{k.name}': cannot determine the output columns of '{k.from_}'", k.loc,
                    node=k.name, hint=f"give every SELECT item a name, or declare `columns:` on '{k.from_}'")
            continue
        unknown = [col.name for col in rel.columns if col.type is None]
        if unknown:
            d.error("PD408", f"sink '{k.name}': cannot infer the type of {', '.join(unknown)} in '{k.from_}'",
                    k.loc, node=k.name,
                    hint=f"declare a `columns:` contract on '{k.from_}' or CAST the expressions")
        names = {col.name.lower() for col in rel.columns}
        for key in k.primary_key:
            if key.lower() not in names:
                d.error("PD406", f"sink '{k.name}' primary_key column '{key}' is not an output column of "
                        f"'{k.from_}'", k.loc, hint=_suggest(key, [col.name for col in rel.columns]), node=k.name)


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
        c.diagnostics.error("PD403", f"connector '{spec.type}' cannot be used for quarantine", q.loc)
        return
    for m in p.models.values():
        if any(e.on_violation == "quarantine" for e in m.expectations):
            endpoint = {k: (v.format(pipeline=p.name, model=m.name) if isinstance(v, str) else v)
                        for k, v in q.endpoint.items()}
            node = f"{m.name}__quarantine"
            opts, problems = build_options(spec, "sink", props, endpoint, q.format, "append",
                                           LowerCtx(p.name, node, q.connection, []), {})
            _report_option_problems(c, problems, q.loc, "quarantine")
            c.options[node] = opts


def _check_pipeline(c: Checked):
    p, d = c.pipeline, c.diagnostics
    if not p.sinks:
        d.error("PD502", "pipeline has no sinks; nothing would run", p.loc)
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
        d.warning("PD501", f"{'source' if n in p.sources else 'model'} '{n}' does not feed any sink", node.loc,
                  hint="it will not be part of the deployed job", node=n)
