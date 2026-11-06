"""Load a pipeline project (pipeline.yaml + SQL files) into the IR.

Responsibilities: YAML parsing with line tracking, structural validation with
"did you mean" suggestions, environment/var resolution and secret collection.
Semantic checks live in validate.py.
"""
from __future__ import annotations

import difflib
import os
import re

import yaml

from .diagnostics import CompileError, Diagnostics, Location
from .ir import (Column, Connection, EventTime, Expectation, Model, Pipeline, Quarantine,
                 Sink, Source)
from .types import TypeParseError, parse_type

NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
VAR_RE = re.compile(r"\$\{(var|secret):([^}]+)\}")
DURATION_RE = re.compile(r"^\s*(\d+)\s*(ms|s|m|h|d)\s*$")
UNIT_MS = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}

TOP_KEYS = {"pipeline", "description", "owner", "defaults", "flink", "vars", "environments",
            "connections", "sources", "models", "sinks", "quarantine"}
SOURCE_COMMON = {"connection", "columns", "primary_key", "event_time", "processing_time",
                 "options", "description", "format"}
SINK_COMMON = {"from", "connection", "mode", "primary_key", "options", "description", "format", "on_conflict"}
MODEL_KEYS = {"sql", "sql_file", "columns", "expectations", "description", "allow_unbounded_state"}


# --- YAML with line numbers ------------------------------------------------------------

class LDict(dict):
    """dict that remembers the source line of itself and of each key."""

    line: int = 0
    key_lines: dict


class _Loader(yaml.SafeLoader):
    pass


def _construct_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    d = LDict()
    d.line = node.start_mark.line + 1
    d.key_lines = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        d[key] = loader.construct_object(value_node, deep=True)
        d.key_lines[key] = key_node.start_mark.line + 1
    return d


_Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def parse_duration_ms(value) -> int:
    if isinstance(value, int):
        return value * 1000
    m = DURATION_RE.match(str(value))
    if not m:
        raise ValueError(f"invalid duration '{value}' (expected e.g. 500ms, 5s, 10m, 1h, 7d)")
    return int(m.group(1)) * UNIT_MS[m.group(2)]


# --- Loader ----------------------------------------------------------------------------

class _Ctx:
    def __init__(self, file: str, diags: Diagnostics):
        self.file = file
        self.diags = diags

    def loc(self, d, key=None) -> Location:
        line = None
        if isinstance(d, LDict):
            line = d.key_lines.get(key, d.line) if key is not None else d.line
        return Location(self.file, line)

    def check_keys(self, d: dict, allowed: set, what: str, extra_ok: bool = False):
        if not isinstance(d, dict):
            return
        for k in d:
            if k in allowed or extra_ok:
                continue
            close = difflib.get_close_matches(str(k), sorted(allowed), n=1)
            self.diags.error("CD001", f"unknown key '{k}' in {what}", self.loc(d, k),
                             hint=f"did you mean '{close[0]}'?" if close else
                             f"allowed keys: {', '.join(sorted(allowed))}")

    def require_mapping(self, value, what, parent, key) -> dict:
        if value is None:
            return LDict()
        if not isinstance(value, dict):
            self.diags.error("CD002", f"{what} must be a mapping", self.loc(parent, key))
            return LDict()
        return value

    def check_name(self, name, what, parent):
        if not isinstance(name, str) or not NAME_RE.match(name):
            self.diags.error("CD003", f"invalid {what} name '{name}'", self.loc(parent, name),
                             hint="names must match [a-z_][a-z0-9_]* (lowercase snake_case)")


def load_pipeline(path: str, env: str | None = None) -> tuple[Pipeline, Diagnostics]:
    """Load a project directory or pipeline.yaml. Raises CompileError on structural errors."""
    file = os.path.join(path, "pipeline.yaml") if os.path.isdir(path) else path
    root = os.path.dirname(os.path.abspath(file))
    diags = Diagnostics()
    rel_file = os.path.relpath(file)
    ctx = _Ctx(rel_file, diags)
    try:
        with open(file) as f:
            doc = yaml.load(f, Loader=_Loader)
    except FileNotFoundError:
        diags.error("CD000", f"pipeline file not found: {file}")
        raise CompileError(diags)
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        diags.error("CD000", f"invalid YAML: {getattr(e, 'problem', e)}",
                    Location(rel_file, mark.line + 1 if mark else None))
        raise CompileError(diags)
    if not isinstance(doc, dict):
        diags.error("CD000", "pipeline.yaml must be a mapping at the top level", Location(rel_file))
        raise CompileError(diags)

    ctx.check_keys(doc, TOP_KEYS, "pipeline")
    secrets: set[str] = set()
    variables = _resolve_vars(doc, env, ctx)

    def subst(value, where_d, where_k):
        """Recursively substitute ${var:..}; collect ${secret:..} (kept as placeholders)."""
        if isinstance(value, str):
            def repl(m):
                kind, name = m.group(1), m.group(2).strip()
                if kind == "secret":
                    secrets.add(name)
                    return m.group(0)
                if name not in variables:
                    diags.error("CD004", f"undefined variable '{name}'", ctx.loc(where_d, where_k),
                                hint="define it under `vars:` (or the selected environment's vars)")
                    return m.group(0)
                return str(variables[name])
            out = VAR_RE.sub(repl, value)
            # a var may itself expand to a secret placeholder
            for m in VAR_RE.finditer(out):
                if m.group(1) == "secret":
                    secrets.add(m.group(2).strip())
            return out
        if isinstance(value, dict):
            nd = LDict()
            nd.line = getattr(value, "line", 0)
            nd.key_lines = getattr(value, "key_lines", {})
            for k, v in value.items():
                nd[k] = subst(v, value, k)
            return nd
        if isinstance(value, list):
            return [subst(v, where_d, where_k) for v in value]
        return value

    name = doc.get("pipeline")
    if not name:
        diags.error("CD005", "missing required key 'pipeline' (the pipeline name)", ctx.loc(doc))
        name = "unnamed"
    else:
        ctx.check_name(name, "pipeline", doc)

    defaults = ctx.require_mapping(doc.get("defaults"), "defaults", doc, "defaults")
    ctx.check_keys(defaults, {"state_ttl"}, "defaults")
    state_ttl_ms = None
    if defaults.get("state_ttl") is not None:
        try:
            state_ttl_ms = parse_duration_ms(defaults["state_ttl"])
        except ValueError as e:
            diags.error("CD006", str(e), ctx.loc(defaults, "state_ttl"))

    flink = ctx.require_mapping(doc.get("flink"), "flink", doc, "flink")
    ctx.check_keys(flink, {"config", "version"}, "flink")
    flink_config = {str(k): str(v) for k, v in
                    ctx.require_mapping(flink.get("config"), "flink.config", flink, "config").items()}

    connections = {}
    for cname, c in ctx.require_mapping(doc.get("connections"), "connections", doc, "connections").items():
        ctx.check_name(cname, "connection", doc["connections"])
        c = ctx.require_mapping(c, f"connection '{cname}'", doc["connections"], cname)
        ctx.check_keys(c, {"type", "properties"}, f"connection '{cname}'")
        if "type" not in c:
            diags.error("CD005", f"connection '{cname}' is missing 'type'", ctx.loc(c))
        props = subst(ctx.require_mapping(c.get("properties"), "properties", c, "properties"), c, "properties")
        connections[cname] = Connection(cname, str(c.get("type", "")), props, ctx.loc(c))

    sources = {}
    for sname, s in ctx.require_mapping(doc.get("sources"), "sources", doc, "sources").items():
        ctx.check_name(sname, "source", doc["sources"])
        s = subst(ctx.require_mapping(s, f"source '{sname}'", doc["sources"], sname), doc["sources"], sname)
        sources[sname] = _load_source(sname, s, ctx)

    models = {}
    for mname, m in ctx.require_mapping(doc.get("models"), "models", doc, "models").items():
        ctx.check_name(mname, "model", doc["models"])
        m = ctx.require_mapping(m, f"model '{mname}'", doc["models"], mname)
        models[mname] = _load_model(mname, m, ctx, root)

    sinks = {}
    for kname, k in ctx.require_mapping(doc.get("sinks"), "sinks", doc, "sinks").items():
        ctx.check_name(kname, "sink", doc["sinks"])
        k = subst(ctx.require_mapping(k, f"sink '{kname}'", doc["sinks"], kname), doc["sinks"], kname)
        sinks[kname] = _load_sink(kname, k, ctx)

    quarantine = None
    if doc.get("quarantine") is not None:
        q = subst(ctx.require_mapping(doc["quarantine"], "quarantine", doc, "quarantine"), doc, "quarantine")
        if "connection" not in q:
            diags.error("CD005", "quarantine is missing 'connection'", ctx.loc(q))
        endpoint = {k: v for k, v in q.items() if k not in ("connection", "format")}
        quarantine = Quarantine(str(q.get("connection", "")), endpoint, q.get("format"), ctx.loc(q))

    if diags.has_errors:
        raise CompileError(diags)

    p = Pipeline(
        name=name, file=rel_file, root_dir=root, env=env,
        description=doc.get("description"), owner=doc.get("owner"),
        state_ttl_ms=state_ttl_ms, flink_config=flink_config,
        connections=connections, sources=sources, models=models, sinks=sinks,
        quarantine=quarantine, secrets=secrets, loc=ctx.loc(doc),
    )
    return p, diags


def _resolve_vars(doc, env, ctx: _Ctx) -> dict:
    variables = dict(ctx.require_mapping(doc.get("vars"), "vars", doc, "vars"))
    envs = ctx.require_mapping(doc.get("environments"), "environments", doc, "environments")
    if env is not None:
        if env not in envs:
            ctx.diags.error("CD007", f"unknown environment '{env}'", ctx.loc(doc, "environments"),
                            hint=f"defined environments: {', '.join(envs) or '(none)'}")
            raise CompileError(ctx.diags)
        e = ctx.require_mapping(envs[env], f"environment '{env}'", envs, env)
        ctx.check_keys(e, {"vars"}, f"environment '{env}'")
        variables.update(ctx.require_mapping(e.get("vars"), "vars", e, "vars"))
    return variables


def _load_columns(raw, ctx: _Ctx, parent, key, what) -> list[Column]:
    cols = []
    raw = ctx.require_mapping(raw, f"{what} columns", parent, key)
    for cname, ctype in raw.items():
        if not isinstance(cname, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", cname):
            ctx.diags.error("CD003", f"invalid column name '{cname}' in {what}", ctx.loc(raw, cname))
            continue
        try:
            cols.append(Column(cname, parse_type(ctype), ctx.loc(raw, cname)))
        except TypeParseError as e:
            ctx.diags.error("CD010", f"bad type for column '{cname}' in {what}: {e}", ctx.loc(raw, cname),
                            hint="use Flink SQL types, e.g. STRING, BIGINT, DECIMAL(10, 2), TIMESTAMP(3)")
    return cols


def _str_list(value, ctx, parent, key, what) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return list(value)
    ctx.diags.error("CD002", f"{what} must be a list of column names", ctx.loc(parent, key))
    return []


def _load_source(name, s, ctx: _Ctx) -> Source:
    what = f"source '{name}'"
    if "connection" not in s:
        ctx.diags.error("CD005", f"{what} is missing 'connection'", ctx.loc(s))
    if "columns" not in s:
        ctx.diags.error("CD005", f"{what} is missing 'columns'", ctx.loc(s),
                        hint="sources must declare their schema; it becomes the table DDL")
    event_time = None
    et = s.get("event_time")
    if et is not None:
        if isinstance(et, str):
            et_d = LDict(column=et)
            et_d.line, et_d.key_lines = s.key_lines.get("event_time", 0), {}
            et = et_d
        et = ctx.require_mapping(et, "event_time", s, "event_time")
        ctx.check_keys(et, {"column", "max_out_of_orderness"}, f"{what}.event_time")
        try:
            delay = parse_duration_ms(et.get("max_out_of_orderness", "0s"))
        except ValueError as e:
            ctx.diags.error("CD006", str(e), ctx.loc(et, "max_out_of_orderness"))
            delay = 0
        event_time = EventTime(str(et.get("column", "")), delay, ctx.loc(et))
    endpoint = {k: v for k, v in s.items() if k not in SOURCE_COMMON}
    return Source(
        name=name, connection=str(s.get("connection", "")), endpoint=endpoint,
        format=s.get("format"),
        columns=_load_columns(s.get("columns"), ctx, s, "columns", what),
        primary_key=_str_list(s.get("primary_key"), ctx, s, "primary_key", "primary_key"),
        event_time=event_time, processing_time=s.get("processing_time"),
        options={str(k): str(v) for k, v in ctx.require_mapping(s.get("options"), "options", s, "options").items()},
        description=s.get("description"), loc=ctx.loc(s),
    )


def _load_model(name, m, ctx: _Ctx, root: str) -> Model:
    what = f"model '{name}'"
    ctx.check_keys(m, MODEL_KEYS, what)
    sql, sql_loc = "", None
    if ("sql" in m) == ("sql_file" in m):
        ctx.diags.error("CD011", f"{what} must have exactly one of 'sql' or 'sql_file'", ctx.loc(m))
    elif "sql" in m:
        sql = str(m["sql"])
        # block scalars start on the line after the key
        sql_loc = Location(ctx.file, m.key_lines["sql"] + (1 if sql.endswith("\n") else 0))
    else:
        path = os.path.join(root, str(m["sql_file"]))
        try:
            with open(path) as f:
                sql = f.read()
            sql_loc = Location(os.path.relpath(path), 1)
        except OSError:
            ctx.diags.error("CD012", f"{what}: cannot read sql_file '{m['sql_file']}'", ctx.loc(m, "sql_file"))

    expectations = []
    raw_exp = m.get("expectations") or []
    if not isinstance(raw_exp, list):
        ctx.diags.error("CD002", f"{what}.expectations must be a list", ctx.loc(m, "expectations"))
        raw_exp = []
    seen = set()
    for e in raw_exp:
        e = ctx.require_mapping(e, "expectation", m, "expectations")
        ctx.check_keys(e, {"name", "check", "on_violation"}, f"{what} expectation")
        ename, check = e.get("name"), e.get("check")
        action = e.get("on_violation", "fail")
        if not ename or not check:
            ctx.diags.error("CD005", "expectations need 'name' and 'check'", ctx.loc(e))
            continue
        ctx.check_name(ename, "expectation", e)
        if ename in seen:
            ctx.diags.error("CD013", f"duplicate expectation '{ename}' in {what}", ctx.loc(e))
        seen.add(ename)
        if action not in ("drop", "fail", "quarantine"):
            ctx.diags.error("CD014", f"on_violation must be drop, fail or quarantine (got '{action}')",
                            ctx.loc(e, "on_violation"))
            continue
        expectations.append(Expectation(ename, str(check), action, ctx.loc(e)))

    return Model(
        name=name, sql=sql, sql_loc=sql_loc,
        columns=_load_columns(m["columns"], ctx, m, "columns", what) if "columns" in m else None,
        expectations=expectations,
        allow_unbounded_state=bool(m.get("allow_unbounded_state", False)),
        description=m.get("description"), loc=ctx.loc(m),
    )


def _load_sink(name, k, ctx: _Ctx) -> Sink:
    what = f"sink '{name}'"
    for req in ("from", "connection"):
        if req not in k:
            ctx.diags.error("CD005", f"{what} is missing '{req}'", ctx.loc(k))
    mode = k.get("mode", "append")
    if mode not in ("append", "upsert"):
        ctx.diags.error("CD014", f"{what}: mode must be 'append' or 'upsert' (got '{mode}')", ctx.loc(k, "mode"))
        mode = "append"
    on_conflict = k.get("on_conflict")
    if on_conflict is not None and on_conflict not in ("error", "nothing", "deduplicate"):
        ctx.diags.error("CD014", f"{what}: on_conflict must be error, nothing or deduplicate (got '{on_conflict}')",
                        ctx.loc(k, "on_conflict"))
        on_conflict = None
    endpoint = {kk: v for kk, v in k.items() if kk not in SINK_COMMON}
    return Sink(
        name=name, from_=str(k.get("from", "")), connection=str(k.get("connection", "")),
        endpoint=endpoint, format=k.get("format"), mode=mode,
        primary_key=_str_list(k.get("primary_key"), ctx, k, "primary_key", "primary_key"),
        options={str(kk): str(v) for kk, v in ctx.require_mapping(k.get("options"), "options", k, "options").items()},
        on_conflict=on_conflict, description=k.get("description"), loc=ctx.loc(k),
    )
