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
from .ir import (Column, Connection, EventTime, Expectation, Function, MLModel, Model, Pipeline, Quarantine,
                 Scaling, Sink, Source)
from .connectors import optstr
from .types import TypeParseError, parse_type

NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
VAR_RE = re.compile(r"\$\{(var|secret):([^}]+)\}")
DURATION_RE = re.compile(r"^\s*(\d+)\s*(ms|s|m|h|d)\s*$")
UNIT_MS = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}

TOP_KEYS = {"pipeline", "description", "owner", "defaults", "flink", "vars", "environments",
            "connections", "sources", "models", "sinks", "quarantine", "scaling", "deploy", "functions", "mode",
            "ml_models", "replicate"}
SOURCE_COMMON = {"connection", "columns", "primary_key", "event_time", "processing_time",
                 "options", "description", "format"}
SINK_COMMON = {"from", "connection", "mode", "primary_key", "options", "description", "format", "on_conflict",
               "metadata"}
MODEL_KEYS = {"process", "sql", "sql_file", "columns", "expectations", "description", "allow_unbounded_state", "scaling",
              "backfill"}
FUNCTION_KINDS = ("scalar", "aggregate", "table", "process_table")
DEPLOY_KEYS = {"image", "namespace", "service_account", "flink_version", "jobmanager", "taskmanager",
               "checkpoints", "savepoints", "upgrade_mode", "secrets", "artifacts", "labels", "flink_config",
               "jars", "pod_template", "target", "session_cluster", "jar_uri", "run", "monitoring", "secrets_from",
               "high_availability"}
SCALING_KEYS = {"max_parallelism", "parallelism", "min", "max", "target_utilization", "max_lag", "autoscale"}


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


def _ldict(value):
    """Recursively convert plain dicts to LDict so line lookups work uniformly."""
    if isinstance(value, dict):
        d = LDict()
        d.line = getattr(value, "line", 0)
        d.key_lines = dict(getattr(value, "key_lines", {}) or {})
        for k, v in value.items():
            d[k] = _ldict(v)
        return d
    if isinstance(value, list):
        return [_ldict(v) for v in value]
    return value


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
        return Location(self.file, line or None)

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
    return load_document(doc, rel_file, root, env, diags)


def load_document(doc: dict, rel_file: str, root: str, env: str | None,
                  diags: Diagnostics | None = None) -> tuple[Pipeline, Diagnostics]:
    """Build the IR from an already-parsed document (YAML, or the Python API's equivalent).

    Mappings may be LDict instances carrying source lines; plain dicts work too."""
    diags = diags or Diagnostics()
    ctx = _Ctx(rel_file, diags)
    doc = _ldict(doc)
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
    flink_config = {str(k): optstr(v) for k, v in
                    ctx.require_mapping(flink.get("config"), "flink.config", flink, "config").items()}

    connections = {}
    for cname, c in ctx.require_mapping(doc.get("connections"), "connections", doc, "connections").items():
        ctx.check_name(cname, "connection", doc["connections"])
        c = ctx.require_mapping(c, f"connection '{cname}'", doc["connections"], cname)
        custom_keys = {"connector", "artifacts", "changelog", "sink_modes", "requires_primary_key"}
        ctx.check_keys(c, {"type", "properties"} | (custom_keys if c.get("type") == "custom" else set()),
                       f"connection '{cname}'")
        if "type" not in c:
            diags.error("CD005", f"connection '{cname}' is missing 'type'", ctx.loc(c))
        props = subst(ctx.require_mapping(c.get("properties"), "properties", c, "properties"), c, "properties")
        custom = {k: c[k] for k in custom_keys if k in c}
        if c.get("type") == "custom":
            if not c.get("connector"):
                diags.error("CD005", f"custom connection '{cname}' is missing 'connector' (the Flink "
                            "'connector' option, e.g. 'clickhouse')", ctx.loc(c))
            arts = c.get("artifacts") or []
            if not isinstance(arts, list) or any(len(str(a).split(":")) != 3 for a in arts):
                diags.error("CD005", f"custom connection '{cname}': artifacts must be a list of Maven "
                            "coordinates 'group:artifact:version'", ctx.loc(c, "artifacts"))
                custom["artifacts"] = []
            if c.get("changelog", "append") not in ("append", "updating", "by_format"):
                diags.error("CD005", f"custom connection '{cname}': changelog must be append, updating or "
                            "by_format", ctx.loc(c, "changelog"))
        connections[cname] = Connection(cname, str(c.get("type", "")), props, ctx.loc(c), custom)

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
        pr = models[mname].process
        for side, spec in (pr or {}).get("side_outputs", {}).items():
            sname = f"{mname}_{side}"  # each side output is a relation of its own
            if sname in doc["models"]:
                diags.error("CD101", f"side output '{side}' of model '{mname}' collides with model '{sname}'",
                            ctx.loc(m, "process"))
            models[sname] = Model(name=sname, sql="", sql_loc=None, columns=spec["columns"], expectations=[],
                                  description=f"side output '{side}' of process model '{mname}'", loc=ctx.loc(m),
                                  process={"side_of": mname, "side": side, "event_time": spec.get("event_time")})

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

    scaling = None
    if doc.get("scaling") is not None:
        scaling = _load_scaling(subst(ctx.require_mapping(doc["scaling"], "scaling", doc, "scaling"), doc, "scaling"),
                                ctx, SCALING_KEYS, "scaling")

    functions = {}
    for fname, fdef in ctx.require_mapping(doc.get("functions"), "functions", doc, "functions").items():
        ctx.check_name(fname, "function", doc["functions"])
        fdef = subst(ctx.require_mapping(fdef, f"function '{fname}'", doc["functions"], fname), doc["functions"], fname)
        fn = _load_function(fname, fdef, ctx)
        if fn is not None:
            functions[fname] = fn

    ml_models = {}
    for mname, mdef in ctx.require_mapping(doc.get("ml_models"), "ml_models", doc, "ml_models").items():
        ctx.check_name(mname, "ml model", doc["ml_models"])
        mdef = subst(ctx.require_mapping(mdef, f"ml model '{mname}'", doc["ml_models"], mname), doc["ml_models"],
                     mname)
        ml_models[mname] = _load_ml_model(mname, mdef, ctx)

    replicate = None
    if doc.get("replicate") is not None:
        from .replicate import KEYS as REPLICATE_KEYS
        replicate = subst(ctx.require_mapping(doc["replicate"], "replicate", doc, "replicate"), doc, "replicate")
        ctx.check_keys(replicate, REPLICATE_KEYS, "replicate")
        replicate = dict(replicate)
        replicate["__loc"] = ctx.loc(doc, "replicate")

    deploy = None
    if doc.get("deploy") is not None:
        deploy = _load_deploy(subst(ctx.require_mapping(doc["deploy"], "deploy", doc, "deploy"), doc, "deploy"), ctx)

    mode = str(doc.get("mode", "streaming"))
    if mode not in ("streaming", "batch"):
        diags.error("CD006", f"mode must be streaming or batch (got '{mode}')", ctx.loc(doc, "mode"))
    if diags.has_errors:
        raise CompileError(diags)

    p = Pipeline(
        name=name, file=rel_file, root_dir=root, env=env,
        description=doc.get("description"), owner=doc.get("owner"),
        state_ttl_ms=state_ttl_ms, flink_config=flink_config,
        connections=connections, sources=sources, models=models, sinks=sinks,
        quarantine=quarantine, secrets=secrets, scaling=scaling, deploy=deploy, functions=functions, loc=ctx.loc(doc),
        mode=mode, ml_models=ml_models, replicate=replicate,
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


def _load_function(name, f, ctx: _Ctx) -> Function | None:
    what = f"function '{name}'"
    lang = f.get("language")
    if lang not in ("java", "python"):
        ctx.diags.error("CD014", f"{what}: language must be java or python", ctx.loc(f, "language") if "language" in f
                        else ctx.loc(f))
        return None
    kind = f.get("kind", "scalar")
    if kind not in FUNCTION_KINDS:
        ctx.diags.error("CD014", f"{what}: kind must be one of {', '.join(FUNCTION_KINDS)}", ctx.loc(f, "kind"))
        return None
    if kind == "process_table" and lang != "java":
        ctx.diags.error("CD014", f"{what}: process table functions are Java only (PyFlink has no "
                        "ProcessTableFunction); use a Python process model", ctx.loc(f, "kind"))
        return None
    allowed = {"language", "returns", "deterministic", "description", "kind"} | (
        {"class", "jar"} if lang == "java" else
        {"file", "class", "accumulator"} if kind == "aggregate" else {"file", "function"})
    ctx.check_keys(f, allowed, what)
    returns = None
    if f.get("returns") is not None:
        try:
            returns = parse_type(f["returns"])
        except TypeParseError as e:
            ctx.diags.error("CD010", f"{what}: bad return type: {e}", ctx.loc(f, "returns"))
    fn = Function(name=name, language=lang, returns=returns, deterministic=bool(f.get("deterministic", True)),
                  description=f.get("description"), loc=ctx.loc(f), kind=kind)
    if kind in ("table", "process_table") and returns is not None and returns.name != "ROW":
        ctx.diags.error("CD010", f"{what}: a {kind.replace('_', ' ')} function returns rows: "
                        "`returns: ROW<name TYPE, ...>`", ctx.loc(f, "returns"))
    if lang == "java":
        if not f.get("class"):
            ctx.diags.error("CD005", f"{what} is missing 'class' (the fully qualified ScalarFunction class)",
                            ctx.loc(f))
        fn.class_name, fn.jar = f.get("class"), f.get("jar")
    else:
        if not f.get("file"):
            ctx.diags.error("CD005", f"{what} is missing 'file' (a .py module in the project)", ctx.loc(f))
        if returns is None:
            ctx.diags.error("CD005", f"{what}: Python functions need `returns` (the Flink SQL result type)",
                            ctx.loc(f))
        fn.file, fn.function = f.get("file"), f.get("function") or name
        if kind == "aggregate":
            fn.function, fn.class_attr = None, f.get("class")
            if not fn.class_attr:
                ctx.diags.error("CD005", f"{what}: a Python aggregate function names its `class` (with "
                                "create_accumulator, accumulate and get_value)", ctx.loc(f))
            try:
                fn.accumulator = parse_type(f.get("accumulator", ""))
            except TypeParseError as e:
                ctx.diags.error("CD005", f"{what}: Python aggregate functions need `accumulator`, the Flink type "
                                f"of what create_accumulator returns ({e})", ctx.loc(f))
    return fn


def _load_ml_model(name, m, ctx: _Ctx) -> MLModel:
    what = f"ml model '{name}'"
    ctx.check_keys(m, {"provider", "input", "output", "properties", "artifacts", "description"}, what)
    for req in ("provider", "input", "output"):
        if req not in m:
            ctx.diags.error("CD005", f"{what} is missing '{req}'", ctx.loc(m))
    # Flink's model options are hyphenated (api-key, system-prompt); YAML keys may use underscores
    props = {str(k).replace("_", "-"): v
             for k, v in ctx.require_mapping(m.get("properties"), f"{what}.properties", m, "properties").items()}
    return MLModel(name=name, provider=str(m.get("provider", "")),
                   input=_load_columns(m.get("input"), ctx, m, "input", what),
                   output=_load_columns(m.get("output"), ctx, m, "output", what),
                   properties=props, artifacts=[str(a) for a in (m.get("artifacts") or [])],
                   description=m.get("description"), loc=ctx.loc(m))


def _load_deploy(d, ctx: _Ctx) -> dict:
    ctx.check_keys(d, DEPLOY_KEYS, "deploy")
    for key in ("jobmanager", "taskmanager"):
        r = ctx.require_mapping(d.get(key), f"deploy.{key}", d, key)
        ctx.check_keys(r, {"memory", "cpu", "slots", "replicas"}, f"deploy.{key}")
    cp = ctx.require_mapping(d.get("checkpoints"), "deploy.checkpoints", d, "checkpoints")
    ctx.check_keys(cp, {"interval", "dir", "mode", "timeout", "min_pause"}, "deploy.checkpoints")
    for key in ("interval", "timeout", "min_pause"):
        if cp.get(key) is not None:
            try:
                parse_duration_ms(cp[key])
            except ValueError as e:
                ctx.diags.error("CD006", str(e), ctx.loc(cp, key))
    ha = d.get("high_availability")
    if ha is not None and not isinstance(ha, bool):
        ctx.check_keys(ctx.require_mapping(ha, "deploy.high_availability", d, "high_availability"),
                       {"storage_dir"}, "deploy.high_availability")
    if d.get("upgrade_mode", "savepoint") not in ("savepoint", "last-state", "stateless"):
        ctx.diags.error("CD014", "deploy.upgrade_mode must be savepoint, last-state or stateless",
                        ctx.loc(d, "upgrade_mode"))
    for key, value in d.items():
        if key != "secrets" and VAR_RE.search(str(value)) and "${secret:" in str(value):
            ctx.diags.error("CD530", f"deploy.{key} cannot use secrets; they are injected into the job's SQL only",
                            ctx.loc(d, key), hint="put the value in the Flink deployment's own configuration")
    return _plain(d)


def _plain(v):
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_plain(x) for x in v]
    return v


def _load_scaling(d, ctx: _Ctx, allowed: set, what: str) -> Scaling:
    ctx.check_keys(d, allowed, what)
    sc = Scaling(loc=ctx.loc(d))
    for key in ("max_parallelism", "parallelism", "min", "max"):
        v = d.get(key)
        if v is None:
            continue
        if isinstance(v, bool) or not isinstance(v, int) and not (isinstance(v, str) and v.isdigit()):
            ctx.diags.error("CD002", f"{what}.{key} must be a positive integer", ctx.loc(d, key))
            continue
        setattr(sc, key, int(v))
    if d.get("target_utilization") is not None:
        try:
            sc.target_utilization = float(d["target_utilization"])
        except (TypeError, ValueError):
            ctx.diags.error("CD002", f"{what}.target_utilization must be a number between 0 and 1",
                            ctx.loc(d, "target_utilization"))
    if d.get("max_lag") is not None:
        try:
            sc.max_lag_ms = parse_duration_ms(d["max_lag"])
        except ValueError as e:
            ctx.diags.error("CD006", str(e), ctx.loc(d, "max_lag"))
    if d.get("autoscale") is not None:
        if not isinstance(d["autoscale"], bool):
            ctx.diags.error("CD002", f"{what}.autoscale must be true or false", ctx.loc(d, "autoscale"))
        else:
            sc.autoscale = d["autoscale"]
    return sc


META_RE = re.compile(r"^(.+?)\s+METADATA(?:\s+FROM\s+'([^']+)')?(\s+VIRTUAL)?$", re.IGNORECASE)


def _load_columns(raw, ctx: _Ctx, parent, key, what) -> list[Column]:
    cols = []
    raw = ctx.require_mapping(raw, f"{what} columns", parent, key)
    for cname, ctype in raw.items():
        if not isinstance(cname, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", cname):
            ctx.diags.error("CD003", f"invalid column name '{cname}' in {what}", ctx.loc(raw, cname))
            continue
        meta, virtual, expression = None, False, None
        if isinstance(ctype, dict):
            spec = ctype
            ctx.check_keys(spec, {"type", "metadata", "virtual", "expression"}, f"{what} column '{cname}'")
            ctype = spec.get("type")
            if ctype is None:
                ctx.diags.error("CD005", f"column '{cname}' in {what} needs a 'type'", ctx.loc(raw, cname))
                continue
            if spec.get("metadata") is not None:
                meta = cname if spec["metadata"] is True else str(spec["metadata"])
            virtual = bool(spec.get("virtual", False))
            if spec.get("expression") is not None:
                expression = str(spec["expression"]).strip()
            if meta and expression:
                ctx.diags.error("CD005", f"column '{cname}' in {what}: give either 'metadata' or 'expression'",
                                ctx.loc(raw, cname))
            if virtual and not meta:
                ctx.diags.error("CD005", f"column '{cname}' in {what}: 'virtual' applies to metadata columns",
                                ctx.loc(raw, cname))
        else:
            mm = META_RE.match(" ".join(str(ctype).split()))
            if mm:
                ctype, meta, virtual = mm.group(1), mm.group(2) or cname, bool(mm.group(3))
        try:
            cols.append(Column(cname, parse_type(ctype), ctx.loc(raw, cname), meta, virtual, expression))
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
    imported = "from_pipeline" in s
    if imported:
        for k in ("connection", "columns", "format"):
            if k in s:
                ctx.diags.error("CD005", f"{what} reads another pipeline's sink: its '{k}' comes from that sink",
                                ctx.loc(s, k))
        if "sink" not in s:
            ctx.diags.error("CD005", f"{what} has from_pipeline but no 'sink'", ctx.loc(s))
    elif "connection" not in s:
        ctx.diags.error("CD005", f"{what} is missing 'connection'", ctx.loc(s))
    if "columns" not in s and not imported:
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
        ctx.check_keys(et, {"column", "max_out_of_orderness", "idle_timeout", "watermark", "alignment", "emit"},
                       f"{what}.event_time")
        try:
            delay = parse_duration_ms(et.get("max_out_of_orderness", "0s"))
        except ValueError as e:
            ctx.diags.error("CD006", str(e), ctx.loc(et, "max_out_of_orderness"))
            delay = 0
        idle = None
        if et.get("idle_timeout") is not None:
            try:
                idle = parse_duration_ms(et.get("idle_timeout"))
            except ValueError as e:
                ctx.diags.error("CD006", str(e), ctx.loc(et, "idle_timeout"))
        alignment = None
        if et.get("alignment") is not None:
            al = ctx.require_mapping(et.get("alignment"), "alignment", et, "alignment")
            ctx.check_keys(al, {"group", "max_drift", "update_interval"}, f"{what}.event_time.alignment")
            try:
                alignment = {"group": str(al.get("group", "")),
                             "max_drift_ms": parse_duration_ms(al.get("max_drift", "0s")),
                             "update_interval_ms": (parse_duration_ms(al["update_interval"])
                                                    if al.get("update_interval") is not None else None)}
            except ValueError as e:
                ctx.diags.error("CD006", str(e), ctx.loc(et, "alignment"))
            if alignment and (not alignment["group"] or not alignment["max_drift_ms"]):
                ctx.diags.error("CD005", f"{what}.event_time.alignment needs 'group' and 'max_drift'",
                                ctx.loc(et, "alignment"))
        emit = et.get("emit")
        if emit is not None and emit not in ("on_event", "on_periodic"):
            ctx.diags.error("CD006", f"{what}.event_time.emit must be on_event or on_periodic", ctx.loc(et, "emit"))
        expression = str(et["watermark"]).strip() if et.get("watermark") is not None else None
        if expression and "max_out_of_orderness" in et:
            ctx.diags.error("CD005", f"{what}.event_time: give either 'watermark' or 'max_out_of_orderness'",
                            ctx.loc(et, "watermark"))
        event_time = EventTime(str(et.get("column", "")), delay, ctx.loc(et), idle, expression, alignment, emit)
    endpoint = {k: v for k, v in s.items() if k not in SOURCE_COMMON | {"from_pipeline", "sink"}}
    return Source(
        name=name, connection=str(s.get("connection", "")), endpoint=endpoint,
        format=s.get("format"),
        columns=_load_columns(s.get("columns"), ctx, s, "columns", what),
        primary_key=_str_list(s.get("primary_key"), ctx, s, "primary_key", "primary_key"),
        event_time=event_time, processing_time=s.get("processing_time"),
        options={str(k): str(v) for k, v in ctx.require_mapping(s.get("options"), "options", s, "options").items()},
        description=s.get("description"), loc=ctx.loc(s),
        from_pipeline={"path": str(s["from_pipeline"]), "sink": str(s.get("sink", ""))} if imported else None,
    )


def _load_model(name, m, ctx: _Ctx, root: str) -> Model:
    what = f"model '{name}'"
    ctx.check_keys(m, MODEL_KEYS, what)
    sql, sql_loc = "", None
    process = None
    if "process" in m:
        if "sql" in m or "sql_file" in m:
            ctx.diags.error("CD011", f"{what}: a process model has no 'sql'", ctx.loc(m, "process"))
        process = _load_process(name, m, ctx, root)
    elif ("sql" in m) == ("sql_file" in m):
        ctx.diags.error("CD011", f"{what} must have exactly one of 'sql', 'sql_file' or 'process'", ctx.loc(m))
    elif "sql" in m:
        sql = str(m["sql"])
        # block scalars start on the line after the key
        line = m.key_lines.get("sql") or 0
        sql_loc = Location(ctx.file, line + (1 if sql.endswith("\n") else 0)) if line else Location(ctx.file)
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
        backfill=bool(m.get("backfill", False)),
        description=m.get("description"),
        scaling=_load_scaling(ctx.require_mapping(m["scaling"], f"{what}.scaling", m, "scaling"), ctx,
                              {"min", "max"}, f"{what}.scaling") if m.get("scaling") is not None else None,
        loc=ctx.loc(m),
        process=process,
    )


PROCESS_KEYS = {"language", "file", "function", "exact_watermarks", "class", "jar", "inputs", "key_by", "config", "changelog", "primary_key", "event_time",
                "side_outputs"}


def _load_process(name, m, ctx: _Ctx, root: str) -> dict | None:
    what = f"model '{name}'.process"
    pr = ctx.require_mapping(m.get("process"), what, m, "process")
    ctx.check_keys(pr, PROCESS_KEYS, what)
    if "columns" not in m:
        ctx.diags.error("CD005", f"model '{name}': a process model must declare its output `columns`", ctx.loc(m))
    language = str(pr.get("language", "java"))
    if language not in ("java", "python"):
        ctx.diags.error("CD006", f"{what}.language must be java or python", ctx.loc(pr, "language"))
    for k in (("class", "inputs") if language == "java" else ("file", "function", "inputs")):
        if not pr.get(k):
            ctx.diags.error("CD005", f"{what} is missing '{k}'", ctx.loc(pr))
    if language == "python" and pr.get("file"):
        from .udfs import check_python_module
        problem = check_python_module(os.path.join(root, str(pr["file"])), str(pr.get("function", "")))
        if problem:
            ctx.diags.error("CD160", f"{what}: {problem}", ctx.loc(pr, "file"))
    changelog = str(pr.get("changelog", "append"))
    if changelog not in ("append", "upsert", "retract"):
        ctx.diags.error("CD006", f"{what}.changelog must be append, upsert or retract", ctx.loc(pr, "changelog"))
    pk = _str_list(pr.get("primary_key"), ctx, pr, "primary_key", "primary_key")
    if changelog == "upsert" and not pk:
        ctx.diags.error("CD005", f"{what}: changelog upsert needs a primary_key", ctx.loc(pr, "changelog"))
    sides = {}
    for side, spec in ctx.require_mapping(pr.get("side_outputs"), f"{what}.side_outputs", pr, "side_outputs").items():
        spec = ctx.require_mapping(spec, f"{what}.side_outputs.{side}", pr["side_outputs"], side)
        ctx.check_keys(spec, {"columns", "event_time"}, f"{what}.side_outputs.{side}")
        ctx.check_name(side, "side output", pr["side_outputs"])
        sides[str(side)] = {"columns": _load_columns(spec.get("columns"), ctx, spec, "columns", f"side output '{side}'"),
                            "event_time": spec.get("event_time")}
    jar = pr.get("jar")
    if jar is not None and not os.path.exists(os.path.join(root, str(jar))):
        ctx.diags.warning("CD164", f"{what}: jar '{jar}' does not exist (build it first)", ctx.loc(pr, "jar"))
    if pr.get("exact_watermarks") and language != "python":
        ctx.diags.error("CD005", f"{what}: exact_watermarks is for Python process models (Java ones see every "
                        "watermark already)", ctx.loc(pr, "exact_watermarks"))
    return {"language": language, "exact_watermarks": bool(pr.get("exact_watermarks", False)),
            "file": str(pr["file"]) if pr.get("file") else None,
            "function": str(pr["function"]) if pr.get("function") else None,
            "class": str(pr.get("class", "")), "jar": str(jar) if jar is not None else None,
            "inputs": _str_list(pr.get("inputs"), ctx, pr, "inputs", "inputs"),
            "key_by": _str_list(pr.get("key_by"), ctx, pr, "key_by", "key_by"),
            "config": {str(k): str(v) for k, v in ctx.require_mapping(pr.get("config"), "config", pr, "config").items()},
            "changelog": changelog, "primary_key": pk, "event_time": pr.get("event_time"), "side_outputs": sides}


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
        metadata={str(c): (str(c) if v is True else str(v))
                  for c, v in ctx.require_mapping(k.get("metadata"), "metadata", k, "metadata").items()},
    )
