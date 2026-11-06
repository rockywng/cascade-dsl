"""Python front end. Builds the same document as pipeline.yaml, so validation, lowering,
verify, tests and upgrade checks behave identically for both.

    from cascade_dsl.api import Pipeline, kafka, secret, expect, sum_, count

    p = Pipeline("orders_analytics", state_ttl="30d")
    events = p.connection("events", kafka(bootstrap_servers=p.var("kafka_bootstrap")))
    orders = p.source("orders", events, topic="orders.v1", format="json",
                      columns={"order_id": "STRING NOT NULL", "amount": "DECIMAL(12, 2)",
                               "order_ts": "TIMESTAMP(3)"},
                      event_time=("order_ts", "5s"))

    @p.model(expectations=[expect.drop("positive_amount", "amount > 0")])
    def big_orders(orders):                     # parameters name the inputs
        return f"SELECT * FROM {orders} WHERE amount > 100"

    @p.model
    def revenue(orders):                        # or use the builder
        return orders.tumble("order_ts", "1m").group_by().agg(revenue=sum_("amount"))

    p.sink("revenue_out", revenue, "print")
"""
from __future__ import annotations

import inspect
import os
import runpy
import sys
from dataclasses import dataclass, field

from .builder import Query, avg, count, count_distinct, interval, lit, max_, min_, sum_  # noqa: F401
from .diagnostics import CompileError, Diagnostics, Location
from .loader import LDict, load_document

_THIS = {os.path.abspath(__file__), os.path.abspath(os.path.join(os.path.dirname(__file__), "builder.py"))}


def _caller_line() -> int:
    for frame in inspect.stack()[1:]:
        if os.path.abspath(frame.filename) not in _THIS:
            return frame.lineno
    return 0


def _ld(mapping: dict, line: int) -> LDict:
    d = LDict()
    d.line = line
    d.key_lines = {}
    for k, v in mapping.items():
        if v is None:
            continue
        d[k] = v
        d.key_lines[k] = line
    return d


def secret(path: str) -> str:
    """Reference to a secret injected at deploy time; never resolved by the compiler."""
    return "${secret:" + path + "}"


@dataclass
class ConnectorSpec:
    type: str
    properties: dict


def connector(type: str, **properties) -> ConnectorSpec:
    return ConnectorSpec(type, properties)


def kafka(**properties) -> ConnectorSpec: return connector("kafka", **properties)
def postgres_cdc(**properties) -> ConnectorSpec: return connector("postgres-cdc", **properties)
def mysql_cdc(**properties) -> ConnectorSpec: return connector("mysql-cdc", **properties)
def iceberg(**properties) -> ConnectorSpec: return connector("iceberg", **properties)
def jdbc(**properties) -> ConnectorSpec: return connector("jdbc", **properties)
def filesystem(**properties) -> ConnectorSpec: return connector("filesystem", **properties)


class expect:
    """Expectation constructors: expect.fail / expect.drop / expect.quarantine (name, check)."""

    @staticmethod
    def fail(name: str, check: str) -> dict: return {"name": name, "check": check, "on_violation": "fail"}

    @staticmethod
    def drop(name: str, check: str) -> dict: return {"name": name, "check": check, "on_violation": "drop"}

    @staticmethod
    def quarantine(name: str, check: str) -> dict:
        return {"name": name, "check": check, "on_violation": "quarantine"}


@dataclass
class Connection:
    name: str


@dataclass
class SqlFunction:
    """A registered UDF. Formats as its name in f-strings; calling it renders a call expression:
    churn_score("amount", "country") -> "churn_score(amount, country)"."""

    name: str

    def __str__(self) -> str:
        return self.name

    def __format__(self, spec) -> str:
        return self.name

    def __call__(self, *args) -> str:
        return f"{self.name}({', '.join(str(a) for a in args)})"


@dataclass
class Relation:
    """A source or model. Formats as its table name inside SQL f-strings; also starts builder chains."""

    pipeline: "Pipeline"
    name: str
    columns: tuple | None = None

    def __str__(self) -> str:
        return self.name

    def __format__(self, spec) -> str:
        return self.name

    def _q(self) -> Query:
        return Query(from_=self.name, base=self.name, columns=self.columns)

    def __getattr__(self, attr):
        if attr.startswith("_"):
            raise AttributeError(attr)
        return getattr(self._q(), attr)  # where/select/tumble/group_by/join/... start a query


@dataclass
class _ModelDef:
    name: str
    fn: object
    sql: str | None
    sql_file: str | None
    params: list[str]
    options: dict
    line: int


@dataclass
class Pipeline:
    name: str
    description: str | None = None
    owner: str | None = None
    state_ttl: str | None = None
    flink_config: dict | None = None
    deploy: dict | None = None  # image, namespace, jobmanager, taskmanager, checkpoints, ... (see docs)
    scaling: dict | None = None  # max_parallelism, parallelism, min, max, target_utilization, max_lag, autoscale
    vars: dict = field(default_factory=dict)
    environments: dict = field(default_factory=dict)

    def __post_init__(self):
        self._line = _caller_line()
        self._connections: dict[str, tuple[ConnectorSpec, int]] = {}
        self._sources: dict[str, tuple[dict, int]] = {}
        self._models: dict[str, _ModelDef] = {}
        self._sinks: dict[str, tuple[dict, int]] = {}
        self._quarantine: tuple[dict, int] | None = None
        self._functions: dict[str, tuple[dict, int]] = {}

    # -- declarations -------------------------------------------------------------------
    def var(self, name: str) -> str:
        return "${var:" + name + "}"

    def connection(self, name: str, spec: ConnectorSpec) -> Connection:
        self._connections[name] = (spec, _caller_line())
        return Connection(name)

    def java_function(self, name: str, class_name: str, *, jar: str | None = None, returns: str | None = None,
                      deterministic: bool = True, description: str | None = None) -> SqlFunction:
        """A Java ScalarFunction; `jar` is relative to the project (or omit it if the class is on the
        runtime image's classpath). `returns` lets the compiler type the column statically."""
        self._functions[name] = ({"language": "java", "class": class_name, "jar": jar, "returns": returns,
                                  "deterministic": deterministic if not deterministic else None,
                                  "description": description}, _caller_line())
        return SqlFunction(name)

    def python_function(self, fn, *, returns: str, name: str | None = None, deterministic: bool = True,
                        description: str | None = None) -> SqlFunction:
        """A plain Python function from a module of the project (not pipeline.py itself), run by PyFlink
        workers. Pass the function object, or "module_file.py:function"."""
        if callable(fn):
            file = inspect.getsourcefile(fn)
            func = fn.__name__
            description = description or (inspect.getdoc(fn) or None)
        else:
            file, _, func = str(fn).partition(":")
        fname = name or func
        self._functions[fname] = ({"language": "python", "file": file, "function": func, "returns": returns,
                                   "deterministic": deterministic if not deterministic else None,
                                   "description": description}, _caller_line())
        return SqlFunction(fname)

    def source(self, name: str, connection, *, columns: dict, format: str | None = None,
               event_time=None, primary_key=None, processing_time: str | None = None,
               options: dict | None = None, description: str | None = None, **endpoint) -> Relation:
        if isinstance(event_time, tuple):
            event_time = {"column": event_time[0], "max_out_of_orderness": event_time[1]}
        spec = {"connection": _conn_name(connection), "columns": dict(columns), "format": format,
                "event_time": event_time, "primary_key": list(primary_key) if primary_key else None,
                "processing_time": processing_time, "options": options, "description": description,
                **endpoint}
        self._sources[name] = (spec, _caller_line())
        return Relation(self, name, tuple(columns))

    def model(self, fn=None, *, name: str | None = None, columns: dict | None = None,
              expectations: list | None = None, allow_unbounded_state: bool = False,
              description: str | None = None, scaling: dict | None = None, backfill: bool = False):
        """Decorator. The function's parameters name its inputs (sources or models); it returns SQL
        (use the parameters in an f-string) or a builder query."""
        line = _caller_line()

        def register(f):
            mname = name or f.__name__
            params = list(inspect.signature(f).parameters)
            opts = {"columns": columns, "expectations": expectations, "description": description or
                    (inspect.getdoc(f) or None),
                    "allow_unbounded_state": allow_unbounded_state or None, "scaling": scaling}
            # co_firstlineno is the first decorator's line on every Python version
            self._models[mname] = _ModelDef(mname, f, None, None, params, opts,
                                            f.__code__.co_firstlineno if hasattr(f, "__code__") else line)
            return Relation(self, mname)

        return register(fn) if fn is not None else register

    def sql_model(self, name: str, sql: str | None = None, *, sql_file: str | None = None,
                  columns: dict | None = None, expectations: list | None = None,
                  allow_unbounded_state: bool = False, description: str | None = None,
                  scaling: dict | None = None, backfill: bool = False) -> Relation:
        """A model given as SQL text (or a .sql file) referencing tables by name."""
        self._models[name] = _ModelDef(name, None, sql, sql_file, [], {
            "columns": columns, "expectations": expectations, "description": description,
            "allow_unbounded_state": allow_unbounded_state or None, "scaling": scaling,
            "backfill": backfill or None}, _caller_line())
        return Relation(self, name)

    def sink(self, name: str, from_, connection, *, mode: str = "append", primary_key=None,
             format: str | None = None, on_conflict: str | None = None, options: dict | None = None,
             description: str | None = None, **endpoint) -> None:
        spec = {"from": str(from_), "connection": _conn_name(connection), "mode": mode,
                "primary_key": list(primary_key) if primary_key else None, "format": format,
                "on_conflict": on_conflict, "options": options, "description": description, **endpoint}
        self._sinks[name] = (spec, _caller_line())

    def quarantine(self, connection, *, format: str | None = None, **endpoint) -> None:
        self._quarantine = ({"connection": _conn_name(connection), "format": format, **endpoint}, _caller_line())

    # -- compilation ----------------------------------------------------------------------
    def to_document(self, file: str = "<python>") -> LDict:
        diags = Diagnostics()
        relations = set(self._sources) | set(self._models)
        doc = _ld({"pipeline": self.name, "description": self.description, "owner": self.owner}, self._line)
        if self.state_ttl:
            doc["defaults"] = _ld({"state_ttl": self.state_ttl}, self._line)
        if self.flink_config:
            doc["flink"] = _ld({"config": _ld(self.flink_config, self._line)}, self._line)
        if self.scaling:
            doc["scaling"] = _ld(self.scaling, self._line)
        if self.deploy:
            doc["deploy"] = _nested(self.deploy, self._line)
        if self.vars:
            doc["vars"] = _ld(self.vars, self._line)
        if self.environments:
            doc["environments"] = _ld({k: _ld({"vars": _ld(v.get("vars", v), self._line)}, self._line)
                                       for k, v in self.environments.items()}, self._line)
        if self._functions:
            base = os.path.dirname(os.path.abspath(file)) if file != "<python>" else os.getcwd()
            fdefs = {}
            for n, (spec, line) in self._functions.items():
                spec = dict(spec)
                if spec.get("file") and os.path.isabs(spec["file"]):
                    spec["file"] = os.path.relpath(spec["file"], base)
                fdefs[n] = _ld(spec, line)
            doc["functions"] = _ld(fdefs, self._line)
        doc["connections"] = _ld({n: _ld({"type": spec.type, "properties": _ld(spec.properties, line)}, line)
                                  for n, (spec, line) in self._connections.items()}, self._line)
        doc["sources"] = _ld({n: _nested(spec, line) for n, (spec, line) in self._sources.items()}, self._line)

        models = {}
        for m in self._models.values():
            entry = {k: v for k, v in m.options.items() if v is not None}
            if m.fn is not None:
                missing = [x for x in m.params if x not in relations]
                if missing:
                    diags.error("CD302", f"model '{m.name}' takes parameter(s) {missing} that are not sources "
                                f"or models", Location(file, m.line),
                                hint="parameters name the model's inputs", node=m.name)
                    continue
                try:
                    out = m.fn(*[_relation(self, x) for x in m.params])
                    entry["sql"] = out.to_sql() if isinstance(out, Query) else str(out)
                except Exception as e:  # builder errors and bugs in user code
                    diags.error("CD303" if isinstance(e, RecursionError) else "CD304",
                                f"model '{m.name}' failed to build: {type(e).__name__}: {e}",
                                Location(file, m.line), node=m.name)
                    continue
            elif m.sql_file:
                entry["sql_file"] = m.sql_file
            else:
                entry["sql"] = m.sql or ""
            if "expectations" in entry:
                entry["expectations"] = [_ld(e, m.line) for e in entry["expectations"]]
            models[m.name] = _nested(entry, m.line)
        doc["models"] = _ld(models, self._line)
        doc["sinks"] = _ld({n: _nested(spec, line) for n, (spec, line) in self._sinks.items()}, self._line)
        if self._quarantine:
            doc["quarantine"] = _nested(*self._quarantine)
        if diags.has_errors:
            raise CompileError(diags)
        return doc

    def compile(self, env: str | None = None, emit: bool = True):
        """Compile in-process (handy in notebooks). Returns a compiler Result."""
        from .analyze import check
        from .compiler import Result
        from .lower import lower
        try:
            p, diags = load_document(self.to_document(), "<python>", os.getcwd(), env)
        except CompileError as e:
            return Result(e.diagnostics)
        c = check(p, diags)
        if diags.has_errors or not emit:
            return Result(diags, c)
        from .udfs import write_python_dir
        lowered = lower(c)
        write_python_dir(p)
        return Result(diags, c, lowered)

    def validate(self, env: str | None = None) -> list[str]:
        return [d.format() for d in self.compile(env, emit=False).diagnostics.items]


def _conn_name(connection) -> str:
    return connection.name if isinstance(connection, Connection) else str(connection)


def _relation(p: Pipeline, name: str) -> Relation:
    cols = tuple(p._sources[name][0]["columns"]) if name in p._sources else None
    return Relation(p, name, cols)


def _nested(spec: dict, line: int) -> LDict:
    return _ld({k: (_ld(v, line) if isinstance(v, dict) and not isinstance(v, LDict) else v)
                for k, v in spec.items()}, line)


def load_python(path: str, env: str | None = None):
    """Execute pipeline.py and load the Pipeline it defines (variable `pipeline`, or the only one)."""
    path = os.path.abspath(path)
    root = os.path.dirname(path)
    rel = os.path.relpath(path)
    diags = Diagnostics()
    sys.path.insert(0, root)
    try:
        ns = runpy.run_path(path, run_name="__cascade__")
    except CompileError:
        raise
    except Exception as e:
        tb = e.__traceback__
        line = None
        while tb is not None:
            if os.path.abspath(tb.tb_frame.f_code.co_filename) == path:
                line = tb.tb_lineno
            tb = tb.tb_next
        diags.error("CD000", f"pipeline.py raised {type(e).__name__}: {e}", Location(rel, line))
        raise CompileError(diags)
    finally:
        sys.path.remove(root)
    found = ns.get("pipeline")
    if not isinstance(found, Pipeline):
        candidates = [v for v in ns.values() if isinstance(v, Pipeline)]
        if len(candidates) != 1:
            diags.error("CD000", "pipeline.py must define exactly one Pipeline (or name it `pipeline`)",
                        Location(rel))
            raise CompileError(diags)
        found = candidates[0]
    doc = found.to_document(rel)
    return load_document(doc, rel, root, env)
