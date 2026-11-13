"""PyFlink runner for pipelines with Python process models.

A Python process model is a function over PyFlink DataStreams:

    def sessionize(ctx, inputs):          # inputs: one DataStream of Rows per declared input
        return inputs[0].process(Sessions(ctx.config), output_type=ctx.output_type).uid(ctx.uid("sessionize"))

PyFlink builds Python operators from the Python side, so these pipelines are driven from Python: this
runner applies the job script like dev.cascade.runner.SqlRunner (setup statements, replication-slot
guards, Paimon tables, Java process models -- all through SqlRunner's Java helpers) and adds the Python
process models in between. It is a standalone file (no cascade_dsl imports): images ship it on its own.

    python pyrunner.py job --setup setup.sql --statements statements.sql [--await] [--restore PATH]
    python pyrunner.py tests DIR OUT.json TIMEOUT          # cascade test (the tools' `run`)

The cascade runtime (and, for tests, tools) jars come from CASCADE_JARS (path-separated) or the job's
classpath (application mode: usrlib).
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys

PROCESS = "-- @@process"
SEPARATOR = "-- @@cascade"


# --- environment -----------------------------------------------------------------------------------

def _jars() -> list[str]:
    return [j for j in os.environ.get("CASCADE_JARS", "").split(os.pathsep) if j]


def environment(batch: bool = False, restore: str | None = None, local: bool = False):
    from pyflink.common import Configuration
    from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    conf = Configuration()
    jars = _jars()
    if jars:
        conf.set_string("pipeline.jars", ";".join("file://" + os.path.abspath(j) for j in jars))
    if restore:
        conf.set_string("execution.state-recovery.path", restore)
    if local:
        conf.set_string("parallelism.default", "1")
        conf.set_string("restart-strategy.type", "none")
        conf.set_string("rest.port", "0")
        conf.set_string("table.local-time-zone", "UTC")
    # the Python workers use this interpreter (it has pyflink)
    conf.set_string("python.executable", sys.executable)
    conf.set_string("python.client.executable", sys.executable)
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    if batch:
        env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    settings = (EnvironmentSettings.in_batch_mode() if batch else EnvironmentSettings.in_streaming_mode())
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    if jars:  # make the cascade classes reachable through the gateway
        from pyflink.util.java_utils import add_jars_to_context_class_loader
        add_jars_to_context_class_loader(["file://" + os.path.abspath(j) for j in jars])
    return env, t_env


def _jvm():
    from pyflink.java_gateway import get_gateway
    return get_gateway().jvm


# --- Python process models -------------------------------------------------------------------------

def python_spec(stmt: str) -> dict | None:
    if not stmt.startswith(PROCESS):
        return None
    spec = json.loads(stmt[len(PROCESS):])
    return spec if spec.get("language") == "python" else None


def type_info(sql_type: str):
    """TypeInformation for a declared column type (what the process function's rows carry)."""
    from pyflink.common import Types
    t = sql_type.upper().replace(" NOT NULL", "").strip()
    base = t.split("(")[0].strip()
    simple = {"STRING": Types.STRING(), "VARCHAR": Types.STRING(), "CHAR": Types.STRING(),
              "BIGINT": Types.LONG(), "INT": Types.INT(), "INTEGER": Types.INT(), "SMALLINT": Types.SHORT(),
              "TINYINT": Types.BYTE(), "BOOLEAN": Types.BOOLEAN(), "DOUBLE": Types.DOUBLE(),
              "FLOAT": Types.FLOAT(), "DECIMAL": Types.BIG_DEC(), "DATE": Types.SQL_DATE(),
              "TIMESTAMP": Types.SQL_TIMESTAMP(), "BYTES": Types.PRIMITIVE_ARRAY(Types.BYTE())}
    if base not in simple:
        raise ValueError(f"Python process models support {', '.join(sorted(simple))} columns, not {sql_type}")
    return simple[base]


class Context:
    """What a Python process function knows about its model (mirrors dev.cascade.api.ProcessContext)."""

    def __init__(self, spec: dict):
        from pyflink.common import Types
        from pyflink.datastream import OutputTag
        self.name = spec["name"]
        self.config = dict(spec.get("config") or {})
        cols = spec["columns"]
        self.output_type = Types.ROW_NAMED([c[0] for c in cols], [type_info(c[1]) for c in cols])
        self._sides = {}
        for side, s in (spec.get("side_outputs") or {}).items():
            st = Types.ROW_NAMED([c[0] for c in s["columns"]], [type_info(c[1]) for c in s["columns"]])
            self._sides[side] = OutputTag(f"{self.name}.{side}", st)

    def uid(self, operator: str) -> str:
        return f"{self.name}.{operator}"

    def side_output(self, name: str):
        if name not in self._sides:
            raise KeyError(f"process model {self.name} declares no side output {name}")
        return self._sides[name]


def apply_python(t_env, spec: dict):
    """Inputs as DataStreams (keyed if asked), the user's function, its output (and side outputs) as views."""
    from pyflink.common import Types
    # loaded without registering the module: PyFlink then pickles the function and what it uses (classes,
    # helpers of the same file) by value, so the Python workers need nothing installed
    path = spec["file"]
    module_spec = importlib.util.spec_from_file_location(f"cascade_process_{spec['name']}", path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    fn = getattr(module, spec["function"])
    keys = list(spec.get("key_by") or [])
    streams = []
    for name in spec["inputs"]:
        table = t_env.from_path(f"`{name}`")
        ds = t_env.to_changelog_stream(table)
        if keys:
            fields = table.get_schema().get_field_names()
            idx = [fields.index(k) for k in keys]
            ds = ds.key_by(lambda row, idx=idx: "\x01".join("\x00" if row[i] is None else str(row[i]) for i in idx),
                           key_type=Types.STRING())
        streams.append(ds)
    ctx = Context(spec)
    out = fn(ctx, streams)
    if out is None:
        raise RuntimeError(f"process model {spec['name']}: the function returned None")
    # side outputs first: PyFlink tells the Python operator about them when they are requested
    sides = {side: out.get_side_output(ctx.side_output(side)) for side in (spec.get("side_outputs") or {})}
    _register(t_env, spec["view"], out, spec)
    for side, s in (spec.get("side_outputs") or {}).items():
        _register(t_env, s["view"], sides[side], s)


def _register(t_env, view: str, stream, spec: dict):
    """The stream as a view. Built in Java: PyFlink's timestamp/date type infos produce java.sql values, so
    those columns are declared bridged to java.sql.Timestamp / java.sql.Date."""
    import re as _re
    jvm = _jvm()
    types = jvm.org.apache.flink.table.api.DataTypes
    cls = jvm.java.lang.Class.forName
    b = jvm.org.apache.flink.table.api.Schema.newBuilder()
    for name, sql_type in spec["columns"]:
        t = sql_type.upper()
        m = _re.match(r"\s*TIMESTAMP\s*(?:\((\d+)\))?", t)
        if m and "LOCAL TIME ZONE" not in t and "LTZ" not in t:
            dt = types.TIMESTAMP(int(m.group(1) or 6)).bridgedTo(cls("java.sql.Timestamp"))
            b.column(name, dt.notNull() if "NOT NULL" in t else dt)
        elif t.strip().startswith("DATE"):
            dt = types.DATE().bridgedTo(cls("java.sql.Date"))
            b.column(name, dt.notNull() if "NOT NULL" in t else dt)
        else:
            b.column(name, sql_type)
    if spec.get("event_time"):
        b.watermark(spec["event_time"], "SOURCE_WATERMARK()")
    if spec.get("primary_key"):
        b.primaryKey(spec["primary_key"])
    modes = jvm.org.apache.flink.table.connector.ChangelogMode
    mode = {"upsert": modes.upsert(), "retract": modes.all()}.get(spec.get("changelog"), modes.insertOnly())
    # what PyFlink's own conversions do first: configure the Python operators (side outputs, timers, ...)
    jvm.org.apache.flink.python.util.PythonConfigUtil.configPythonOperator(
        stream._j_data_stream.getExecutionEnvironment())
    table = t_env._j_tenv.fromChangelogStream(stream._j_data_stream, b.build(), mode)
    t_env._j_tenv.createTemporaryView(f"`{view}`", table)


def apply_setup(t_env, stmts: list[str]) -> bool:
    """Every setup statement, in order: Python process models here, the rest through SqlRunner."""
    runner = _jvm().dev.cascade.runner.SqlRunner
    processes = False
    for stmt in stmts:
        spec = python_spec(stmt)
        if spec is not None:
            apply_python(t_env, spec)
            processes = True
        else:
            processes |= runner.applySetup(t_env._j_tenv, stmt)
    return processes


# --- commands -------------------------------------------------------------------------------------------

def run_job(setup: str, statements: str, wait: bool, restore: str | None):
    batch = "'execution.runtime-mode' = 'batch'" in open(setup).read()
    env, t_env = environment(batch=batch, restore=restore)
    jvm = _jvm()
    jenv = jvm.java.lang.System.getenv()
    stmts = list(jvm.dev.cascade.runner.SqlRunner.readScript(setup, jenv))
    option = jvm.org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH
    restoring = bool(restore) or bool(env._j_stream_execution_environment.getConfiguration()
                                      .getOptional(option).orElse(""))
    java_list = jvm.java.util.ArrayList()
    for s in stmts:
        java_list.add(s)
    renames = jvm.dev.cascade.runner.Guards.resolve(java_list, restoring, jenv)
    stmts = [jvm.dev.cascade.runner.Guards.rename(s, renames) for s in stmts]
    apply_setup(t_env, stmts)
    j_set = t_env._j_tenv.createStatementSet()
    for s in jvm.dev.cascade.runner.SqlRunner.readScript(statements, jenv):
        j_set.addInsertSql(s[:-1] if s.endswith(";") else s)
    j_set.attachAsDataStream()  # SQL and the Python operators as one job
    name = t_env.get_config().get("pipeline.name", "cascade")
    if wait:
        env.execute(name)
    else:
        env.execute_async(name)


def run_tests(directory: str, out: str, timeout: int):
    """Like the tools' `run`, for tests of pipelines with Python process models: each test's setup is
    applied here, its query or capture runs through dev.cascade.tools.Main.runPreparedTest."""
    results = []
    for name in sorted(os.listdir(directory)):
        test = os.path.join(directory, name)
        if not os.path.isdir(test):
            continue
        try:
            _, t_env = environment(local=True)
            with open(os.path.join(test, "setup.sql")) as f:
                stmts = [s.strip() for s in f.read().split(f"\n{SEPARATOR}\n") if s.strip()]
            apply_setup(t_env, stmts)
            r = json.loads(_jvm().dev.cascade.tools.Main.runPreparedTest(t_env._j_tenv, test, timeout))
        except Exception as e:  # noqa: BLE001
            r = {"error": f"{type(e).__name__}: {e}"}
        r["name"] = name
        results.append(r)
    with open(out, "w") as f:
        json.dump({"results": results}, f)


def _submit_script(path: str, restore: str | None = None, allow_state_loss: bool = False):
    """A whole script (setup, process directives, inserts) as one job, on the local MiniCluster."""
    env, t_env = environment(restore=restore, local=True)
    if restore:
        env._j_stream_execution_environment.getConfiguration()  # noqa: B018 -- restore set in environment()
        t_env.get_config().set("execution.state-recovery.ignore-unclaimed-state", str(allow_state_loss).lower())
    with open(path) as f:
        stmts = [s.strip() for s in f.read().split(f"\n{SEPARATOR}\n") if s.strip()]
    inserts = [s for s in stmts if s.upper().startswith("INSERT")]
    apply_setup(t_env, [s for s in stmts if not s.upper().startswith("INSERT")])
    j_set = t_env._j_tenv.createStatementSet()
    for s in inserts:
        j_set.addInsertSql(s[:-1] if s.endswith(";") else s)
    j_set.attachAsDataStream()
    return env.execute_async("cascade-restore-test")


def _wait_running(client, seconds: int) -> str:
    import time
    deadline = time.time() + seconds
    status = None
    while time.time() < deadline:
        status = client.get_job_status().result().name
        if status in ("RUNNING", "FAILED", "CANCELED", "FINISHED"):
            return status
        time.sleep(0.5)
    return status or "UNKNOWN"


def restore_test(old: str, new: str, savepoints: str, seconds: int, allow_state_loss: bool, out: str):
    """Like the tools' restore-test-statements, for pipelines with Python process models: run the old
    script, savepoint it, restore the new script from it strictly (no unclaimed state)."""
    import time

    def log(msg):
        print(f"cascade restore-test: {msg}", file=sys.stderr, flush=True)
    result = {}
    old_job = new_job = None
    try:
        old_job = _submit_script(old)
        log(f"old job {_wait_running(old_job, 60)}")
        time.sleep(seconds)
        log("savepoint")
        # PyFlink 2.3's JobClient.trigger_savepoint calls a Java overload Flink 2 removed (no format type)
        fmt = _jvm().org.apache.flink.core.execution.SavepointFormatType.CANONICAL
        savepoint = old_job._j_job_client.triggerSavepoint("file://" + os.path.abspath(savepoints), fmt).get()
        result["savepoint"] = savepoint
        log(f"savepoint {savepoint}")
        try:
            old_job.cancel().result()
        except Exception:  # noqa: BLE001 -- already gone
            pass
        old_job = None
        new_job = _submit_script(new, restore=savepoint, allow_state_loss=allow_state_loss)
        status = _wait_running(new_job, 60)
        time.sleep(3)
        try:
            status = new_job.get_job_status().result().name
        except Exception:  # noqa: BLE001 -- its MiniCluster is gone: the job ended
            status = "FAILED"
        result["restored"] = status == "RUNNING"
        result["status"] = status
        if status != "RUNNING":
            try:
                new_job.get_job_execution_result().result()
            except Exception as e:  # noqa: BLE001
                result["error"] = str(e)[-4000:]
    except Exception as e:  # noqa: BLE001
        result["restored"] = False
        result["error"] = f"{type(e).__name__}: {str(e)[-4000:]}"
    finally:
        for job in (old_job, new_job):
            if job is not None:
                try:
                    job.cancel().result()
                except Exception:  # noqa: BLE001
                    pass
    with open(out, "w") as f:
        json.dump(result, f)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pyrunner")
    sub = ap.add_subparsers(dest="cmd", required=True)
    j = sub.add_parser("job")
    j.add_argument("--setup", required=True)
    j.add_argument("--statements", required=True)
    j.add_argument("--await", dest="wait", action="store_true")
    j.add_argument("--restore")
    t = sub.add_parser("tests")
    t.add_argument("dir")
    t.add_argument("out")
    t.add_argument("timeout", type=int)
    r = sub.add_parser("restore-test")
    r.add_argument("old")
    r.add_argument("new")
    r.add_argument("savepoints")
    r.add_argument("seconds", type=int)
    r.add_argument("out")
    r.add_argument("--allow-state-loss", action="store_true")
    a = ap.parse_args(argv)
    if a.cmd == "job":
        run_job(a.setup, a.statements, a.wait, a.restore)
    elif a.cmd == "tests":
        run_tests(a.dir, a.out, a.timeout)
    else:
        restore_test(a.old, a.new, a.savepoints, a.seconds, a.allow_state_loss, a.out)


if __name__ == "__main__":
    main()
