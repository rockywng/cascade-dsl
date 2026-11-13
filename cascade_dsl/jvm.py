"""Driver for runtime/target/cascade-tools.jar (local Flink in an embedded MiniCluster)."""
from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
import tempfile

import yaml

from .lower import Stmt

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET = os.path.join(REPO, "runtime", "target")
SEPARATOR = "-- @@cascade"
DEFAULT_JAVA_OPTS = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED")


class ToolsError(RuntimeError):
    pass


def find_flink_home(explicit: str | None = None) -> str | None:
    for cand in (explicit, os.environ.get("FLINK_HOME")):
        if cand and os.path.exists(os.path.join(cand, "bin", "sql-client.sh")):
            return cand
    return None


def python_executable() -> str | None:
    """Interpreter for PyFlink workers: $CASCADE_PYTHON, or this one if it has pyflink installed."""
    if os.environ.get("CASCADE_PYTHON"):
        return os.environ["CASCADE_PYTHON"]
    import importlib.util
    import sys
    return sys.executable if importlib.util.find_spec("pyflink") else None


def runtime_jar() -> str | None:
    jar = os.path.join(TARGET, "cascade-runtime.jar")
    return jar if os.path.exists(jar) else None


def script(stmts: list[Stmt]) -> str:
    return f"\n{SEPARATOR}\n".join(st.render() for st in stmts) + "\n"


class Tools:
    def __init__(self, flink_home: str):
        self.flink_home = flink_home
        self.tools_jar = os.path.join(TARGET, "cascade-tools.jar")
        if not os.path.exists(self.tools_jar) or not runtime_jar():
            raise ToolsError("JVM tools are not built; run: FLINK_HOME=<flink> runtime/build.sh")

    def _java(self) -> str:
        return os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else "java"

    def _java_major(self) -> int:
        if not hasattr(self, "_major"):
            proc = subprocess.run([self._java(), "-version"], capture_output=True, text=True)
            m = re.search(r'version "(\d+)(?:\.(\d+))?', proc.stderr)
            major = int(m.group(1)) if m else 17
            self._major = int(m.group(2)) if major == 1 and m and m.group(2) else major
        return self._major

    def _java_opts(self) -> list[str]:
        opts = DEFAULT_JAVA_OPTS
        try:
            with open(os.path.join(self.flink_home, "conf", "config.yaml")) as f:
                conf = yaml.safe_load(f) or {}
            opts = conf.get("env", {}).get("java", {}).get("opts", {}).get("all") or opts
        except (OSError, yaml.YAMLError, AttributeError):
            pass
        # Flink's config lists flags for newer JDKs; its launch scripts drop them on older ones
        newer = {"--sun-misc-unsafe-memory-access": 23, "--enable-native-access": 22}
        major = self._java_major()
        return [o for o in str(opts).split() if all(not o.startswith(f) or major >= v for f, v in newer.items())]

    def _run(self, args: list[str], timeout: int, unshaded_planner: bool = False,
             extra_cp: list[str] | None = None) -> None:
        java = self._java()
        cp = [self.tools_jar, runtime_jar(), os.path.join(self.flink_home, "lib", "*")]
        if unshaded_planner:
            # the planner loader hides Calcite and the planner's operation classes: list lib/ without it
            # and put opt/'s unshaded planner in its place
            planner = sorted(glob.glob(os.path.join(self.flink_home, "opt", "flink-table-planner_2.12-*.jar")))
            if not planner:
                raise ToolsError("column lineage needs opt/flink-table-planner_2.12-*.jar in the Flink distribution")
            cp = cp[:2] + [j for j in sorted(glob.glob(os.path.join(self.flink_home, "lib", "*.jar")))
                           if "flink-table-planner-loader" not in os.path.basename(j)] + planner[:1]
        # Python UDF support lives in opt/ in the Flink distribution
        cp += sorted(glob.glob(os.path.join(self.flink_home, "opt", "flink-python*.jar")))[:1]
        # connector jars that aren't in FLINK_HOME/lib, e.g. from `cascade fetch-jars`
        cp += [j for j in os.environ.get("CASCADE_EXTRA_JARS", "").split(os.pathsep) if j]
        cp += list(extra_cp or [])
        cp = os.pathsep.join(cp)
        cmd = [java, *self._java_opts(),
               f"-Dlog4j.configurationFile={os.path.join(REPO, 'runtime', 'tools', 'log4j2.properties')}",
               "-cp", cp, "dev.cascade.tools.Main", *args]
        try:
            env = dict(os.environ)
            py = python_executable()
            if py:
                env["CASCADE_PYTHON"] = py
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        except subprocess.TimeoutExpired:
            raise ToolsError(f"cascade-tools timed out after {timeout}s")
        if proc.returncode != 0:
            raise ToolsError(f"cascade-tools failed (exit {proc.returncode}):\n{proc.stderr[-4000:]}")

    @staticmethod
    def _read(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    def describe(self, stmts: list[Stmt], timeout: int = 180) -> dict:
        with tempfile.TemporaryDirectory(prefix="cascade-describe-") as d:
            path = os.path.join(d, "script.sql")
            with open(path, "w") as f:
                f.write(script([s for s in stmts if s.kind != "insert"]))
            out = os.path.join(d, "out.json")
            self._run(["describe", path, out], timeout)
            return self._read(out)

    def column_lineage(self, stmts: list[Stmt], timeout: int = 180) -> dict:
        """{"inserts": [{"sink", "columns": [{"name", "derived", "from": [{"table", "column"}]}]}]}: where
        every sink column comes from, by the planner."""
        with tempfile.TemporaryDirectory(prefix="cascade-lineage-") as d:
            path = os.path.join(d, "script.sql")
            with open(path, "w") as f:
                f.write(script(stmts))
            out = os.path.join(d, "out.json")
            self._run(["column-lineage", path, out], timeout, unshaded_planner=True)
            return self._read(out)

    def run_tests(self, cases: list[tuple], per_test_timeout: int = 60, timeout: int = 900) -> list[dict]:
        """cases: (name, setup statements, query[, (sink table, capture file)]). A capture runs the
        query (an INSERT) to completion and returns what the sink received. One result per case."""
        with tempfile.TemporaryDirectory(prefix="cascade-tests-") as d:
            index = {}
            for i, (name, stmts, query, *rest) in enumerate(cases):
                td = os.path.join(d, "tests", f"{i:04d}")
                os.makedirs(td)
                index[f"{i:04d}"] = name
                with open(os.path.join(td, "setup.sql"), "w") as f:
                    f.write(script(stmts))
                with open(os.path.join(td, "query.sql"), "w") as f:
                    f.write(query)
                if rest and rest[0]:
                    with open(os.path.join(td, "capture.txt"), "w") as f:
                        f.write(f"{rest[0][0]}\n{rest[0][1]}\n")
            out = os.path.join(d, "out.json")
            if any('"language": "python"' in script(c[1]) for c in cases):
                self._run_python_tests(os.path.join(d, "tests"), out, per_test_timeout, timeout)
            else:
                self._run(["run", os.path.join(d, "tests"), out, str(per_test_timeout)], timeout)
            results = self._read(out)["results"]
            for r in results:
                r["name"] = index[r["name"]]
            return results

    def _run_python_tests(self, tests: str, out: str, per_test_timeout: int, timeout: int):
        """Tests of pipelines with Python process models run on cascade_dsl/pyrunner.py (PyFlink)."""
        py = python_executable()
        if py is None:
            raise ToolsError("Python process models need PyFlink: pip install apache-flink, or set CASCADE_PYTHON")
        runner = os.path.join(os.path.dirname(__file__), "pyrunner.py")
        env = {**os.environ, "CASCADE_JARS": os.pathsep.join([self.tools_jar, runtime_jar()])}
        # -P: not cascade_dsl/ on sys.path (its types.py would shadow the standard library's)
        p = subprocess.run([py, "-P", runner, "tests", tests, out, str(per_test_timeout)], capture_output=True, text=True,
                           timeout=timeout, env=env)
        if p.returncode != 0 or not os.path.exists(out):
            raise ToolsError("Python test runner failed:\n" + (p.stderr or p.stdout)[-4000:])

    def compile_plan(self, stmts: list[Stmt], out_path: str, timeout: int = 180) -> dict:
        with tempfile.TemporaryDirectory(prefix="cascade-plan-") as d:
            path = os.path.join(d, "script.sql")
            with open(path, "w") as f:
                f.write(script(stmts))
            self._run(["compile-plan", path, os.path.abspath(out_path)], timeout)
            result_path = os.path.abspath(out_path) + ".result.json"
            result = self._read(result_path)
            os.remove(result_path)
            return result

    def restore_test(self, old_setup: list[Stmt], old_plan: str, new_setup: list[Stmt], new_plan: str,
                     seconds: int = 8, timeout: int = 400, allow_state_loss: bool = False) -> dict:
        d = tempfile.mkdtemp(prefix="cascade-restore-")
        try:
            out = os.path.join(d, "out.json")
            paths = []
            for label, stmts in (("old", old_setup), ("new", new_setup)):
                path = os.path.join(d, f"{label}-setup.sql")
                with open(path, "w") as f:
                    f.write(script(stmts))
                paths.append(path)
            self._run(["restore-test", paths[0], os.path.abspath(old_plan), paths[1], os.path.abspath(new_plan),
                       os.path.join(d, "savepoints"), str(seconds), out,
                       "allow" if allow_state_loss else "strict"], timeout)
            return self._read(out)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def restore_test_statements(self, old: list[Stmt], new: list[Stmt], seconds: int = 8, timeout: int = 400,
                                allow_state_loss: bool = False, jars: list[str] | None = None) -> dict:
        """`jars`: UDF jars. A whole-script job is built in this JVM, so `USING JAR` classes must be on its
        classpath (the planner loads them, but deserializing the job graph doesn't)."""
        d = tempfile.mkdtemp(prefix="cascade-restore-")
        try:
            out = os.path.join(d, "out.json")
            paths = []
            for label, stmts in (("old", old), ("new", new)):
                path = os.path.join(d, f"{label}.sql")
                with open(path, "w") as f:
                    f.write(script(stmts))
                paths.append(path)
            if any('"language": "python"' in script(s) for s in (old, new)):
                # Python process models: the restore test runs on the Python runner
                py = python_executable()
                if py is None:
                    raise ToolsError("Python process models need PyFlink: pip install apache-flink, or CASCADE_PYTHON")
                runner = os.path.join(os.path.dirname(__file__), "pyrunner.py")
                p = subprocess.run([py, "-P", runner, "restore-test", paths[0], paths[1], os.path.join(d, "savepoints"),
                                    str(seconds), out] + (["--allow-state-loss"] if allow_state_loss else []),
                                   capture_output=True, text=True, timeout=timeout,
                                   env={**os.environ, "CASCADE_JARS": os.pathsep.join([self.tools_jar, runtime_jar()])})
                if not os.path.exists(out):
                    raise ToolsError("Python restore test failed:\n" + (p.stderr or p.stdout)[-4000:])
                return self._read(out)
            self._run(["restore-test-statements", paths[0], paths[1], os.path.join(d, "savepoints"), str(seconds),
                       out, "allow" if allow_state_loss else "strict"], timeout, extra_cp=jars)
            return self._read(out)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def job_graph(self, setup: list[Stmt], plan: str, timeout: int = 180) -> dict:
        with tempfile.TemporaryDirectory(prefix="cascade-jobgraph-") as d:
            path = os.path.join(d, "setup.sql")
            with open(path, "w") as f:
                f.write(script(setup))
            out = os.path.join(d, "out.json")
            self._run(["job-graph", path, os.path.abspath(plan), out], timeout)
            return self._read(out)
