"""Verify generated SQL with the real Flink planner (no cluster needed).

Production connectors are usually not on a developer's classpath, so verification swaps
them for built-in stand-ins with the same schema and changelog semantics:

  append source      -> datagen
  changelog source   -> filesystem + debezium-json   (an updating, keyed changelog)
  any sink/quarantine-> blackhole                    (accepts inserts, updates and deletes)

Then it runs `EXPLAIN CHANGELOG_MODE` on the statement set via sql-client in embedded
mode and cross-checks the planner's facts against the static analysis.
"""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field

from .analyze import Checked

TABLE_RE = re.compile(r"CREATE TEMPORARY TABLE `([^`]+)` \((.*?)\n\) WITH \((.*?)\n\);", re.DOTALL)
PLANNER_OPS = {
    "GroupAggregate": "group_agg", "GlobalGroupAggregate": "group_agg", "LocalGroupAggregate": None,
    "WindowAggregate": "window_agg", "LocalWindowAggregate": None, "GlobalWindowAggregate": "window_agg",
    "Join": "regular_join", "IntervalJoin": "interval_join", "TemporalJoin": "temporal_join",
    "Deduplicate": "dedup", "Rank": "dedup", "OverAggregate": "over_agg", "WindowJoin": "window_join",
    "LookupJoin": "lookup_join", "Match": "match_recognize", "SinkMaterializer": "sink_materializer",
    "ChangelogNormalize": "changelog_normalize",
}


@dataclass
class VerifyResult:
    ok: bool
    plan: str = ""
    sink_modes: dict[str, str] = field(default_factory=dict)
    planner_ops: list[str] = field(default_factory=list)
    findings: list[str] = field(default_factory=list)
    error: str = ""


def find_flink_home(explicit: str | None = None) -> str | None:
    for cand in (explicit, os.environ.get("FLINK_HOME")):
        if cand and os.path.exists(os.path.join(cand, "bin", "sql-client.sh")):
            return cand
    return None


def localize(sql: str, c: Checked, tmpdir: str) -> str:
    """Rewrite connector WITH clauses to built-in stand-ins."""
    p = c.pipeline

    def repl(m):
        name, body = m.group(1), m.group(2)
        if name in p.sources:
            if p.relations[name].changelog == "updating":
                path = os.path.join(tmpdir, name)
                os.makedirs(path, exist_ok=True)
                opts = f"  'connector' = 'filesystem',\n  'path' = '{path}',\n  'format' = 'debezium-json'"
            else:
                opts = "  'connector' = 'datagen'"
        else:
            opts = "  'connector' = 'blackhole'"
        return f"CREATE TEMPORARY TABLE `{name}` ({body}\n) WITH (\n{opts}\n);"

    out = TABLE_RE.sub(repl, sql)
    out = out.replace("EXECUTE STATEMENT SET", "EXPLAIN CHANGELOG_MODE STATEMENT SET")
    if not _runtime_jar():
        # no runtime jar built: plan with an equivalent filter instead of the assertion UDF
        out = re.sub(r"^CREATE TEMPORARY SYSTEM FUNCTION .*$", "", out, flags=re.MULTILINE)
        out = re.sub(r"PIPEDSL_ASSERT\((.*?) IS TRUE, '(?:[^']|'')*'\)", r"\1 IS TRUE", out)
    return out


def _runtime_jar() -> str | None:
    jar = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "runtime", "target",
                       "pipedsl-runtime.jar")
    return jar if os.path.exists(jar) else None


def run_verify(c: Checked, sql: str, flink_home: str, timeout: int = 180) -> VerifyResult:
    with tempfile.TemporaryDirectory(prefix="pipec-verify-") as tmp:
        script = os.path.join(tmp, "verify.sql")
        local_sql = localize(sql, c, tmp)
        with open(script, "w") as f:
            f.write(local_sql)
        cmd = [os.path.join(flink_home, "bin", "sql-client.sh"), "embedded", "-f", script]
        jar = _runtime_jar()
        if jar:
            cmd[2:2] = ["-j", jar]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return VerifyResult(False, error=f"sql-client timed out after {timeout}s")
    output = proc.stdout + proc.stderr
    err = re.search(r"\[ERROR\] Could not execute SQL statement\. Reason:\s*\n(.*?)(?:\n\n|\Z)", output, re.DOTALL)
    if err or "== Optimized Execution Plan ==" not in output:
        msg = err.group(1).strip() if err else output[-3000:]
        return VerifyResult(False, error=msg)
    plan = output[output.index("== Abstract Syntax Tree =="):]
    plan = plan[:plan.rfind("|")] if "|" in plan else plan
    return _crosscheck(c, plan)


def _crosscheck(c: Checked, plan: str) -> VerifyResult:
    p = c.pipeline
    res = VerifyResult(True, plan=plan)
    physical = plan.split("== Optimized Physical Plan ==")[-1].split("== Optimized Execution Plan ==")[0]
    # the Sink node itself reports [NONE]; what it receives is the mode of its direct input
    lines = physical.splitlines()
    for i, line in enumerate(lines):
        m = re.match(r"Sink\(table=\[default_catalog\.default_database\.([^\]]+)\]", line)
        if m and i + 1 < len(lines):
            mode = re.search(r"changelogMode=\[([A-Z,]+)\]", lines[i + 1])
            if mode:
                res.sink_modes[m.group(1)] = mode.group(1)
    for m in re.finditer(r"^\s*(?:\+- |:- )?([A-Za-z]+)\(", physical, re.MULTILINE):
        op = PLANNER_OPS.get(m.group(1))
        if op:
            res.planner_ops.append(op)

    for k in p.sinks.values():
        mode = res.sink_modes.get(k.name)
        if mode is None:
            continue
        planner_updating = any(x in mode for x in ("UA", "UB", "D"))
        static_updating = p.relations[k.from_].changelog == "updating"
        if planner_updating != static_updating:
            res.findings.append(
                f"sink '{k.name}': planner changelog [{mode}] but static analysis said "
                f"'{p.relations[k.from_].changelog}' — the planner is authoritative")
            if planner_updating and k.mode == "append":
                res.ok = False
                res.findings.append(f"sink '{k.name}' is append-only but receives updates [{mode}]")
    static_kinds = {o.kind for n in c.order for o in p.relations[n].stateful_ops}
    for op in sorted(set(res.planner_ops) - static_kinds - {"changelog_normalize", "sink_materializer",
                                                           "lookup_join", "group_agg"}):
        res.findings.append(f"planner uses '{op}', which static analysis did not detect")
    if "sink_materializer" in res.planner_ops:
        res.findings.append("planner added a SinkUpsertMaterializer (sink primary key differs from the upsert "
                            "key); this adds state — consider aligning the sink primary_key with the GROUP BY key")
    return res
