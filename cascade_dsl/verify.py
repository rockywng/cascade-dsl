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

from . import jvm
from .analyze import Checked
from .localize import localize
from .lower import render_script

PLANNER_OPS = {
    "GroupAggregate": "group_agg", "GlobalGroupAggregate": "group_agg", "LocalGroupAggregate": None,
    "WindowAggregate": "window_agg", "LocalWindowAggregate": None, "GlobalWindowAggregate": "window_agg",
    "Join": "regular_join", "IntervalJoin": "interval_join", "TemporalJoin": "temporal_join",
    "Deduplicate": "dedup", "Rank": "dedup", "OverAggregate": "over_agg", "WindowJoin": "window_join",
    "LookupJoin": "lookup_join", "Match": "match_recognize", "SinkMaterializer": "sink_materializer",
    "ChangelogNormalize": "changelog_normalize", "ProcessTableFunction": "process_table",
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
    return jvm.find_flink_home(explicit)


_UDF_CALL = re.compile(r"CASCADE_(?:ASSERT|EXPECT)\((.*?) IS TRUE(?:, '(?:[^']|'')*'){3}\)")


def localized_script(lowered, c: Checked, tmpdir: str) -> str:
    stmts = localize(lowered, c, "plan", tmpdir)
    out = render_script(stmts, lowered.header, statement_set="EXPLAIN CHANGELOG_MODE")
    if not jvm.runtime_jar():
        # runtime jar not built: plan with equivalent filters instead of the expectation UDFs
        out = re.sub(r"^CREATE TEMPORARY SYSTEM FUNCTION .*$", "", out, flags=re.MULTILINE)
        out = _UDF_CALL.sub(r"\1 IS TRUE", out)
    return out


def run_verify(c: Checked, lowered, flink_home: str, timeout: int = 180) -> VerifyResult:
    with tempfile.TemporaryDirectory(prefix="cascade-verify-") as tmp:
        script = os.path.join(tmp, "verify.sql")
        with open(script, "w") as f:
            f.write(localized_script(lowered, c, tmp))
        cmd = [os.path.join(flink_home, "bin", "sql-client.sh"), "embedded", "-f", script]
        jar = jvm.runtime_jar()
        if jar:
            cmd[2:2] = ["-j", jar]
        if any(f.language == "python" for f in c.pipeline.functions.values()):
            import glob
            for py_jar in sorted(glob.glob(os.path.join(flink_home, "opt", "flink-python*.jar")))[:1]:
                cmd[2:2] = ["-j", py_jar]
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
    for m in re.finditer(r"^[\s:|+\-]*([A-Za-z]+)\(", physical, re.MULTILINE):
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
