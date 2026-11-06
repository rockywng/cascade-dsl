"""Compiled plans, UID pinning and savepoint-restore tests.

Flink only assigns explicit operator UIDs to pipelines started from a compiled plan, using
`table.exec.uid.format` = "<id>_<transformation>", where <id> is a counter over the plan's nodes.
Any change that adds, removes or reorders nodes renumbers the counter, so even untouched operators
get new UIDs and their state no longer maps on restore.

`pin_uids` fixes that: it matches the new plan's nodes against the previous plan structurally
(a node matches if its own definition AND everything upstream of it is identical) and writes the
old UID pattern into each matched node's persisted configuration. The format option is read
per node, so matched operators keep their UIDs and their state, and only operators that changed
get fresh ones.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field

from .compiler import compile_pipeline
from .jvm import Tools
from .localize import localize

UID_KEY = "table.exec.uid.format"
DEFAULT_UID = "<id>_<transformation>"


class PlanError(RuntimeError):
    pass


def _hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()[:20]


def _content(node: dict) -> dict:
    """A node's own definition, without identity/cosmetic fields."""
    c = {k: v for k, v in node.items() if k not in ("id", "description")}
    conf = dict(c.get("configuration") or {})
    conf.pop(UID_KEY, None)
    if conf:
        c["configuration"] = conf
    else:
        c.pop("configuration", None)
    if isinstance(c.get("state"), list):
        # the retention time is a property of the state, not its identity: a new TTL applies to the
        # restored entries (only turning TTL on or off changes the state's serializer)
        c["state"] = [{**st, "ttl": bool(st.get("ttl")) and st.get("ttl") not in ("0 ms",)} if isinstance(st, dict)
                      else st for st in c["state"]]
    return c


def _graph(plan: dict):
    nodes = {n["id"]: n for n in plan["nodes"]}
    inputs = defaultdict(list)
    outputs = defaultdict(list)
    for e in plan["edges"]:
        inputs[e["target"]].append(e["source"])
        outputs[e["source"]].append(e["target"])
    return nodes, inputs, outputs


def _merkle(plan: dict) -> dict[int, str]:
    nodes, inputs, _ = _graph(plan)
    memo: dict[int, str] = {}

    def visit(i):
        if i not in memo:
            memo[i] = _hash([_content(nodes[i]), [visit(x) for x in inputs[i]]])
        return memo[i]

    for i in nodes:
        visit(i)
    return memo


def _sinks_reached(plan: dict) -> dict[int, frozenset]:
    nodes, _, outputs = _graph(plan)
    memo: dict[int, frozenset] = {}

    def visit(i):
        if i not in memo:
            n = nodes[i]
            own = set()
            if n["type"].startswith("stream-exec-sink"):
                own.add(n.get("dynamicTableSink", {}).get("table", {}).get("identifier", str(i)))
            for o in outputs[i]:
                own |= visit(o)
            memo[i] = frozenset(own)
        return memo[i]

    for i in nodes:
        visit(i)
    return memo


# exec node types that keep state. Not every one declares a "state" entry in the plan JSON
# (temporal joins, for example, do not), so the type is the reliable signal.
STATEFUL_TYPES = ("stream-exec-table-source-scan", "stream-exec-sink", "stream-exec-changelog-normalize",
                  "stream-exec-temporal-join", "stream-exec-join", "stream-exec-multi-join",
                  "stream-exec-interval-join", "stream-exec-window-join", "stream-exec-group-aggregate",
                  "stream-exec-global-group-aggregate", "stream-exec-incremental-group-aggregate",
                  "stream-exec-window-aggregate", "stream-exec-global-window-aggregate",
                  "stream-exec-group-window-aggregate", "stream-exec-deduplicate", "stream-exec-rank",
                  "stream-exec-window-rank", "stream-exec-window-deduplicate", "stream-exec-over-aggregate",
                  "stream-exec-match", "stream-exec-limit", "stream-exec-sort-limit")

# stateful operators whose output depends on input from before they started: started empty next to
# sources that resume from saved positions, they only ever see post-upgrade input
HISTORY_TYPES = {
    "stream-exec-temporal-join": "its versioned table starts empty and the source will not resend old "
                                 "rows, so lookups miss for every key that has not changed since the upgrade",
    "stream-exec-join": "it only joins rows that arrive after the upgrade",
    "stream-exec-multi-join": "it only joins rows that arrive after the upgrade",
    "stream-exec-group-aggregate": "it only aggregates rows that arrive after the upgrade",
    "stream-exec-global-group-aggregate": "it only aggregates rows that arrive after the upgrade",
    "stream-exec-incremental-group-aggregate": "it only aggregates rows that arrive after the upgrade",
    "stream-exec-deduplicate": "it does not know rows seen before the upgrade",
    "stream-exec-rank": "it does not know rows seen before the upgrade",
    "stream-exec-over-aggregate": "it only aggregates rows that arrive after the upgrade",
    "stream-exec-changelog-normalize": "it does not know rows seen before the upgrade",
}


def is_stateful(node: dict) -> bool:
    return "state" in node or node["type"].startswith(STATEFUL_TYPES)


def history_warning(node: dict) -> str | None:
    t = node["type"].rsplit("_", 1)[0]
    return HISTORY_TYPES.get(t)


def _label(node: dict) -> str:
    return node.get("description", node["type"]).split("(")[0] + f" #{node['id']}"


def effective_uid_format(node: dict) -> str:
    fmt = (node.get("configuration") or {}).get(UID_KEY, DEFAULT_UID)
    return fmt.replace("<id>", str(node["id"]))


@dataclass
class PinReport:
    exact: list[tuple[str, str]] = field(default_factory=list)  # (new label, old label)
    drift: list[tuple[str, str]] = field(default_factory=list)
    new_stateful: list[str] = field(default_factory=list)
    # new operators that need pre-upgrade input to be correct: (label, why)
    needs_history: list[tuple[str, str]] = field(default_factory=list)
    dropped_stateful: list[str] = field(default_factory=list)
    # replaced sink operators without declared state (most sinks keep none; a transactional
    # exactly-once writer does, and would lose its pending transactions)
    dropped_sinks: list[str] = field(default_factory=list)

    @property
    def restorable(self) -> bool:
        return not self.dropped_stateful


def pin_uids(old: dict, new: dict, allow_drift: bool = True, tag: str | None = None) -> tuple[dict, PinReport]:
    """Return a copy of `new` whose matching nodes reuse the UIDs they had in `old`."""
    new = copy.deepcopy(new)
    old_nodes, _, _ = _graph(old)
    new_nodes, _, _ = _graph(new)
    old_m, new_m = _merkle(old), _merkle(new)
    by_merkle = defaultdict(list)
    for i in sorted(old_nodes):
        by_merkle[old_m[i]].append(i)
    tag = tag or _hash(new)[:6]
    report = PinReport()
    matched_old: set[int] = set()
    assignment: dict[int, int] = {}

    # pass 1: identical node with identical upstream
    for i in sorted(new_nodes):
        cands = [o for o in by_merkle.get(new_m[i], []) if o not in matched_old]
        if cands:
            assignment[i] = cands[0]
            matched_old.add(cands[0])
            report.exact.append((_label(new_nodes[i]), _label(old_nodes[cands[0]])))

    # pass 2: identical stateful node whose upstream changed (e.g. a filter was edited), feeding
    # the same sinks -- keep its state, like `check-upgrade` reports as SEMANTIC_DRIFT
    if allow_drift:
        old_sinks, new_sinks = _sinks_reached(old), _sinks_reached(new)
        local_old = defaultdict(list)
        for o in old_nodes:
            if o not in matched_old and is_stateful(old_nodes[o]):
                local_old[(_hash(_content(old_nodes[o])), old_sinks[o])].append(o)
        for i in sorted(new_nodes):
            if i in assignment or not is_stateful(new_nodes[i]):
                continue
            cands = local_old.get((_hash(_content(new_nodes[i])), new_sinks[i]), [])
            cands = [o for o in cands if o not in matched_old]
            if len(cands) == 1:
                assignment[i] = cands[0]
                matched_old.add(cands[0])
                report.drift.append((_label(new_nodes[i]), _label(old_nodes[cands[0]])))

    for node in new["nodes"]:
        conf = dict(node.get("configuration") or {})
        if node["id"] in assignment:
            conf[UID_KEY] = effective_uid_format(old_nodes[assignment[node["id"]]])
        else:
            # fresh, collision-free UIDs for new/changed operators
            conf[UID_KEY] = f"{tag}-<id>_<transformation>"
            if is_stateful(node):
                report.new_stateful.append(_label(node))
                why = history_warning(node)
                if why and old_nodes:  # a first deployment has no "before"
                    report.needs_history.append((_label(node), why))
        node["configuration"] = conf
    for o in sorted(old_nodes):
        n = old_nodes[o]
        if o in matched_old or not is_stateful(n):
            continue
        if n["type"].startswith("stream-exec-sink") and "state" not in n:
            report.dropped_sinks.append(_label(n))
        else:
            report.dropped_stateful.append(_label(n))
    return new, report


# --- CLI-level operations --------------------------------------------------------------------

def plan_setup(stmts) -> list:
    """Statements a compiled plan needs registered before EXECUTE PLAN (plans reference
    temporary tables and functions by name only, which also keeps connector secrets out of plans)."""
    return [s for s in stmts if s.kind in ("set", "function", "source", "sink", "quarantine")]


def compile_plan(path: str, flink_home: str, out: str, env: str | None = None, local: bool = False,
                 previous: str | None = None, workdir: str | None = None):
    """Compile the project's job to a Flink plan JSON (optionally pinned to a previous plan).
    Returns (compile result, pin report or None, setup statements)."""
    r = compile_pipeline(path, env)
    if r.lowered is None:
        raise PlanError("compilation failed:\n" + "\n".join(d.format() for d in r.diagnostics.errors))
    tmp = workdir or tempfile.mkdtemp(prefix="cascade-plan-")
    stmts = localize(r.lowered, r.checked, "restore", tmp) if local else r.lowered.statements
    result = Tools(flink_home).compile_plan(stmts, out)
    if not result.get("ok"):
        raise PlanError("Flink could not compile the plan:\n" + result.get("error", "?"))
    report = None
    if previous:
        with open(previous) as f:
            old = json.load(f)
        with open(out) as f:
            new = json.load(f)
        pinned, report = pin_uids(old, new)
        with open(out, "w") as f:
            json.dump(pinned, f, indent=2)
    return r, report, plan_setup(stmts)


def operator_map_for(r, setup, plan_path: str, flink_home: str) -> dict:
    """operators.json for a compiled (and possibly pinned) plan: vertex IDs -> models."""
    from .operators import operator_map
    graph = Tools(flink_home).job_graph(setup, plan_path)
    if graph.get("error"):
        raise PlanError("Flink could not build the job graph:\n" + graph["error"])
    with open(plan_path) as f:
        plan = json.load(f)
    return operator_map(plan, graph, r.checked)


def restore_test(old_path: str, new_path: str, flink_home: str, env: str | None = None,
                 pin: bool = True, seconds: int = 8, allow_state_loss: bool = False) -> dict:
    """Run the old version locally, take a savepoint, and restore the new version from it.

    Uses local stand-in connectors (datagen / blackhole), strict mode (no unclaimed state allowed)."""
    tools = Tools(flink_home)
    with tempfile.TemporaryDirectory(prefix="cascade-restore-") as d:
        old_plan = os.path.join(d, "old.json")
        new_plan = os.path.join(d, "new.json")
        _, _, old_setup = compile_plan(old_path, flink_home, old_plan, env, local=True,
                                       workdir=os.path.join(d, "old"))
        _, report, new_setup = compile_plan(new_path, flink_home, new_plan, env, local=True,
                                            previous=old_plan if pin else None, workdir=os.path.join(d, "new"))
        if report is None:
            with open(old_plan) as f:
                old = json.load(f)
            with open(new_plan) as f:
                _, report = pin_uids(old, json.load(f))  # analysis only; the plan stays unpinned
        result = tools.restore_test(old_setup, old_plan, new_setup, new_plan, seconds,
                                    allow_state_loss=allow_state_loss)
    result["pinned"] = pin
    result["report"] = report
    return result
