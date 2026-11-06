"""Savepoint/upgrade compatibility: diff the manifest of the running version against a new one.

Each stateful node (sources and models with stateful operators) has a `state_id` derived
from its logic and its input schemas. When it changes, the old state cannot be mapped onto
the new operator, so the upgrade either loses that state (allowNonRestoredState) or fails.
"""
from __future__ import annotations

from dataclasses import dataclass

ORDER = {"breaking": 3, "state_loss": 2, "warning": 1, "ok": 0}


@dataclass
class Change:
    level: str  # ok | warning | state_loss | breaking
    code: str
    node: str
    message: str


def diff_manifests(old: dict, new: dict) -> tuple[str, list[Change]]:
    changes: list[Change] = []
    on, nn = old.get("nodes", {}), new.get("nodes", {})

    for name, o in on.items():
        n = nn.get(name)
        if o.get("state_id") is None:
            continue
        if n is None or n.get("state_id") is None:
            changes.append(Change("state_loss", "STATE_DROPPED", name,
                                  f"{o['kind']} '{name}' held state and is gone; its state will be discarded "
                                  "(deploy with allowNonRestoredState)"))
            continue
        if n["state_id"] != o["state_id"]:
            why = []
            if n["logic_hash"] != o["logic_hash"]:
                why.append("its definition changed")
            if n["inputs"] != o["inputs"]:
                why.append(f"its inputs changed ({', '.join(o['inputs'])} -> {', '.join(n['inputs'])})")
            elif n["state_id"] != o["state_id"] and n["logic_hash"] == o["logic_hash"]:
                why.append("an input schema changed")
            if o["kind"] == "source":
                msg = (f"source '{name}' identity changed ({'; '.join(why)}); stored offsets/positions "
                       "will not be restored and it will start from its configured startup mode")
            else:
                ops = ", ".join(sorted({x['kind'] for x in o['stateful_ops']}))
                msg = (f"model '{name}' ({ops}) is incompatible with its saved state: {'; '.join(why)}. "
                       "It will restart from empty state; consider a backfill")
            changes.append(Change("state_loss", "STATE_INCOMPATIBLE", name, msg))
        elif n.get("upstream_hash") != o.get("upstream_hash") and o["kind"] == "model":
            changes.append(Change("warning", "SEMANTIC_DRIFT", name,
                                  f"model '{name}' keeps its state but upstream logic changed; existing state "
                                  "was built with the old logic (results may mix old and new semantics)"))

    for name, n in nn.items():
        if n.get("state_id") is not None and (name not in on or on[name].get("state_id") is None):
            what = "will start from its startup mode" if n["kind"] == "source" else "starts with empty state"
            changes.append(Change("warning", "STATE_NEW", name, f"{n['kind']} '{name}' is new and {what}"))

    os_, ns = old.get("sinks", {}), new.get("sinks", {})
    for name, o in os_.items():
        n = ns.get(name)
        if n is None:
            changes.append(Change("warning", "SINK_REMOVED", name, f"sink '{name}' is no longer written"))
            continue
        if n["schema_hash"] != o["schema_hash"]:
            oc = {c["name"]: c["type"] for c in o["columns"]}
            nc = {c["name"]: c["type"] for c in n["columns"]}
            removed = [c for c in oc if c not in nc]
            retyped = [c for c in oc if c in nc and oc[c] != nc[c]]
            added = [c for c in nc if c not in oc]
            level = "breaking" if removed or retyped else "warning"
            parts = ([f"removed {removed}"] if removed else []) + ([f"retyped {retyped}"] if retyped else []) \
                + ([f"added {added}"] if added else [])
            changes.append(Change(level, "SINK_SCHEMA_CHANGED", name,
                                  f"sink '{name}' schema changed: {', '.join(parts) or 'column order'}; "
                                  "downstream consumers and the target table must be migrated"))
        if n.get("on_conflict") != o.get("on_conflict"):
            changes.append(Change("warning", "SINK_CONFLICT_POLICY_CHANGED", name,
                                  f"sink '{name}' on_conflict {o.get('on_conflict')} -> {n.get('on_conflict')}; "
                                  "the sink materializer state may not be restorable"))
        if n["mode"] != o["mode"] or n["primary_key"] != o["primary_key"]:
            changes.append(Change("breaking", "SINK_SEMANTICS_CHANGED", name,
                                  f"sink '{name}' changed mode/key ({o['mode']} {o['primary_key']} -> "
                                  f"{n['mode']} {n['primary_key']})"))
        if n["options"].get("connector") != o["options"].get("connector") or \
                n["options"].get("topic") != o["options"].get("topic"):
            changes.append(Change("warning", "SINK_TARGET_CHANGED", name, f"sink '{name}' writes to a new target"))
    for name in ns:
        if name not in os_:
            changes.append(Change("warning", "SINK_ADDED", name, f"sink '{name}' is new"))

    changes += _scaling_changes(old, new)
    verdict = max((c.level for c in changes), key=ORDER.get, default="ok")
    return verdict, sorted(changes, key=lambda c: -ORDER[c.level])


def _default_max_parallelism(parallelism: int) -> int:
    """Flink's derived max parallelism when none is configured."""
    p = parallelism + parallelism // 2
    rounded = 1 << max(p - 1, 0).bit_length()
    return min(max(rounded, 128), 32768)


def _scaling_changes(old: dict, new: dict) -> list[Change]:
    keyed = any(n.get("kind") == "model" and n.get("stateful_ops") for n in old.get("nodes", {}).values())
    if not keyed:
        return []
    o = (old.get("scaling") or {}).get("max_parallelism")
    n = (new.get("scaling") or {}).get("max_parallelism")
    if o == n:
        return []
    if o is not None and n is not None:
        return [Change("breaking", "MAX_PARALLELISM_CHANGED", "scaling",
                       f"max_parallelism {o} -> {n}: keyed state is split into {o} key groups in the savepoint "
                       "and cannot be restored with a different number; keep the old value")]
    if o is None:
        p = (old.get("scaling") or {}).get("parallelism") or 1
        return [Change("warning", "MAX_PARALLELISM_PINNED", "scaling",
                       f"max_parallelism is now {n}, but the running job derived its own (likely "
                       f"{_default_max_parallelism(p)} at parallelism {p}); restore fails unless they match. "
                       "Check the savepoint's value before deploying")]
    return [Change("warning", "MAX_PARALLELISM_UNSET", "scaling",
                   f"max_parallelism is no longer set (was {o}); Flink keeps the savepoint's value on restore, "
                   "but new deployments from scratch would derive a different one")]
