"""Map a compiled plan's operators back to the pipeline's sources, models and sinks.

Flink inlines views, so the deployed job has no notion of "model". Observability ("which model is
backpressured?") and per-model autoscaling need the mapping, which is rebuilt here from:

  1. sources and sinks: scan / sink nodes name their table
  2. anchors: each model's stateful operators (aggregates, joins, dedup, ...) are matched to plan
     nodes of a compatible type that read the same sources and feed the same sinks, in topological
     order, so a chain of two aggregations maps in order
  3. everything else (Calc, Exchange, local pre-aggregation, ...) by region: a stateless node
     belongs to the stateless models fused into it (models between its nearest upstream and
     downstream anchors in the pipeline graph); without any, to its upstream model's projection,
     or to the downstream model when the upstream is a source

Operators of one Flink vertex can belong to several models (operator chaining); `vertices` lists
each vertex with the models it runs, which is what metrics (keyed by vertex ID) need.
"""
from __future__ import annotations

import re
from collections import defaultdict

from .analyze import Checked

# stateful op kind (from the static analysis) -> exec node types that implement it
ANCHOR_TYPES = {
    "group_agg": ("group-aggregate", "global-group-aggregate", "incremental-group-aggregate"),
    "global_agg": ("group-aggregate", "global-group-aggregate"),
    "distinct": ("group-aggregate", "global-group-aggregate"),
    "window_agg": ("window-aggregate", "global-window-aggregate", "group-window-aggregate"),
    "regular_join": ("join", "multi-join"),
    "interval_join": ("interval-join",),
    "temporal_join": ("temporal-join", "lookup-join"),
    "window_join": ("window-join",),
    "dedup": ("deduplicate", "rank", "window-deduplicate", "window-rank"),
    "over_agg": ("over-aggregate",),
    "match_recognize": ("match",),
    "process_table": ("process-table-function",),
}
SOURCE_TAIL = {"watermark-assigner", "drop-update-before", "changelog-normalize", "mini-batch-assigner"}
LOCAL_PREFIX = "local-"
NAME_RE = re.compile(r"\[(\d+)\]")


def node_type(node: dict) -> str:
    t = node["type"]
    t = t[len("stream-exec-"):] if t.startswith("stream-exec-") else t
    return t.rsplit("_", 1)[0]


def _table(ident: str | None) -> str | None:
    if not ident:
        return None
    return ident.replace("`", "").split(".")[-1]


class _Graph:
    def __init__(self, plan: dict):
        self.nodes = {n["id"]: n for n in plan["nodes"]}
        self.inputs = defaultdict(list)
        self.outputs = defaultdict(list)
        for e in plan["edges"]:
            self.inputs[e["target"]].append(e["source"])
            self.outputs[e["source"]].append(e["target"])
        self.order = self._topo()

    def _topo(self) -> list[int]:
        indeg = {i: len(self.inputs[i]) for i in self.nodes}
        ready = sorted(i for i, d in indeg.items() if d == 0)
        out = []
        while ready:
            i = ready.pop(0)
            out.append(i)
            for o in self.outputs[i]:
                indeg[o] -= 1
                if indeg[o] == 0:
                    ready.append(o)
            ready.sort()
        return out

    def closure(self, start: int, edges) -> set[int]:
        seen, stack = set(), [start]
        while stack:
            for x in edges[stack.pop()]:
                if x not in seen:
                    seen.add(x)
                    stack.append(x)
        return seen


def map_plan(plan: dict, c: Checked) -> dict[int, dict]:
    """exec node id -> {"owner": (kind, name), "models": [model names], "how": str}"""
    p = c.pipeline
    g = _Graph(plan)
    owner: dict[int, tuple[str, str]] = {}
    how: dict[int, str] = {}

    # 1. sources and sinks
    for i, n in g.nodes.items():
        t = node_type(n)
        if t.startswith("table-source-scan"):
            name = _table(n.get("scanTableSource", {}).get("table", {}).get("identifier"))
            if name in p.sources:
                owner[i], how[i] = ("source", name), "table"
        elif t == "sink":
            name = _table(n.get("dynamicTableSink", {}).get("table", {}).get("identifier"))
            if name in p.sinks:
                owner[i], how[i] = ("sink", name), "table"
            elif name and name.endswith("__quarantine") and name[: -len("__quarantine")] in p.models:
                owner[i], how[i] = ("model", name[: -len("__quarantine")]), "quarantine sink"
    for i in g.order:  # watermarking, changelog normalization right after a scan
        if i in owner:
            continue
        ins = g.inputs[i]
        t = node_type(g.nodes[i])
        if t in SOURCE_TAIL or (t == "exchange" and any(node_type(g.nodes[o]) == "changelog-normalize"
                                                        for o in g.outputs[i])):
            if len(ins) == 1 and owner.get(ins[0], ("",))[0] == "source":
                owner[i], how[i] = owner[ins[0]], "source"

    # pipeline-graph facts for matching
    rels = p.relations
    descendants: dict[str, set[str]] = defaultdict(set)
    for n in reversed(list(p.sources) + c.order):
        for i in rels[n].inputs:
            descendants[i] |= {n} | descendants[n]

    def ancestors(n: str) -> set[str]:
        out, stack = set(), [n]
        while stack:
            for i in rels[stack.pop()].inputs:
                if i not in out:
                    out.add(i)
                    stack.append(i)
        return out

    def ir_sinks(model: str) -> set[str]:
        reach = {model} | descendants[model]
        out = {k.name for k in p.sinks.values() if k.from_ in reach}
        out |= {f"{m}__quarantine" for m in reach if m in p.models
                and any(e.on_violation == "quarantine" for e in p.models[m].expectations)}
        return out

    def plan_sources(i):
        return {owner[x][1] for x in g.closure(i, g.inputs) | {i} if owner.get(x, ("",))[0] == "source"}

    def plan_sinks(i):
        out = set()
        for x in g.closure(i, g.outputs) | {i}:
            if node_type(g.nodes[x]) == "sink":
                out.add(_table(g.nodes[x].get("dynamicTableSink", {}).get("table", {}).get("identifier")))
        return out

    # 2. anchors, models in topological order, most upstream plan node first
    for m in c.order:
        ops = rels[m].stateful_ops
        if not ops:
            continue
        srcs = {a for a in ancestors(m) if a in p.sources}
        sinks = ir_sinks(m)
        cols = {col.name.lower() for col in rels[m].columns or []}
        for op in ops:
            types = ANCHOR_TYPES.get(op.kind, ())
            cands = [i for i in g.order if i not in owner and node_type(g.nodes[i]) in types
                     and plan_sources(i) == srcs and plan_sinks(i) <= sinks and plan_sinks(i)]
            if not cands:
                continue

            def overlap(i):
                fields = _field_names(g.nodes[i])
                return len(fields & cols)
            best = max(cands, key=lambda i: (overlap(i), -g.order.index(i)))
            # prefer the most upstream candidate unless a later one clearly matches the output
            first = cands[0]
            pick = first if overlap(first) >= overlap(best) else best
            owner[pick], how[pick] = ("model", m), f"anchor ({op.kind})"
            # its local pre-aggregation, through the exchange
            for x in g.inputs[pick]:
                chain = [x] + ([y for y in g.inputs[x]] if node_type(g.nodes[x]) == "exchange" else [])
                for y in chain:
                    if y not in owner and node_type(g.nodes[y]).startswith(LOCAL_PREFIX):
                        owner[y], how[y] = ("model", m), "local pre-aggregation"

    anchors = dict(owner)

    # 3. regions
    def nearest(i, edges) -> set[tuple[str, str]]:
        found, stack, seen = set(), list(edges[i]), set()
        while stack:
            x = stack.pop()
            if x in seen:
                continue
            seen.add(x)
            if x in anchors:
                found.add(anchors[x])
            else:
                stack.extend(edges[x])
        return found

    def as_model(o: tuple[str, str]) -> str | None:
        if o[0] == "model":
            return o[1]
        if o[0] == "sink":
            return p.sinks[o[1]].from_
        return None

    result: dict[int, dict] = {}
    for i in g.order:
        if i in anchors:
            o = anchors[i]
            result[i] = {"owner": o, "models": [as_model(o)] if as_model(o) else [], "how": how[i]}
            continue
        ups, downs = nearest(i, g.inputs), nearest(i, g.outputs)
        t = node_type(g.nodes[i])
        down_models = {as_model(o) for o in downs if as_model(o)}
        # sink-side downstream ends are the sink's input model (inclusive), anchors are exclusive
        down_limit = {as_model(o) for o in downs if o[0] == "model"}
        down_incl = {p.sinks[o[1]].from_ for o in downs if o[0] == "sink"}
        up_models = {o[1] for o in ups if o[0] == "model"}
        up_names = {o[1] for o in ups}
        between = set()
        for u in up_names:
            for dm in down_limit | down_incl:
                cands = descendants[u] & (ancestors(dm) | ({dm} if dm in down_incl else set()))
                between |= {x for x in cands if x in p.models and not rels[x].stateful_ops}
        ordered = [m for m in c.order if m in between]
        if t == "exchange" and len(down_models) == 1 and downs and all(o[0] == "model" for o in downs):
            models, why = sorted(down_models), "shuffle into keyed operator"
        elif ordered:
            models, why = ordered, "fused stateless model(s)"
        elif up_models:
            models, why = [m for m in c.order if m in up_models], "projection after upstream operator"
        elif down_models:
            models, why = [m for m in c.order if m in down_models], "input of downstream operator"
        else:
            models, why = [], "unattributed"
        o = ("model", models[-1]) if models else (sorted(ups)[0] if ups else ("unknown", ""))
        result[i] = {"owner": o, "models": models, "how": why}
    return result


def _field_names(node: dict) -> set[str]:
    t = node.get("outputType")
    if isinstance(t, dict):
        return {f["name"].lower() for f in t.get("fields", [])}
    if isinstance(t, str):
        return {x.lower() for x in re.findall(r"`([^`]+)`", t)}
    return set()


def operator_map(plan: dict, job_graph: dict, c: Checked) -> dict:
    """Combine the exec-node mapping with the JobGraph (vertex IDs as used by metrics and REST)."""
    mapping = map_plan(plan, c)
    p = c.pipeline
    per_model: dict[str, dict] = {m: {"exec_nodes": [], "operators": [], "vertices": []} for m in c.order}
    per_source: dict[str, dict] = {s: {"exec_nodes": [], "operators": [], "vertices": []} for s in p.sources}
    per_sink: dict[str, dict] = {k: {"exec_nodes": [], "operators": [], "vertices": []} for k in p.sinks}
    buckets = {"model": per_model, "source": per_source, "sink": per_sink}
    for i, info in mapping.items():
        kind, name = info["owner"]
        if kind in buckets and name in buckets[kind]:
            buckets[kind][name]["exec_nodes"].append(i)
        for m in info["models"]:
            if m in per_model and (kind, name) != ("model", m):
                per_model[m]["exec_nodes"].append(i)
    vertices = []
    for v in job_graph.get("vertices", []):
        ops = []
        owners = set()
        models = []
        for op in v["operators"]:
            m = NAME_RE.search(op.get("name") or "")
            node = int(m.group(1)) if m else None
            info = mapping.get(node, {"owner": ("unknown", ""), "models": [], "how": "not in plan"})
            ops.append({**op, "exec_node": node, "owner": {"kind": info["owner"][0], "name": info["owner"][1]},
                        "models": info["models"], "how": info["how"]})
            owners.add(info["owner"])
            for x in info["models"]:
                if x not in models:
                    models.append(x)
            kind, name = info["owner"]
            if kind in buckets and name in buckets[kind]:
                b = buckets[kind][name]
                b["operators"].append(op["id"])
                if v["id"] not in b["vertices"]:
                    b["vertices"].append(v["id"])
            for x in info["models"]:
                if x in per_model and v["id"] not in per_model[x]["vertices"]:
                    per_model[x]["vertices"].append(v["id"])
                    if op["id"] not in per_model[x]["operators"]:
                        per_model[x]["operators"].append(op["id"])
        vertices.append({"id": v["id"], "name": v["name"], "parallelism": v["parallelism"],
                         "max_parallelism": v["max_parallelism"], "inputs": v["inputs"],
                         "models": [m for m in c.order if m in models],
                         "sources": sorted(n for k, n in owners if k == "source"),
                         "sinks": sorted(n for k, n in owners if k == "sink"),
                         "operators": ops})
    for b in per_model.values():
        b["exec_nodes"] = sorted(set(b["exec_nodes"]))
    unmapped = [m for m, b in per_model.items() if not b["exec_nodes"]]
    scaling = {m.name: {"min": m.scaling.min, "max": m.scaling.max} for m in p.models.values() if m.scaling}
    return {
        "format_version": 1,
        "pipeline": p.name,
        "models": per_model,
        "sources": per_source,
        "sinks": per_sink,
        "vertices": vertices,
        # models with no operator of their own were optimized away or fully fused into consumers
        "fused_away": unmapped,
        "model_scaling": scaling,
    }
