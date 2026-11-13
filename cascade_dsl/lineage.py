"""Pipelines across projects: who reads whom (`from_pipeline`), and what a change to one breaks.

    cascade lineage <root>                    Mermaid graph of every project under <root>
    cascade impact <project> --root <root>    recompile every project that reads <project>'s sinks
"""
from __future__ import annotations

import os

from .compiler import compile_pipeline

SKIP = {".git", ".work", "node_modules", "target", ".cascade", "__pycache__", "deploy"}


def projects(root: str) -> list[str]:
    out = []
    for d, dirs, files in os.walk(root):
        dirs[:] = sorted(x for x in dirs if x not in SKIP and not x.startswith("."))
        if "pipeline.yaml" in files:
            out.append(os.path.realpath(d))
    return out


def graph(root: str, env: str | None = None) -> tuple[dict, list[dict]]:
    """({project dir: pipeline name or None if it doesn't compile}, [edges {from, sink, to, source}])."""
    names, edges = {}, []
    for d in projects(root):
        r = compile_pipeline(d, env, emit=False)
        p = r.checked.pipeline if r.checked else None
        names[d] = p.name if p and not r.diagnostics.has_errors else None
        for imp in (p.imports if p else []):
            edges.append({"from": os.path.realpath(os.path.join(d, imp["path"])), "sink": imp["sink"],
                          "to": d, "source": imp["source"]})
    return names, edges


def mermaid(root: str, env: str | None = None) -> str:
    names, edges = graph(root, env)
    ident = {d: f"p{i}" for i, d in enumerate(sorted(names))}
    lines = ["flowchart LR"]
    for d in sorted(names):
        label = names[d] or f"{os.path.basename(d)} (does not compile)"
        lines.append(f'  {ident[d]}["{label}"]')
    for e in edges:
        if e["from"] in ident:
            lines.append(f'  {ident[e["from"]]} -->|"{e["sink"]} → {e["source"]}"| {ident[e["to"]]}')
    return "\n".join(lines) + "\n"


def impact(project: str, root: str, env: str | None = None,
           flink_home: str | None = None) -> list[tuple[str, str, list[str]]]:
    """Every project that (transitively) reads `project`, recompiled (and, with a Flink home, planned
    by Flink, which also catches what static analysis can't, e.g. a renamed column in a WHERE)."""
    project = os.path.realpath(project)
    _, edges = graph(root, env)
    readers, frontier = [], [project]
    while frontier:
        cur = frontier.pop()
        for e in edges:
            if e["from"] == cur and e["to"] not in readers:
                readers.append(e["to"])
                frontier.append(e["to"])
    out = []
    for d in readers:
        r = compile_pipeline(d, env)
        name = r.checked.pipeline.name if r.checked else os.path.basename(d)
        errors = [x.format() for x in r.diagnostics.errors]
        if flink_home and not errors and r.lowered is not None:
            from .verify import run_verify
            v = run_verify(r.checked, r.lowered, flink_home)
            if v.error:
                errors.append(f"Flink: {v.error}")
        out.append((d, name, errors))
    return out


# --- column-level lineage and OpenLineage ----------------------------------------------------

ENDPOINT_NAME_KEYS = ("topic", "table", "event_hub", "stream_arn", "index", "path", "collection", "stream")


def _dataset(p, node_kind: str, name: str) -> dict:
    """OpenLineage identity of a source or sink: namespace `<type>://<connection>`, name = what it reads
    or writes (topic, table, path ...), else the node's own name."""
    node = (p.sources if node_kind == "source" else p.sinks)[name]
    conn = p.connections.get(node.connection)
    kind = conn.type if conn else node.connection
    target = next((str(node.endpoint[k]) for k in ENDPOINT_NAME_KEYS if node.endpoint.get(k)), name)
    return {"namespace": f"{kind}://{node.connection}", "name": target}


def column_lineage(path: str, flink_home: str, env: str | None = None) -> dict:
    """Where every sink column comes from, by the Flink planner (views expanded):
    {sink: {column: {"derived": bool, "unknown": bool, "from": [(source, column)]}}}, sources named as
    in the pipeline. `unknown` marks columns the planner can't trace (anything behind a window table
    function), whose `from` is then only what was found. Raises LineageError when the pipeline doesn't
    compile or Flink can't plan it."""
    from .jvm import Tools
    from .localize import localize
    import tempfile
    r = compile_pipeline(path, env)
    if r.lowered is None:
        raise LineageError("compilation failed:\n" + "\n".join(d.format() for d in r.diagnostics.errors))
    p = r.checked.pipeline
    with tempfile.TemporaryDirectory(prefix="cascade-lineage-") as tmp:
        stmts = localize(r.lowered, r.checked, "plan", tmp)
        raw = Tools(flink_home).column_lineage(stmts)
    if raw.get("error"):
        raise LineageError(raw["error"])
    out: dict[str, dict] = {}
    for ins in raw["inserts"]:
        if ins.get("error"):
            raise LineageError(ins["error"])
        sink = ins["sink"]
        if sink not in p.sinks:
            continue  # the quarantine table
        out[sink] = {c["name"]: {"derived": c["derived"], "unknown": bool(c.get("unknown")) or bool(ins.get("incomplete")),
                                 "from": [(f["table"], f["column"]) for f in c["from"] if f["table"] in p.sources]}
                     for c in ins["columns"]}
    return out


class LineageError(RuntimeError):
    pass


def openlineage_event(path: str, flink_home: str, env: str | None = None, event_type: str = "COMPLETE",
                      run_id: str | None = None, namespace: str = "cascade",
                      event_time: str | None = None) -> dict:
    """An OpenLineage RunEvent for the pipeline: job `<pipeline>`, its sources as inputs and sinks as
    outputs, each output carrying the schema and column-level lineage facets."""
    import datetime
    import uuid
    r = compile_pipeline(path, env)
    if r.checked is None or r.diagnostics.has_errors:
        raise LineageError("compilation failed:\n" + "\n".join(d.format() for d in r.diagnostics.errors))
    p = r.checked.pipeline
    cols = column_lineage(path, flink_home, env)
    producer = "https://github.com/cascade/cascade-dsl"
    facet = {"_producer": producer}
    inputs = [{"namespace": (d := _dataset(p, "source", s.name))["namespace"], "name": d["name"],
               "facets": {"schema": {**facet, "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
                                     "fields": [{"name": c.name, "type": str(c.type)} for c in s.columns]}}}
              for s in p.sources.values() if not s.copy_of]
    outputs = []
    for name, sink in p.sinks.items():
        d = _dataset(p, "sink", name)
        fields = {}
        for col, info in cols.get(name, {}).items():
            subtype = "TRANSFORMATION" if info["derived"] else "IDENTITY"
            fields[col] = {"inputFields": [
                {**_dataset(p, "source", s), "field": c,
                 "transformations": [{"type": "DIRECT", "subtype": subtype}]} for s, c in info["from"]]}
        rel = p.relations.get(sink.from_)
        schema_cols = rel.columns if rel and rel.columns else []
        outputs.append({"namespace": d["namespace"], "name": d["name"], "facets": {
            "schema": {**facet, "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
                       "fields": [{"name": c.name, "type": str(c.type) if c.type else "UNKNOWN"} for c in schema_cols]},
            "columnLineage": {**facet, "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
                              "fields": fields}}})
    return {"eventType": event_type, "eventTime": event_time or datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "run": {"runId": run_id or str(uuid.uuid4())},
            "job": {"namespace": namespace, "name": p.name},
            "inputs": inputs, "outputs": outputs, "producer": producer,
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"}
