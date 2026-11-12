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
