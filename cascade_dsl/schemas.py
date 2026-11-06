"""Exact schema inference with the Flink validator, persisted as cascade.lock.json.

Static inference is fast but can't type every expression. `cascade infer` asks Flink for the exact
output schema of every model and writes a lock file; the compiler then uses those types for any
model whose fingerprint (SQL + contract + input schemas) still matches, and warns when it is stale.
Compilation itself stays JVM-free, so editors and CI don't need Flink.
"""
from __future__ import annotations

import json
import os
import tempfile

from .analyze import model_fingerprint
from .compiler import Result, compile_pipeline
from .diagnostics import Diagnostics
from .ir import OutputColumn
from .jvm import Tools
from .localize import localize
from .lower import lower
from .types import TypeParseError, parse_type

LOCK_FILE = "cascade.lock.json"
# errors that exact inference is meant to resolve
TYPE_ERRORS = {"CD408", "CD324"}


class InferError(RuntimeError):
    def __init__(self, message: str, result: Result | None = None):
        super().__init__(message)
        self.result = result


def lock_path(root_dir: str) -> str:
    return os.path.join(root_dir, LOCK_FILE)


def read_lock(root_dir: str) -> dict | None:
    try:
        with open(lock_path(root_dir)) as f:
            return json.load(f).get("models")
    except (OSError, ValueError):
        return None


def infer(path: str, flink_home: str, env: str | None = None) -> tuple[dict, str]:
    """Returns (lock document, path written)."""
    r = compile_pipeline(path, env, emit=False, use_lock=False)
    if r.checked is None:
        raise InferError("pipeline does not load", r)
    blocking = [d for d in r.diagnostics.errors if d.code not in TYPE_ERRORS]
    if blocking:
        raise InferError("fix these errors first", r)
    c = r.checked
    p = c.pipeline
    lowered = lower(c, partial=True)
    with tempfile.TemporaryDirectory(prefix="cascade-infer-") as d:
        described = Tools(flink_home).describe(localize(lowered, c, "plan", d))
    if described.get("error"):
        raise InferError(f"Flink rejected the pipeline:\n{described['error']}")
    views = described["views"]

    exact: dict[str, list[OutputColumn] | None] = {s: p.relations[s].columns for s in p.sources}
    models = {}
    problems = Diagnostics()
    for name in c.order:
        info = views.get(name)
        if info is None or isinstance(info, dict):
            problems.error("CD362", f"Flink could not resolve model '{name}': "
                           f"{(info or {}).get('error', 'view missing')}", p.models[name].loc, node=name)
            exact[name] = None
            continue
        inputs = p.relations[name].inputs
        models[name] = {
            "fingerprint": model_fingerprint(p, name, inputs, exact),
            "columns": info,
        }
        cols = []
        for col in info:
            try:
                cols.append(OutputColumn(col["name"], parse_type(col["type"])))
            except TypeParseError:
                cols.append(OutputColumn(col["name"], None))
        exact[name] = cols
    if problems.has_errors:
        raise InferError("\n".join(x.format() for x in problems.errors))

    doc = {"format_version": 1, "generated_by": "cascade infer", "models": models}
    out = lock_path(p.root_dir)
    with open(out, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    return doc, out
